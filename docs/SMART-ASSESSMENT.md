# Smart group — Phase 1 assessment

Snapshot: `vernesong/Alpha` @ `96feb2a1` (2026-11-07), merge-base with `MetaCubeX/Alpha` = `478af2a5`.
Line numbers below refer to that commit. "Confirmed" = read in the code by us; "Reported" = claimed in an
upstream PR, not yet reproduced by us (Phase 3 will reproduce).

Written to be reusable for the Cain (sing-box) port: sections 1, 5 and 6 are engine-agnostic.

---

## 1. Architecture (one page)

**Size of the feature.** `git diff metacubex/Alpha...vernesong/Alpha --stat`: 70 files, +13,661 / −211, across 285
non-merge commits. The Smart core is ~10.9k lines:

| Area | Files | Lines (approx) |
|---|---|---|
| Group / selector | `adapter/outboundgroup/smart.go` | 2,571 |
| Stats records, EMA, flood/fail caches | `component/smart/stats.go`, `cache.go` | 2,480 |
| Exit-IP / exit-region watcher | `component/smart/exit.go` | 1,383 |
| Heuristic weight | `component/smart/weight.go` | 424 |
| bbolt-backed store + write queue | `component/smart/store.go`, `component/profile/cachefile/smart.go` | 1,018 |
| Response-aware probe verdicts, target keys | `component/smart/response.go`, `target.go` | 1,020 |
| LightGBM model, feature transforms, data collector | `component/smart/lightgbm/*` | 2,040 |
| Kernel TCP retransmit/loss sampling | `component/smart/tcpstats/*` | ~620 (only tested part) |
| `StatusTest` (HTTP probe through a node) | `adapter/adapter.go` | +283 |
| Conn wrappers: first-read latency, close/error callbacks | `common/callback/*` | ~320 |
| Per-connection rate/max-rate tracking, target index | `tunnel/statistic/*` | +173 |
| API: `/group/.../smart` weights, flush cache | `hub/route/groups.go`, `cache.go` | +163 |
| Model updater | `component/updater/update_lgbm.go` | 160 |

**Data flow of one connection.**

1. `DialContext` / `ListenPacketContext` (smart.go:239 / :296) call `selectProxies` (:910), which reads the
   learned per-target ranking (`filterProxies`, :693), drops nodes marked failed/blocked for that target/ASN/host,
   and returns an ordered candidate list (plus "pinned" state from the unwrap cache).
2. TCP: candidates are dialed in batches of up to `parallelDials`=5 racing in parallel (`ParallelDialContext`, :352),
   up to `maxRetries`=5 batches; per-node timeout = 5× remembered connect time, capped at default (`dialTimeout`, :430).
   UDP: candidates are tried one by one, up to `maxSelected`=10.
3. The winner is written to the unwrap cache (`adoptUnwrapWinner`, :505) and the conn is wrapped with
   first-read latency + read/write error capture + close callback (`WrapConnWithMetric`, :549 / :594).
4. On close, `recordConnectionStats` (:1428) builds a `ModelInput` (success/fail, connect time, first-byte latency,
   up/down MB, max rates, duration, TCP loss from `tcpstats`, ASN/host/geo), computes a weight with
   `CalculateWeight` (heuristic) or `lightgbm.PredictWeight` (if `uselightgbm`), EMA-merges it into the per
   (group, config, target, node) record, and runs `checkNodeQuality` (:1977) which may mark the node failed or
   degraded for that target and force-close its other connections.
5. Records are queued and flushed to bbolt (`cache.db`) every 5 min (`FlushQueue`).
6. Background tasks per group (`InitSmart`, :2310): ranking refresh (5 min), stability check via MAD of delays
   (10 min), blocked-node recovery (10 min), host status probe via `StatusTest` (30 min), prefetch (15 min),
   ASN claim refresh (2 min), cleanups (2 h). Three more tasks are process-global (`smartInitOnce`).

**Weight store.** Key space `group/config/{stats|node|ranking|prefetch|failures}/target/node` in bbolt, with an
in-memory write queue and an LRU view cache. UDP targets are namespaced with a `udp|` prefix
(`store.go:38`, `NetScopedTarget`) — this is how TCP and UDP are "scored separately": same formula, separate keys.

**LightGBM hook.** `lightgbm.GetModel()` loads `Model.bin` (downloaded from the vernesong release page,
`update_lgbm.go`); `PredictWeight` (lightgbm.go:685) builds a feature vector from `ModelInput`, applies optional
transforms, and falls back to the heuristic on NaN/≤0 output. The collector (`collector.go`) can dump training rows
to CSV (capped at 100 MB). The model is opaque: no training code or dataset is in the repo.

## 2. Code-quality verdict

**Ambitious, actively maintained, but not production-grade.** The design is sound (score real traffic, keep TCP/UDP
apart, decay old evidence, per-target memory) and the author fixes reported bugs quickly. But:

- **No tests for the decision logic** (section 3). Most of the 50+ "fix:" commits in the log are regressions in
  logic that a table-driven test would have caught (index-out-of-range on provider change `67796d9b`/`0a49e0a9`,
  "incorrect connect and latency score" `b09c220b`/`f4c7e172`, "success twice" `e3ce0d1b`, unaligned atomic panic
  `c8760a5e`, rate-window slice panic `075afcd4`, fallback panic `7d14c4f2`).
- **Large, multi-purpose files** (`smart.go` 2.5k lines, `stats.go` 2k) with heavy use of package-level globals
  (`smartInitOnce`, `flushQueueOnce`, global collector, global store), which makes isolated testing hard.
- **Magic numbers everywhere** (scene thresholds, clamps, TTLs) with no rationale or calibration data in the repo.
- **Fail-hard guard in the main binary**: `main.go:83-95` replaces `net.DefaultResolver.Dial` with "dump all
  stacks + `os.Exit(2)`". Any Smart code path that accidentally dials without a proxy dialer kills the process.
  This is what turned PR #12's bug into a crash loop.
- Commit messages are terse ("chore: refine", "fix typo"); upstream merges from MetaCubeX Alpha arrive every few days.
- `go vet` on the Smart packages: clean. Build: clean, no warnings.

## 3. Test coverage

| Package | Tests |
|---|---|
| `component/smart/tcpstats` | Yes: `tcpstats_test.go`, `tcpstats_unwrap_test.go` (pass) |
| `component/smart` (weight, stats, store, exit, response, target) | **None** |
| `component/smart/lightgbm` | **None** |
| `adapter/outboundgroup` Smart group | **None** (only the pre-existing `loadbalance_test.go`) |
| `adapter.StatusTest` (the crash path) | **None** |

So ~10k of the ~10.9k Smart lines have zero unit tests. Open upstream PRs #10 and #14 contain some tests, but they are
not merged.

## 4. Known bug classes (from commits and PRs; upstream has issues disabled)

| # | Class | Root cause | Status |
|---|---|---|---|
| A | **Process exits repeatedly** (≈52×/day on a 59-group gateway) | `f35e8767` removed `DialContext` from `StatusTest`'s `http.Transport`; a probe that got 302'd to `http://` fell back to the stdlib dialer → `net.DefaultResolver` → `main.go` guard → `os.Exit(2)`. Triggered both by the 30-min host check and by traffic (`checkNodeQuality`). | Fixed by PR #12 (`a866d768`); fix is a restored hook plus a comment, **no regression test**. Guard still in place. |
| B | **Self-inflicted connection churn** | `adoptUnwrapWinner` (smart.go:518) calls `closeSameConnection(..., force=false)` on *every* successful dial: any live connection to the same target through this group on a *different* node is closed (smart.go:2072-2091). Concurrent page loads that win on different nodes kill each other → TLS `unexpected EOF`, reconnect storms. | **Confirmed in code.** PR #15 (open) reports 1/15 success vs 15/15 on a fixed node. |
| C | **Probe judges the wrong thing** | `StatusTest` follows up to 3 redirects to *any* host; 429/407 not treated as failures; a timeout blocks a node for 24 h; transport errors on target re-checks are ignored. | Reported in PRs #14, #17 (open). Not yet reproduced by us. |
| D | **Panics on changing node lists** | Index arithmetic over proxy slices assumed a stable list across provider reloads. | Fixed `67796d9b`, `0a49e0a9`, `9da16ad5`; no tests. |
| E | **Atomic / race bugs** | Unaligned 64-bit atomics on 32-bit, racy callbacks, records races. | Fixed `c8760a5e`, `067637af`, `837e03b8`, `4bdb1fd0`, `3cffe621`; no `-race` CI. |
| F | **Wrong-score bugs** | Connect/latency score formula errors, double-counted success, lastUsed not recorded, average update. | Fixed `b09c220b`, `e3ce0d1b`, `b016da36`, PR #4; no tests. |
| G | **Memory** | h3 stream hijack leak `c8f88da4`; geodata startup memory `1f9b2c2b`; collector size limit ignored `9cb24c6d`. | Fixed; no long-run test. |
| H | **Blackhole misdiagnosis** | PR #16 (closed): TCP with upload>0/download=0. Author found the motivating case was a misdiagnosis; a narrower stalled check (443 or no-Host) exists at smart.go:2013. | Closed. Shows false positives are a real risk. |

New findings from this read (all **confirmed in code**, to be turned into tests/repros in Phase 2/3):

- **LightGBM panic fallback is dead code.** `PredictWeight` (lightgbm.go:685) has unnamed results; the deferred
  `recover()` assigns the fallback to a *local* `prediction`, so after a panic the function returns `(0, false)`, not
  the heuristic weight. A node whose features crash the model silently gets weight 0.
- **Missing measurements score as perfect.** `CalculateWeight` (weight.go:121-135): if connect time or latency is 0
  and the conn did not fail, it is set to **1 ms** — the best possible score — instead of "unknown".
- **Latency resolution is very coarse.** `latencyScore = exp(-ms/1500)` clamped to [0.3, 0.8] (weight.go:139-142):
  every latency below ~335 ms gets the same 0.8. A 30 ms node and a 300 ms node are indistinguishable on latency
  (only a +0.1 "quality bonus" below 100 ms separates them).
- **Lifecycle globals.** `Close()` (smart.go:2555) resets the package-level `smartInitOnce` without synchronization
  and calls `lightgbm.CloseAllCollectors()` from *any* group's close; `flushQueueOnce` is never reset, so only the
  first `Close()` in the process lifetime flushes the queue — after that, up to 5 min of learned stats are lost on each
  reload/shutdown. Process-global tasks are bound to whichever group's context ran `smartInitOnce` first.

## 5. Five biggest reliability risks (ranked)

1. **Any non-proxied dial = process exit.** The `main.go` guard plus untested `StatusTest` / probe code means the next
   refactor that drops a dial hook brings back bug A. Impact: total outage loop. Mitigation: regression test that
   drives `StatusTest` through redirects to http/https/other host and asserts the guard is never hit; convert the guard
   to a logged error in our builds (or keep it only in tests).
2. **Success-path sibling close (bug B).** Smart actively kills healthy connections, TCP *and* UDP. For a Zoom call
   (long UDP flow to the same target), any new flow to that target that wins on a different node closes the call's
   media flow. Impact: user-visible drops that look like "bad node". Mitigation: remove non-forced close (PR #15
   approach), keep only forced close on confirmed degradation.
3. **No tests on scoring/selection.** Every formula change risks silently mis-ranking nodes; the fix log shows this
   happening repeatedly. Mitigation: Phase 2 table-driven tests + fake dialers.
4. **False-positive blocking via probes.** Cross-host redirects, timeouts → 24 h block, ignored 429s, and the "missing
   data = 1 ms" rule can both block good nodes and favour unmeasured ones. Impact: wrong node for hours.
5. **State lifecycle on reload.** Unsynchronized global `sync.Once` reset, collectors closed by one group, queue flush
   only once per process, background tasks tied to one group's context. Impact: lost learning, races on config
   reload (frequent on routers/GUIs). Mitigation: move globals into an explicitly owned manager; `-race` tests that
   reload repeatedly.

## 6. UDP scoring today vs what Zoom-grade selection needs

**What it does today** (confirmed):

- Separate key space (`udp|target`), so UDP history doesn't mix with TCP.
- Same `CalculateWeight` with mild re-weighting for UDP (weight.go:144-148): success-rate weight ×1.1 (cap 0.6),
  latency weight ×1.2 (cap 0.5).
- "Latency" for UDP = **one sample**: time from the first `WriteTo` to the first `ReadFrom` of the session
  (`common/callback/read_callback.go:75-90`).
- **Everything is recorded only when the session closes** (`registerPacketClosureMetricsCallback`, smart.go:1714).
  A 60-minute call contributes nothing until it ends.
- No packet loss, no jitter for UDP (`LossRate` is TCP-only via kernel `tcpstats`; the field comment says
  "0 = … UDP").
- "Interactive" scene detection exists (weight.go:268) but only changes weights *after the fact*.
- Dial "success" for UDP = `ListenPacketContext` returned, which for most protocols does not prove the path carries UDP.

**Why it would not have saved the 11-09 Zoom call:** while the call was running, the congested HK node had *no new
evidence against it*: no session had closed yet, no loss/jitter was measured, and latency below ~335 ms scores the
same. It would also have stayed pinned via the unwrap cache. The extra risk is bug B, which could have cut the
call outright.

**What Zoom-grade selection needs** (engine-agnostic; same list applies to Cain/sing-box):

1. **In-session sampling**: periodic (e.g. every 5–10 s) per-flow stats for long UDP flows — packets in/out,
   inter-arrival jitter, gaps — fed into the node score while the flow is alive, not only at close.
2. **Real UDP RTT/loss**: an active UDP probe per node (e.g. a STUN binding request to a known STUN server through
   the node, every N seconds while a real-time flow is active) giving RTT, jitter and loss. STUN is what Zoom/WebRTC
   use, so it's a faithful signal.
3. **Finer latency scoring**: no 0.8 clamp below 335 ms; for the interactive scene, score jitter and p95 RTT, not mean.
4. **Make-before-break migration**: for UDP you cannot move a flow, but new flows (and the client's own reconnect) must
   go to the better node, and the bad node must be demoted for that target class quickly (seconds, not minutes).
5. **Never close healthy flows** on another flow's success (fix bug B first).
6. **Classify real-time traffic early** (port ranges / SNI / ASN of Zoom, Teams, Meet, WebRTC) so the stricter
   UDP policy applies from the first packet, not after a 3-minute "interactive" scene threshold.

## 7. Pinning Smart onto upstream's stable line instead of Alpha

Upstream stable = MetaCubeX `Meta` branch, tag **v1.19.33** (2026-10-21, equals `metacubex/Meta` HEAD).
`metacubex/Alpha` is only **14 commits** ahead of it (sudoku/snell/ss features, tun route fix, tfo-go fix).

Experiment (done in Phase 1): applied the full Smart diff (`git diff metacubex/Alpha...vernesong/Alpha`) with
`git apply --3way` onto `v1.19.33` → **applied cleanly**; `go build .` → **succeeded**. The only files touched by
both the stable→Alpha delta and Smart are `go.mod`/`go.sum`.

What would have to change to make this the default:

1. Maintain Smart as a **patch series** (or a branch rebased on each stable tag), not as merges of upstream Alpha.
   Practical shape: branch `smart-stable` = `v1.19.x` tag + squashed/curated Smart commits.
2. Keep a small compatibility shim list: the Smart hooks into `tunnel/statistic`, `common/callback`,
   `adapter/outboundgroup/groupbase.go`, `config/config.go`, `hub/route/*`, `constant/*`. These are where future
   stable releases are most likely to conflict.
3. CI job that re-applies the series onto the newest stable tag weekly and runs build + tests; a failed apply is the
   signal to port.
4. Drop or vendor Alpha-only dependencies if any appear (none today beyond `go.mod`).
5. Version string: builds currently report `Mihomo Meta 1.10.0 … unknown time`; set `-ldflags` with the stable tag +
   Smart commit so deployed binaries are identifiable.

## 8. Build on this Mac

- `go build -o mihomo .` on darwin/arm64, Go 1.27.2: **19.8 s wall** (74 s user; includes module downloads), exit 0,
  no warnings.
- `./mihomo -v` → `Mihomo Meta 1.10.0 darwin arm64 with go1.27.2 unknown time`.
- `go vet ./component/smart/... ./adapter/outboundgroup/...` → clean.
- `go test ./component/smart/... ./adapter/outboundgroup/...` → pass (only tcpstats + loadbalance tests exist).

## 9. Phase 2 addendum — test harness and new findings

Base changed: `smart-hardening` is now `v1.19.33` (upstream stable) + one squashed Smart commit
(`git diff metacubex/Alpha...vernesong/Alpha` at `96feb2a1`, `.github/` excluded). Smart source files are
byte-identical to `vernesong/Alpha@96feb2a1`, so the line numbers above still hold.

Tests added (all table-driven, fake nodes, no network):

| File | Covers |
|---|---|
| `adapter/statustest_test.go` | PR #12 crash class: `StatusTest`, `StatusProbe`, `TargetExitProbe` driven through https→http, cross-host and 3-hop redirects with a recording copy of the `main.go` resolver guard. Verified to FAIL (7/7) when the `DialContext` hook is removed. |
| `adapter/outboundgroup/smart_test.go` | dial batch bounds, `ParallelDialContext` (winner, failures, all-fail, deadline, no leaked conns), TCP/UDP fail-over through `DialContext`/`ListenPacketContext`, same-node siblings kept, and bug B (cross-node close) for UDP and TCP. |
| `component/smart/weight_test.go` | min samples, monotonicity (failures, connect, latency, loss, staleness; TCP and UDP), priority scaling, degenerate inputs stay finite, time-decay table, scene table, traffic-factor bounds, known bugs W1/W2. |

`knownBug(...)` markers: a test for a confirmed, unfixed defect skips while the defect reproduces and **fails as
soon as it stops reproducing**, so the fix must also convert it into a normal assertion. Current markers:
B-UDP, B-TCP (smart.go:518), W1 (missing measurement = 1 ms), W2 (latency clamp). Each was checked to flip.

New findings while building the harness (confirmed; to be ranked in Phase 3):

- **R1 — data race on `Metadata.SmartBlock`.** `closeSameConnection(force=true)` writes
  `tracker.Info().Metadata.SmartBlock = "degraded"` (smart.go:2085) on *another* connection's metadata while that
  connection's close callback reads it (smart.go:1704). Caught by `go test -race` in a cross-node scenario.
- **R2 — forced close is not scoped to the failing node.** On a confirmed failure (`failedBlock`) the TCP path calls
  `closeSameConnection(..., force=true)` (smart.go:1638), and with `force` every connection of the target in the
  group is closed regardless of which node it uses — including UDP flows to the same target key. A dial failure on
  node B can therefore close healthy connections on node A.
- **Observation:** a >100 MB stream at >5 MB/min is classified `sceneTransfer`, not `sceneStreaming`, because the
  transfer check runs first (weight.go:284 vs :290). Documented in the scene test; not yet judged a bug.
