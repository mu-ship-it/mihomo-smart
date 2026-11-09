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

## 10. Phase 3 bug hunt — ranked findings (hunt pass, before any fix)

Adversarial scenarios run with fake nodes: node flaps, provider reload mid-connection, all nodes
failing then recovering, UDP-only traffic, and a 200k-connection "48 h" memory run (heap profiled).
Each confirmed defect has a repro test that is a `knownBug` marker until fixed.

| Rank | ID | Finding | Impact | Repro |
|---|---|---|---|---|
| 1 | **B** | Every successful dial closes the target's live connections on *other* nodes (`adoptUnwrapWinner` → `closeSameConnection(force=false)`, smart.go:518). | Kills healthy Zoom UDP media and concurrent page loads whenever a new flow lands elsewhere (e.g. after one refused dial drops the pin). | `TestSmartNewFlowOnOtherNodeMustNotKillHealthy{UDPSession,TCPConn}` |
| 2 | **R2** | A confirmed failure of node X force-closes the target's connections on **every** node, incl. UDP (`force=true` ignores `proxyName`; smart.go:1638/2083). | One bad node kills healthy calls on good nodes. Measured: b failing closed a's TCP conn *and* UDP call. | `TestHuntR2FailureOnOneNodeMustNotCloseOtherNodes` |
| 3 | **U1** | A node that accepts UDP sessions but never returns a packet is scored healthy: no error at close, latency 0 → treated as 1 ms. | Smart keeps sending Zoom/WebRTC to a UDP-dead node. Measured: 6 silent sessions → weight 1.14 (top). | `TestHuntU1UDPBlackholeMustNotScoreAsHealthy` |
| 4 | **R1** | Data race on `Metadata.SmartBlock`: written on other connections (smart.go:2085, 1202, hub API) while close callbacks / stats read it (1704, 1730, 1597, 1985). | Torn string reads can crash the process; wrong "degraded" accounting. | `SMART_HUNT_RACE=1 go test -race -run TestHuntR1` (opt-in until fixed) |
| 5 | **LC** | Lifecycle: a config reload builds the new groups before closing the old ones, so the 3 process-global tasks (queue flush, cache tuning, orphan clean-up) die with the old group's context and never restart; `flushQueueOnce` means only the first `Close` ever flushes queued learning; `smartInitOnce` is reset unsynchronized; any group's `Close` closes all LightGBM collectors. | After the first API reload: learning stops being persisted on time and is dropped at shutdown; orphan records never cleaned. | `TestHuntLCGlobalTasksSurviveConfigReload`, `TestHuntLCEveryCloseFlushesQueuedLearning` |
| 6 | **L1** | LightGBM: `recover()` covers only `PredictSingle`; panics in `NFeatures`/transforms escape into the stats goroutine (process exit), and a recovered panic returns weight 0 instead of the heuristic. | With `uselightgbm`, a bad model file can crash the core or zero every node. | `TestPredictWeightFallsBackToHeuristicOnModelPanic` |
| 7 | **M1** | `ProbeThrottle` maps are trimmed only by age (10 min), not by count. | A burst of unique targets (P2P/DHT, scanners) grows memory: 42.7 MB per 200k targets in the profile. All other caches stayed flat (LRU-bounded): +2.1 MB at 50k→200k conns. | `TestProbeThrottleHasAHardCap` |
| 8 | **W2** | Latency/connect score clamped at 0.8: every latency below ~335 ms scores the same. | Cannot prefer a 30 ms node over a 300 ms one — the Zoom-relevant range. | `TestCalculateWeightDistinguishesLatencyBelow335ms` |
| 9 | **W1** | A connection with no first-read latency (never received a byte) is scored as 1 ms. (Connect time 0 is a real sub-ms dial on multiplexed transports and stays fast.) | Unanswered connections look like the best ones; feeds U1. | `TestCalculateWeightMissingLatencyIsNotBest` |

Scenarios that held up (no defect): all nodes failing then recovering (first dial after recovery succeeds);
a flapping node (traffic shifts to the stable node: 36 of 40 dials); provider reload removing the pinned node
(fails over, re-adds cleanly; an empty provider falls back to `COMPATIBLE` as configured); no goroutine growth
over 200k connections.

Not bugs, kept as feature backlog (Zoom-grade selection, §6): UDP stats only at session close; no UDP
jitter/loss; no active STUN probe. Observation only: >100 MB streams classified as transfer.

## 11. Phase 3 fix pass

Every finding in §10 is fixed, and its marker is now an ordinary regression test. Each test was checked
three ways: it fails on the pre-fix code (`57c6540a`), it passes with the fix, and it fails again when only
that fix is reverted (mutation check).

| ID | Fix | Regression test(s) |
|---|---|---|
| B | `adoptUnwrapWinner` only updates the pin; it no longer closes live connections on other nodes. | `TestSmartNewFlowOnOtherNodeMustNotKillHealthy{UDPSession,TCPConn}` |
| R2 | `closeSameConnection(force)` → `closeNodeConnections`: closes only the failing node's connections to the target, and only those on the failure's network (a TCP failure no longer drops a UDP call). | `TestHuntR2…`, `TestSmartFailingNodeOwnConnectionsAreStillClosed` |
| U1 | A UDP session that sent data for ≥ 4 s (`stalledConnectionGrace`) and never received a packet closes with `errUDPNoReply`, so it counts as a failure, drops the pin and steers new flows elsewhere. | `TestHuntU1UDPBlackholeMustNotScoreAsHealthy` |
| R1 | `Metadata.GetSmartBlock/SetSmartBlock`, guarded by a lock that `Clone`/`Pure` also take; all cross-goroutine readers and writers use them (Smart, tunnel, hub API). | `TestHuntR1ForcedCloseRacesCloseCallback` (CI runs it with `-race`) |
| LC | Process-global tasks are reference-counted (`acquireGlobals`/`releaseGlobals`), so they survive a reload and stop with the last group. Every `Close` flushes the queue, `Close` is idempotent, and collectors close only with the last group. `InitCollector` closes the collector it replaces, groups fetch the current collector per sample, and `DataCollector.Close` is idempotent. | `TestHuntLCGlobalTasksSurviveConfigReload`, `TestHuntLCEveryCloseFlushesQueuedLearning` |
| L1 | `PredictWeight` has named results and one `recover` around the whole model path, falling back to the heuristic weight. | `TestPredictWeightFallsBackToHeuristicOnModelPanic` |
| M1 | `ProbeThrottle.enforceCap`: once a map reaches 2 × 4096 entries, the oldest entries are evicted down to 4096. In-flight or suspected entries count as recent activity. | `TestProbeThrottleHasAHardCap` |
| W2 | Delay score = `0.3 + 0.5·e^(−ms/scale)·decay`, with scale 400 ms for latency and 600 ms for connect. Same [0.3, 0.8] range, but 30/120/330/1000 ms now score 0.76/0.67/0.52/0.34 instead of a flat 0.8 below about 335 ms. | `TestCalculateWeightDistinguishesLatencyBelow335ms` |
| W1 | A first-read latency of 0 is "unknown": it gets a neutral 0.55 and no low-latency bonus. It is still scored above a failed connection. A connect time of 0 is unchanged (a real sub-ms dial). | `TestCalculateWeightMissingLatencyIsNotBest` |

Behaviour notes for operators:
- **W2 changes ranking within the existing range.** Nodes in the 100–400 ms band now differ by up to about 0.15
  in delay score where they used to tie. Weights of slow nodes drop slightly, so a node already near
  `AllowedWeight` (0.4) can cross it sooner.
- **U1 can mark legitimately one-way UDP as failed.** This only affects a session that is ≥ 4 s long and
  received zero packets, and it is scoped per target. Rare through a proxy, but possible (e.g. syslog
  forwarding).
- **R1 leaves one unguarded reader.** The hub `/connections` JSON snapshot reads `SmartBlock` without the
  lock, as it does for every other field. This is an upstream pattern and outside Smart.

## 12. Phase 4 — Zoom-grade UDP scoring and history (design notes for Zoom and Cain)

**Why.** On 11-09 the Zoom call lagged on a node that passed TCP probes. Smart scored UDP from one first-reply latency
per session and recorded nothing until the session closed (§6), so a call degrading in the middle was invisible to the
selector. Phase 4 adds three things, each usable on its own: live UDP sampling, an active STUN probe, and a timestamped
outcome history for a later seasonal model.

### 12.1 What was built

**Live UDP sampling** (`component/smart/udplive.go`, `adapter/outboundgroup/smart_udp.go`)
- Every UDP session through the group gets a `UDPMeter` on its packet path. The meter counts packets and bytes each
  way, the longest reply gap, and an RFC 3550-style smoothed inter-arrival jitter. Spacing above 500 ms counts as
  silence (a muted mic), not jitter.
- Every `udp-sample-interval` (default 2 s), one goroutine per group cuts every live session into a window:
  - **Loss** is judged from the window's reply ratio (packets in ÷ packets out) against the best ratio recently seen
    for the same target on any node of the group (`UDPReplyReference`, bounded at 2048 targets, LRU). If one node
    answers the same application less often than another, it is losing packets.
  - A window with fewer than 8 packets sent is not judged.
  - A session that had replies and now gets none counts as loss 1.
  - A session that never got any reply is left to the close path (U1), since it may be one-way.
- Loss and jitter go into EMAs on the node's record for the target (`udp_loss`, `udp_jitter_ms`, `udp_samples`,
  persisted with the stats).
- The node's **UDP weight is then recomputed on the spot** (the heuristic score, then the factor below). TCP weight is never touched.
- The UDP weight is multiplied by
  `UDPLiveFactor = (1 − 0.6·(1 − e^(−12·loss))) · (1 − 0.3·(1 − e^(−jitter/50 ms)))`:
  - 2 % loss → ×0.87; 5 % → ×0.73; 10 % → ×0.58; 20 % → ×0.45
  - 30 ms jitter → ×0.86
  - floor ×0.28
- The same factor applies when the session closes, and to the LightGBM path (applied after the model, which does not
  know these features).
- Once a node's live loss reaches 10 % or its jitter reaches 60 ms, and it is the target's UDP pin, the pin and the
  prefetched ranking are dropped. The next UDP flow (including the client's own reconnect) is then placed on the
  current ranking. A UDP flow cannot be moved while it runs, so this is the make-before-break step from §6.4.
- Live windows do not change success/failure counts, since a 1-hour call is one session, not 1,800. A target first
  seen in the open session is scored as if it had the minimum two samples, so it does not wait for its first close.

**Active STUN probe** (`component/smart/stun.go`)
- Sends RFC 5389 binding requests through one node to a server list:
  - default `stun.l.google.com:19302` and `stun.cloudflare.com:3478`
  - 50 packets per second, round robin across the servers
  - each request has a random transaction id
- Replies are matched one by one, which gives **real loss**, RTTs (mean and minimum), jitter (mean absolute
  difference of consecutive RTTs) and the mapped address (the node's public exit).
- Server names are resolved with mihomo's own resolver, never the system resolver (the PR #12 crash class).
- Results go through the same `recordUDPQuality` as live windows. Each probe counts as one sample: a success if any
  reply came back and loss < 50 %. They also update latency.
- Two entry points:
  - `POST /group/{name}/probe`: runs the probe across the group's nodes, at most 4 at a time and one probe per group
    at a time ("pre-meeting warm-up").
  - **Automatic probing**: every 10 minutes it probes the most recently active UDP target on its best 3 nodes. It is
    capped by `udp-probe-budget` node probes per clock hour and is **off by default** (budget 0).

**Timestamped history** (`component/smart/history.go`)
- Every recorded outcome is kept with its timestamp, node, network, ok/failed and the latency, loss and jitter it
  measured: closed connections (`conn`), live windows (`live`) and probes (`probe`).
- Storage is one chunk per target and UTC hour (`smart/history/<config>/<group>/<target>/h<unix hour>`) in the
  existing bbolt store, written through the existing write queue:
  - each chunk keeps up to 64 raw events plus exact per node and network sums, so a busy hour stays small while its
    totals stay exact
  - the current hour is in memory and flushed every minute and on close
- Retention: chunks older than `history-retention` (default 8 weeks, i.e. two weekly seasons) and, past
  `history-max-chunks` (default 8192 per group), the oldest hours are pruned hourly.
- A group flush via the API deletes its history too.
- `GET /group/{name}/history?target=…[&network=tcp|udp][&node=…][&tz=IANA]` returns per-hour-of-week aggregates:
  - bucket 0 is Monday 00:00 in `tz`, default the process's zone
  - per node and network it reports count, successes, failures, success rate, mean latency, loss and jitter
  - `weeks` is the number of distinct calendar hours behind each bucket
- There is no prediction yet. This is the input a Holt-Winters fit with a 168-hour season needs.

### 12.2 Knobs and defaults (group options, `type: smart`)

| Option | Default | Meaning |
|---|---|---|
| `udp-sample-interval` | `2000` (ms) | Live window length. `-1` turns live sampling off. |
| `udp-probe-servers` | Google + Cloudflare STUN | `host:port` list for the probe (API and automatic). |
| `udp-probe-budget` | `0` (off) | Automatic node probes per clock hour. Each probes 3 s at 50 packets/s, about 8 KB each way. |
| `history-retention` | `1344` (h, 8 weeks) | History age cap. |
| `history-max-chunks` | `8192` | History chunk cap per group (one chunk = one target-hour, at most ~6 KB). |

Fixed internals worth knowing:
- minimum 8 packets in a window to judge loss
- 2 % loss dead zone
- unpin at 10 % loss or 60 ms jitter
- at most 1,024 sampled sessions per group (more are scored at close only)
- at most 1,024 history chunks pending in memory per group
- probe duration 200 ms to 30 s (default 5 s)

**Cost.** The meter adds about 14 ns and no allocations per packet (`BenchmarkUDPMeterPacket`, M-series Mac), plus
one clock read. Sampling is one goroutine per group, waking every 2 s. Memory is bounded by the session, reference and
pending-chunk caps above.

### 12.3 API contracts

`POST /group/{name}/probe`
- Body: `{"target": "zoom.us" | "<weights key>", "duration": "5s" | "duration_ms": 5000, "servers": [...]?, "nodes": [...]?}`.
- `target` is the key the weights are stored under. A host is mapped the way a connection to it without a matching
  rule would be (`zoom.us` → `*.zoom.us`). A key that contains a space or `[` (a rule key) is used as is.
- Responses:
  - **200**: `{group, target, duration_ms, servers, results: [{node, sent, received, loss, rtt_ms, min_rtt_ms, jitter_ms, mapped, error?, started_at, weight_before, weight_after, skipped?}]}`
  - **400**: not a Smart group, bad JSON, no target, bad or out-of-range duration, or no usable STUN server
  - **404**: unknown group
  - **409**: a probe is already running
  - **405**: wrong method

`GET /group/{name}/history`
- **200**: `{group, target, timezone, utc_offset_minutes, from, to, hours_covered, events, buckets: [{hour_of_week, weekday, hour, weeks, nodes: [{node, network, count, success, failure, success_rate, avg_latency_ms?, avg_loss?, avg_jitter_ms?}]}]}`
- **400**: missing target, bad network, unknown tz, or not a Smart group
- **404**: unknown group

### 12.4 Tests (all under `-race` in CI)

- `TestSmartLiveUDPSamplingDemotesLossyNodeWhileCallRuns`:
  - Two simultaneous calls to `zoom.us` over deterministic fake links, at 200 packets/s with 100 ms sampling. The
    lossy link drops every 4th reply and swings its delay between 5 and 35 ms every 8 packets.
  - The lossy node's UDP weight falls below the clean one's within N = 3 s (measured about 0.3 s, i.e. 3 windows).
  - The clean node leads the UDP ranking, and both TCP weights are bit-identical before and after.
  - Typical estimates: lossy loss 0.29 (0.25 injected; replies still in flight at a cut push it up), jitter 5.7 ms
    against 0.2 ms clean.
- Live-sampling bounds: sampling can be turned off, and sessions are released on close.
- Probe:
  - real local STUN server with scripted loss and delay (`component/smart/stuntest`)
  - a lossy uplink (every 4th request dropped) measures 25 % loss and ends with a lower UDP weight than a clean node
  - one history event per node
  - the rejections (no target, duration, servers, busy)
- `TestSmartAutoProbeStaysWithinHourlyBudget`: budget 2 with 3 nodes → 2 probes, then 0 in the same hour, 2 the next
  hour, and 0 with the default budget.
- API contract tests in `hub/route` (DIRECT nodes, local STUN):
  - every status code above
  - the report and bucket shapes
  - the filters
  - the probe's effect on the selector's ranking
- Unit tests:
  - meter counts, cuts, jitter (even spacing ≈ 0; a silence is not jitter), open gaps
  - reference judging (noise dead zone, outage, one-way)
  - factor shape
  - STUN encode and parse (IPv4 and IPv6 XOR-MAPPED-ADDRESS)
  - history aggregation, time zones, event cap with exact totals, retention and chunk-cap pruning, `/` in target keys

### 12.5 Limits (stated honestly)

- **Loss is relative.** With only one node ever carrying a target, a path that loses from the first packet looks
  normal to the live estimate; gaps, jitter, the U1 close rule and the probe still see it. Applications with very
  asymmetric or bursty request/response patterns can show reply-ratio noise; the dead zone and EMA damp it, but it is
  an estimate, not a sequence-number count.
- **Jitter is receive-side spacing.** Reordering inside a dense stream does not show; queue build-up (bursts and gaps)
  does. RTP timestamps would be exact but need parsing the media protocol.
- **A running UDP flow is not moved.** The group only stops steering new flows to the bad node.
- The probe measures the path to the STUN servers, not to Zoom's media servers. It is a good proxy for "this node's UDP
  is healthy right now", not an exact one.
- History hours are UTC chunks; in a zone with a 30- or 45-minute offset a chunk is filed under the local hour it
  starts in.
- Not done from §6: early classification of real-time traffic (§6.6).

### 12.6 How Cain (sing-box) would reuse this

The three pieces are engine-neutral:
- `udplive.go` (meter, reference, factor) and `stun.go` (probe) depend only on the Go standard library.
- `history.go` needs only a key-value store with prefix scans.

For Cain:
1. **Meter:** wrap the `N.PacketConn` that sing-box's `urltest`/selector outbound hands to the router, in the same
   place Smart wraps it (`WriteTo` = sent, `ReadFrom`/`ReadPacket` = received). Keep it non-replaceable so the
   zero-copy fast path does not bypass it.
2. **Scoring:** keep a per (outbound, destination key) UDP score and multiply it by `UDPLiveFactor`. Use the same
   unpin rule for the outbound chosen for that destination.
3. **Probe:** call `outbound.ListenPacket` toward the STUN server and run `RunSTUNProbe` over it. On iOS, run it from
   the Network Extension, inside the extension's memory budget: one probe at a time, at most 30 s.
   - Expose "warm up before a meeting" as an app action (the equivalent of `POST /probe`).
   - Run automatic probing only on Wi-Fi or with a small budget.
4. **History:** store the hour chunks in the app's cache DB with the same retention, and fit the weekly seasonal model
   on-device or on the Mini later.

The defaults above are the ones to start Cain with.
