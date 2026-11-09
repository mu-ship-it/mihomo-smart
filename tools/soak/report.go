package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// dayData is everything recorded on one local day.
type dayData struct {
	day  string
	recs []Rec
}

func loadDays(c *Config) ([]dayData, error) {
	files, err := filepath.Glob(filepath.Join(c.DataDir, "*.jsonl"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	var out []dayData
	for _, f := range files {
		fh, err := os.Open(f)
		if err != nil {
			continue
		}
		d := dayData{day: strings.TrimSuffix(filepath.Base(f), ".jsonl")}
		sc := bufio.NewScanner(fh)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			var r Rec
			if json.Unmarshal(sc.Bytes(), &r) == nil {
				d.recs = append(d.recs, r)
			}
		}
		_ = fh.Close()
		sort.SliceStable(d.recs, func(i, j int) bool { return d.recs[i].TS < d.recs[j].TS })
		out = append(out, d)
	}
	return out, nil
}

// instStats summarises one instance over a set of records.
type instStats struct {
	httpN, httpOK         int
	ttfb, total           []float64
	switches              int
	stunN, stunOK         int
	stunLoss, stunJit     []float64
	stunRTT               []float64
	calls                 []Rec
	cpuPeak, memPeak      float64
	restartsMin, restarts int
	restartSeen           bool
	inactive              int
	groupNow              []string
	errs                  map[string]int
	perSite               map[string]*siteStats
}

type siteStats struct {
	n, ok int
	ttfb  []float64
	nodes map[string]int
}

func newInstStats() *instStats {
	return &instStats{errs: map[string]int{}, perSite: map[string]*siteStats{}}
}

func summarize(recs []Rec, inst string) *instStats {
	s := newInstStats()
	lastNode := map[string]string{}
	lastNow := ""
	for _, r := range recs {
		if r.Inst != inst {
			continue
		}
		switch r.Kind {
		case "http":
			s.httpN++
			ss := s.perSite[r.Target]
			if ss == nil {
				ss = &siteStats{nodes: map[string]int{}}
				s.perSite[r.Target] = ss
			}
			ss.n++
			if r.OK {
				s.httpOK++
				ss.ok++
				s.ttfb = append(s.ttfb, r.TTFBMs)
				s.total = append(s.total, r.TotalMs)
				ss.ttfb = append(ss.ttfb, r.TTFBMs)
			} else {
				s.errs[errClass(r.Err)]++
			}
			if r.Node != "" {
				ss.nodes[r.Node]++
				if p, ok := lastNode[r.Target]; ok && p != r.Node {
					s.switches++
				}
				lastNode[r.Target] = r.Node
			}
		case "stun":
			s.stunN++
			if r.OK {
				s.stunOK++
				s.stunRTT = append(s.stunRTT, r.RTTMs)
				s.stunJit = append(s.stunJit, r.JitterMs)
			}
			if r.Sent > 0 {
				s.stunLoss = append(s.stunLoss, r.Loss)
			}
		case "call":
			s.calls = append(s.calls, r)
		case "sys":
			if r.CPUPct > s.cpuPeak {
				s.cpuPeak = r.CPUPct
			}
			if r.MemMB > s.memPeak {
				s.memPeak = r.MemMB
			}
			if !s.restartSeen {
				s.restartsMin, s.restartSeen = r.Restarts, true
			}
			if r.Restarts > s.restartsMin+s.restarts {
				s.restarts = r.Restarts - s.restartsMin
			}
			if r.Active != "" && r.Active != "active" {
				s.inactive++
			}
			if r.GroupNow != "" {
				if lastNow != "" && r.GroupNow != lastNow {
					s.groupNow = append(s.groupNow, r.GroupNow)
				}
				lastNow = r.GroupNow
			}
		}
	}
	return s
}

func errClass(e string) string {
	switch {
	case e == "":
		return "unknown"
	case strings.Contains(e, "timeout") || strings.Contains(e, "deadline"):
		return "timeout"
	case strings.Contains(e, "reset"):
		return "reset"
	case strings.Contains(e, "EOF"):
		return "EOF"
	case strings.Contains(e, "refused"):
		return "refused"
	default:
		if len(e) > 60 {
			e = e[:60]
		}
		return e
	}
}

func (s *instStats) okPct() float64 {
	if s.httpN == 0 {
		return 0
	}
	return float64(s.httpOK) / float64(s.httpN) * 100
}

func (s *instStats) callAgg() (loss, jit, rtt, worst float64, n int) {
	var l, j, r []float64
	for _, c := range s.calls {
		if c.Sent == 0 {
			continue
		}
		l = append(l, c.Loss)
		if c.Recv > 0 {
			j = append(j, c.JitterMs)
			r = append(r, c.RTTMs)
		}
		if c.WorstLoss > worst {
			worst = c.WorstLoss
		}
	}
	return mean(l), mean(j), mean(r), worst, len(l)
}

func lastExt(recs []Rec) float64 {
	v := 0.0
	for _, r := range recs {
		if r.Kind == "budget" {
			v = r.ExtMBDay
		}
	}
	return v
}

func haAgg(recs []Rec) (med, max float64, n int) {
	var v []float64
	for _, r := range recs {
		if r.Kind == "ha" && r.HAms >= 0 && r.OK {
			v = append(v, r.HAms)
			if r.HAms > max {
				max = r.HAms
			}
		}
	}
	return pct(v, 0.5), max, len(v)
}

func names(c *Config) (string, string) {
	a, b := "smart", "stock"
	if len(c.Instances) >= 2 {
		a, b = c.Instances[0].Name, c.Instances[1].Name
	}
	return a, b
}

// dailyLine is the one-line summary of a finished day.
func dailyLine(c *Config, loc *time.Location, day string) string {
	days, err := loadDays(c)
	if err != nil {
		return ""
	}
	idx := -1
	for i, d := range days {
		if d.day == day {
			idx = i
		}
	}
	if idx < 0 {
		return ""
	}
	recs := days[idx].recs
	a, b := names(c)
	sa, sb := summarize(recs, a), summarize(recs, b)
	la, ja, _, _, na := sa.callAgg()
	lb, jb, _, _, nb := sb.callAgg()
	ham, hamax, _ := haAgg(recs)
	return fmt.Sprintf("Soak %s (day %d): HTTP ok %s %.1f%% / %s %.1f%%, TTFB p50 %.0f/%.0f ms, p95 %.0f/%.0f ms; "+
		"calls (%d/%d) loss %.1f%%/%.1f%%, jitter %.1f/%.1f ms; node switches %d/%d; restarts %d/%d; "+
		"CPU peak %.1f/%.1f%%, RAM peak %.0f/%.0f MB; traffic %.0f MB; HA median %.1f ms (max %.1f).",
		day, idx+1, a, sa.okPct(), b, sb.okPct(),
		pct(sa.ttfb, 0.5), pct(sb.ttfb, 0.5), pct(sa.ttfb, 0.95), pct(sb.ttfb, 0.95),
		na, nb, la*100, lb*100, ja, jb, sa.switches, sb.switches+len(sb.groupNow), sa.restarts, sb.restarts,
		sa.cpuPeak, sb.cpuPeak, sa.memPeak, sb.memPeak, lastExt(recs), ham, hamax)
}

func writeReport(c *Config, loc *time.Location, final bool) error {
	days, err := loadDays(c)
	if err != nil {
		return err
	}
	a, b := names(c)
	st := loadState(c)
	var all []Rec
	for _, d := range days {
		all = append(all, d.recs...)
	}

	var w strings.Builder
	p := func(f string, args ...any) { fmt.Fprintf(&w, f, args...) }
	p("# Soak report: %s vs %s\n\n", a, b)
	p("Updated %s (Asia/Shanghai).", time.Now().In(loc).Format("2006-01-02 15:04"))
	if st.Start > 0 {
		p(" Started %s; planned length %d days.", time.UnixMilli(st.Start).In(loc).Format("2006-01-02 15:04"), c.Days)
	}
	p("\n\n")
	if st.Stopped != "" {
		p("**Stopped:** %s\n\n", st.Stopped)
	}
	p("`%s` is our build from smart-hardening (Smart group). `%s` is upstream stable v1.19.33 (url-test group over the same nodes). "+
		"Both listen on 127.0.0.1 only. Load goes through each instance alternately, in random order: HTTP fetches every %s to %s, "+
		"a short STUN session every %s, and a %s simulated call (~%d kbps up, 50 packets/s) at %s, run on both instances at once. "+
		"HTTP success means any HTTP answer came back. Latency is time to first byte through the proxy.\n\n",
		a, b, c.HTTPEvery, strings.Join(c.Sites, ", "), c.STUNEvery, c.CallDuration, c.CallKbps, strings.Join(c.CallTimes, ", "))

	sa, sb := summarize(all, a), summarize(all, b)
	p("## Whole soak so far\n\n")
	statsTable(&w, a, b, sa, sb, haLine(all, st.HABaseline))

	p("\n## Per day\n\n")
	p("| Day | HTTP ok %s / %s | TTFB p50 | TTFB p95 | Call loss | Call jitter | Switches | Restarts | CPU peak | RAM peak MB | Traffic MB | HA median ms |\n", a, b)
	p("|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, d := range days {
		x, y := summarize(d.recs, a), summarize(d.recs, b)
		lx, jx, _, _, _ := x.callAgg()
		ly, jy, _, _, _ := y.callAgg()
		ham, _, _ := haAgg(d.recs)
		p("| %s | %.1f%% / %.1f%% | %.0f / %.0f | %.0f / %.0f | %.1f%% / %.1f%% | %.1f / %.1f | %d / %d | %d / %d | %.1f%% / %.1f%% | %.0f / %.0f | %.0f | %.1f |\n",
			d.day, x.okPct(), y.okPct(), pct(x.ttfb, 0.5), pct(y.ttfb, 0.5), pct(x.ttfb, 0.95), pct(y.ttfb, 0.95),
			lx*100, ly*100, jx, jy, x.switches, y.switches+len(y.groupNow), x.restarts, y.restarts,
			x.cpuPeak, y.cpuPeak, x.memPeak, y.memPeak, lastExt(d.recs), ham)
	}

	p("\n## Calls\n\n")
	p("| Call | Instance | Node | Loss | Worst %ds window | Longest loss burst | Jitter ms | RTT mean / p95 ms |\n", c.CallWindowSec)
	p("|---|---|---|---|---|---|---|---|\n")
	for _, r := range all {
		if r.Kind != "call" {
			continue
		}
		node := r.Node
		if node == "" {
			node = "?"
		}
		p("| %s | %s | %s | %.1f%% | %.1f%% | %d pkts | %.1f | %.0f / %.0f |\n", r.CallID, r.Inst, node, r.Loss*100, r.WorstLoss*100, r.MaxBurst, r.JitterMs, r.RTTMs, r.RTTP95Ms)
	}

	p("\n## Per site (whole soak)\n\n")
	p("| Site | %s ok | %s TTFB p50 / p95 | %s top nodes | %s ok | %s TTFB p50 / p95 | %s top nodes |\n", a, a, a, b, b, b)
	p("|---|---|---|---|---|---|---|\n")
	for _, site := range c.Sites {
		x, y := sa.perSite[site], sb.perSite[site]
		p("| %s | %s | %s | %s | %s | %s | %s |\n", site, siteOK(x), siteLat(x), topNodes(x), siteOK(y), siteLat(y), topNodes(y))
	}

	p("\n## Failures by kind (whole soak)\n\n")
	p("- %s: %s\n- %s: %s\n", a, errList(sa.errs), b, errList(sb.errs))

	p("\n## Events\n\n")
	n := 0
	for _, r := range all {
		if r.Kind == "event" {
			p("- %s %s\n", time.UnixMilli(r.TS).In(loc).Format("01-02 15:04"), r.Msg)
			n++
		}
	}
	if n == 0 {
		p("- none\n")
	}

	if final {
		p("\n## Final comparison\n\n%s\n", finalText(c, a, b, sa, sb, all, st))
		if c.FinalFile != "" {
			_ = os.WriteFile(c.FinalFile, []byte(finalText(c, a, b, sa, sb, all, st)+"\n"), 0o644)
		}
	}

	tmp := c.Report + ".tmp"
	if err := os.WriteFile(tmp, []byte(w.String()), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, c.Report)
}

func statsTable(w *strings.Builder, a, b string, sa, sb *instStats, ha string) {
	p := func(f string, args ...any) { fmt.Fprintf(w, f, args...) }
	la, ja, ra, wa, na := sa.callAgg()
	lb, jb, rb, wb, nb := sb.callAgg()
	p("| | %s | %s |\n|---|---|---|\n", a, b)
	p("| HTTP success | %.2f%% (%d/%d) | %.2f%% (%d/%d) |\n", sa.okPct(), sa.httpOK, sa.httpN, sb.okPct(), sb.httpOK, sb.httpN)
	p("| TTFB p50 / p95 | %.0f / %.0f ms | %.0f / %.0f ms |\n", pct(sa.ttfb, 0.5), pct(sa.ttfb, 0.95), pct(sb.ttfb, 0.5), pct(sb.ttfb, 0.95))
	p("| Full fetch p50 / p95 | %.0f / %.0f ms | %.0f / %.0f ms |\n", pct(sa.total, 0.5), pct(sa.total, 0.95), pct(sb.total, 0.5), pct(sb.total, 0.95))
	p("| STUN sessions answered | %s | %s |\n", ratio(sa.stunOK, sa.stunN), ratio(sb.stunOK, sb.stunN))
	p("| STUN loss mean / jitter p50 / RTT p50 | %.1f%% / %.1f ms / %.0f ms | %.1f%% / %.1f ms / %.0f ms |\n",
		mean(sa.stunLoss)*100, pct(sa.stunJit, 0.5), pct(sa.stunRTT, 0.5), mean(sb.stunLoss)*100, pct(sb.stunJit, 0.5), pct(sb.stunRTT, 0.5))
	p("| Calls: loss mean / worst window | %.2f%% / %.1f%% (%d calls) | %.2f%% / %.1f%% (%d calls) |\n", la*100, wa*100, na, lb*100, wb*100, nb)
	p("| Calls: jitter / RTT mean | %.1f / %.0f ms | %.1f / %.0f ms |\n", ja, ra, jb, rb)
	p("| Node switches (per-site changes; group switches) | %d; %d | %d; %d |\n", sa.switches, len(sa.groupNow), sb.switches, len(sb.groupNow))
	p("| Restarts / inactive minutes | %d / %d | %d / %d |\n", sa.restarts, sa.inactive, sb.restarts, sb.inactive)
	p("| CPU peak (1-min, %% of one core) | %.1f%% | %.1f%% |\n", sa.cpuPeak, sb.cpuPeak)
	p("| RAM peak | %.0f MB | %.0f MB |\n", sa.memPeak, sb.memPeak)
	p("\n%s\n", ha)
}

func haLine(all []Rec, base float64) string {
	med, max, n := haAgg(all)
	return fmt.Sprintf("Home Assistant: baseline %.1f ms; %d checks during the soak, median %.1f ms, worst %.1f ms (the soak stops if a check and its recheck both exceed 2x baseline and the floor).", base, n, med, max)
}

func ratio(a, b int) string {
	if b == 0 {
		return "-"
	}
	return fmt.Sprintf("%.1f%% (%d/%d)", float64(a)/float64(b)*100, a, b)
}

func siteOK(s *siteStats) string {
	if s == nil {
		return "-"
	}
	return ratio(s.ok, s.n)
}

func siteLat(s *siteStats) string {
	if s == nil || len(s.ttfb) == 0 {
		return "-"
	}
	return fmt.Sprintf("%.0f / %.0f ms", pct(s.ttfb, 0.5), pct(s.ttfb, 0.95))
}

func topNodes(s *siteStats) string {
	if s == nil || len(s.nodes) == 0 {
		return "-"
	}
	type kv struct {
		k string
		v int
	}
	var l []kv
	for k, v := range s.nodes {
		l = append(l, kv{k, v})
	}
	sort.Slice(l, func(i, j int) bool { return l[i].v > l[j].v || (l[i].v == l[j].v && l[i].k < l[j].k) })
	var parts []string
	for i := 0; i < len(l) && i < 3; i++ {
		parts = append(parts, fmt.Sprintf("%s (%d)", l[i].k, l[i].v))
	}
	return strings.Join(parts, ", ")
}

func errList(m map[string]int) string {
	if len(m) == 0 {
		return "none"
	}
	var parts []string
	for k, v := range m {
		parts = append(parts, fmt.Sprintf("%s ×%d", k, v))
	}
	sort.Strings(parts)
	return strings.Join(parts, ", ")
}

func finalText(c *Config, a, b string, sa, sb *instStats, all []Rec, st *State) string {
	var w strings.Builder
	la, ja, _, wa, na := sa.callAgg()
	lb, jb, _, wb, nb := sb.callAgg()
	fmt.Fprintf(&w, "Final comparison after %d days (%s vs %s):\n", c.Days, a, b)
	fmt.Fprintf(&w, "- HTTP success %.2f%% vs %.2f%%; TTFB p50 %.0f vs %.0f ms, p95 %.0f vs %.0f ms.\n",
		sa.okPct(), sb.okPct(), pct(sa.ttfb, 0.5), pct(sb.ttfb, 0.5), pct(sa.ttfb, 0.95), pct(sb.ttfb, 0.95))
	fmt.Fprintf(&w, "- Calls (%d vs %d): loss %.2f%% vs %.2f%%, worst %ds window %.1f%% vs %.1f%%, jitter %.1f vs %.1f ms.\n",
		na, nb, la*100, lb*100, c.CallWindowSec, wa*100, wb*100, ja, jb)
	fmt.Fprintf(&w, "- STUN sessions answered %s vs %s; mean loss %.1f%% vs %.1f%%.\n",
		ratio(sa.stunOK, sa.stunN), ratio(sb.stunOK, sb.stunN), mean(sa.stunLoss)*100, mean(sb.stunLoss)*100)
	fmt.Fprintf(&w, "- Node switches %d vs %d (+%d url-test group switches); restarts %d vs %d; CPU peak %.1f%% vs %.1f%%; RAM peak %.0f vs %.0f MB.\n",
		sa.switches, sb.switches, len(sb.groupNow), sa.restarts, sb.restarts, sa.cpuPeak, sb.cpuPeak, sa.memPeak, sb.memPeak)
	fmt.Fprintf(&w, "- %s", haLine(all, st.HABaseline))
	return w.String()
}
