package main

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/smart"
)

func TestSTUNRequestPadding(t *testing.T) {
	var txid [12]byte
	txid[0] = 7
	for _, size := range []int{0, 20, 250, 2000} {
		b := stunRequest(txid, size)
		got, ok := smart.ParseSTUNBindingRequest(b)
		if !ok || got != txid {
			t.Fatalf("size %d: not a binding request", size)
		}
		if l := int(binary.BigEndian.Uint16(b[2:4])); 20+l != len(b) || l%4 != 0 {
			t.Fatalf("size %d: header length %d, packet %d", size, l, len(b))
		}
		if size == 250 && (len(b) < 240 || len(b) > 250) {
			t.Fatalf("size 250 gave %d bytes", len(b))
		}
		if len(b) > 20+4+760 {
			t.Fatalf("size %d: padding not capped (%d)", size, len(b))
		}
	}
}

func TestHADegraded(t *testing.T) {
	cases := []struct {
		m, base, floor float64
		want           bool
	}{
		{2.0, 1.2, 25, false}, // 1.7x
		{3.0, 1.2, 25, false}, // 2.5x but below the floor
		{30, 1.2, 25, true},   // well above both
		{40, 30, 25, false},   // above floor, under 2x
		{61, 30, 25, true},    // over 2x and the floor
	}
	for _, c := range cases {
		if got := haDegraded(c.m, c.base, c.floor); got != c.want {
			t.Errorf("haDegraded(%v, %v, %v) = %v", c.m, c.base, c.floor, got)
		}
	}
}

func TestSummarizeAndDailyLine(t *testing.T) {
	dir := t.TempDir()
	loc, _ := time.LoadLocation("Asia/Shanghai")
	c := &Config{Instances: []Instance{{Name: "smart"}, {Name: "stock"}}, DataDir: dir, Report: filepath.Join(dir, "report.md"),
		Sites: []string{"github.com"}, TZ: "Asia/Shanghai", Days: 7, CallWindowSec: 10}
	rec := &recorder{dir: dir, loc: loc}
	base := time.Date(2026, 11, 10, 10, 0, 0, 0, loc).UnixMilli()
	add := func(off int64, r Rec) { r.TS = base + off; rec.add(r) }
	// smart: 3 ok, 1 failure, one node switch; stock: 4 ok on one node
	add(0, Rec{Kind: "http", Inst: "smart", Target: "github.com", Node: "A", OK: true, TTFBMs: 100})
	add(1, Rec{Kind: "http", Inst: "smart", Target: "github.com", Node: "A", OK: true, TTFBMs: 200})
	add(2, Rec{Kind: "http", Inst: "smart", Target: "github.com", Node: "B", OK: true, TTFBMs: 300})
	add(3, Rec{Kind: "http", Inst: "smart", Target: "github.com", Err: "i/o timeout"})
	for i := int64(0); i < 4; i++ {
		add(10+i, Rec{Kind: "http", Inst: "stock", Target: "github.com", Node: "A", OK: true, TTFBMs: 400})
	}
	add(20, Rec{Kind: "call", Inst: "smart", Sent: 100, Recv: 99, Loss: 0.01, JitterMs: 3, RTTMs: 80, OK: true})
	add(21, Rec{Kind: "call", Inst: "stock", Sent: 100, Recv: 90, Loss: 0.10, JitterMs: 9, RTTMs: 90, OK: true})
	add(30, Rec{Kind: "sys", Inst: "smart", CPUPct: 2.5, MemMB: 40, Restarts: 0, Active: "active"})
	add(90, Rec{Kind: "sys", Inst: "smart", CPUPct: 1.5, MemMB: 55, Restarts: 1, Active: "active"})
	add(40, Rec{Kind: "budget", ExtMBDay: 12.5})
	add(50, Rec{Kind: "ha", HAms: 1.3, OK: true})

	days, err := loadDays(c)
	if err != nil || len(days) != 1 {
		t.Fatalf("loadDays: %v %d", err, len(days))
	}
	s := summarize(days[0].recs, "smart")
	if s.httpN != 4 || s.httpOK != 3 || s.switches != 1 || s.restarts != 1 || s.memPeak != 55 || s.cpuPeak != 2.5 {
		t.Fatalf("smart summary: %+v", s)
	}
	if s.errs["timeout"] != 1 {
		t.Fatalf("errors: %v", s.errs)
	}
	if p := pct(s.ttfb, 0.5); p != 200 {
		t.Fatalf("p50 = %v", p)
	}
	line := dailyLine(c, loc, "2026-11-10")
	for _, want := range []string{"day 1", "smart 75.0%", "stock 100.0%", "loss 1.0%/10.0%", "node switches 1/0", "restarts 1/0", "traffic 12 MB"} {
		if !strings.Contains(line, want) {
			t.Errorf("daily line misses %q: %s", want, line)
		}
	}
	if err := writeReport(c, loc, true); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(c.Report)
	if !strings.Contains(string(b), "## Final comparison") || !strings.Contains(string(b), "| 2026-11-10 |") {
		t.Fatalf("report:\n%s", b)
	}
}

func TestNextCall(t *testing.T) {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	r := &runner{c: &Config{CallTimes: []string{"09:00", "14:00", "21:00"}}, loc: loc}
	cases := map[string]string{
		"2026-11-10 08:59": "2026-11-10 09:00",
		"2026-11-10 09:00": "2026-11-10 14:00",
		"2026-11-10 20:00": "2026-11-10 21:00",
		"2026-11-10 22:00": "2026-11-11 09:00",
	}
	for now, want := range cases {
		n, _ := time.ParseInLocation("2006-01-02 15:04", now, loc)
		if got := r.nextCall(n).In(loc).Format("2006-01-02 15:04"); got != want {
			t.Errorf("nextCall(%s) = %s, want %s", now, got, want)
		}
	}
}
