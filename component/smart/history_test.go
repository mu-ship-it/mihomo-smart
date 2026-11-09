package smart

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/metacubex/bbolt"
)

// newTestStore opens a throw-away bbolt file as the package store and restores the previous
// one afterwards.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	bdb, err := bbolt.Open(filepath.Join(t.TempDir(), "cache.db"), 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	prev := db
	st := NewStore(bdb)
	t.Cleanup(func() {
		opQueue.reset()
		db = prev
		_ = bdb.Close()
	})
	return st
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func findBucket(r HistoryReport, how int) *HistoryBucket {
	for i := range r.Buckets {
		if r.Buckets[i].HourOfWeek == how {
			return &r.Buckets[i]
		}
	}
	return nil
}

func TestHistoryAggregatesPerHourOfWeek(t *testing.T) {
	st := newTestStore(t)
	clock := &fakeClock{t: time.Date(2026, 11, 2, 9, 15, 0, 0, time.UTC)} // a Monday
	h := NewHistoryRecorder(st, "g", "cfg", 0, 0)
	h.SetClock(clock.now)

	udp := func(node string, ok bool, lat int64, loss, jit float64) {
		h.Record("zoom.us", HistoryEvent{Node: node, Network: HistoryNetworkUDP, OK: ok, LatencyMs: lat, Loss: loss, JitterMs: jit, Source: HistorySourceProbe},
			HistoryRecordOptions{HasLoss: true, HasJitter: true})
	}
	// week 1, Monday 09:xx
	udp("hk", true, 40, 0.0, 4)
	udp("hk", true, 60, 0.1, 8)
	udp("jp", false, 0, 1, 0)
	h.Record("zoom.us", HistoryEvent{Node: "hk", Network: HistoryNetworkTCP, OK: true, LatencyMs: 100, Source: HistorySourceConnection}, HistoryRecordOptions{})
	h.Flush()

	// week 2, Monday 09:xx and Tuesday 10:xx
	clock.t = clock.t.Add(7*24*time.Hour + 30*time.Minute)
	udp("hk", true, 50, 0.2, 12)
	clock.t = clock.t.Add(25 * time.Hour)
	udp("hk", true, 30, 0, 2)
	h.Flush()
	st.FlushQueue(true) // persisted, then read back through a fresh recorder

	fresh := NewHistoryRecorder(st, "g", "cfg", 0, 0)
	fresh.SetClock(clock.now)
	rep := fresh.Report("zoom.us", HistoryNetworkUDP, "", time.UTC)
	if rep.HoursCovered != 3 || rep.Events != 5 || len(rep.Buckets) != 2 {
		t.Fatalf("report: hours %d events %d buckets %d", rep.HoursCovered, rep.Events, len(rep.Buckets))
	}
	mon := findBucket(rep, 9)
	if mon == nil || mon.Weekday != "Mon" || mon.Hour != 9 || mon.Weeks != 2 || len(mon.Nodes) != 2 {
		t.Fatalf("Monday 09 bucket: %+v", mon)
	}
	hk := mon.Nodes[0]
	if hk.Node != "hk" || hk.Count != 3 || hk.Success != 3 || *hk.AvgLatencyMs != 50 ||
		*hk.AvgLoss < 0.099 || *hk.AvgLoss > 0.101 || *hk.AvgJitterMs != 8 {
		t.Fatalf("hk on Monday 09: %+v lat %v loss %v jit %v", hk, *hk.AvgLatencyMs, *hk.AvgLoss, *hk.AvgJitterMs)
	}
	jp := mon.Nodes[1]
	if jp.Node != "jp" || jp.Failure != 1 || jp.SuccessRate != 0 || jp.AvgLatencyMs != nil {
		t.Fatalf("jp on Monday 09: %+v", jp)
	}
	if tue := findBucket(rep, 24+10); tue == nil || tue.Weekday != "Tue" || tue.Weeks != 1 {
		t.Fatalf("Tuesday 10 bucket: %+v", tue)
	}

	if tcp := fresh.Report("zoom.us", HistoryNetworkTCP, "", time.UTC); tcp.Events != 1 || tcp.Buckets[0].Nodes[0].AvgLoss != nil {
		t.Fatalf("tcp filter: %+v", tcp)
	}
	if one := fresh.Report("zoom.us", "", "jp", time.UTC); one.Events != 1 {
		t.Fatalf("node filter: %d events", one.Events)
	}
	if none := fresh.Report("other.example", "", "", time.UTC); len(none.Buckets) != 0 || none.Buckets == nil {
		t.Fatalf("unknown target must give an empty, non-null bucket list: %+v", none)
	}
}

func TestHistoryUsesTheReportTimeZone(t *testing.T) {
	st := newTestStore(t)
	clock := &fakeClock{t: time.Date(2026, 11, 2, 1, 0, 0, 0, time.UTC)} // Monday 09:00 in UTC+8
	h := NewHistoryRecorder(st, "g", "cfg", 0, 0)
	h.SetClock(clock.now)
	h.Record("zoom.us", HistoryEvent{Node: "hk", Network: HistoryNetworkUDP, OK: true}, HistoryRecordOptions{})
	loc := time.FixedZone("UTC+8", 8*3600)
	rep := h.Report("zoom.us", "", "", loc)
	if len(rep.Buckets) != 1 || rep.Buckets[0].HourOfWeek != 9 || rep.UTCOffsetMinutes != 480 {
		t.Fatalf("%+v", rep)
	}
	if utc := h.Report("zoom.us", "", "", time.UTC); utc.Buckets[0].HourOfWeek != 1 {
		t.Fatalf("utc bucket %d", utc.Buckets[0].HourOfWeek)
	}
}

func TestHistoryChunkCapsEventsButKeepsExactTotals(t *testing.T) {
	st := newTestStore(t)
	clock := &fakeClock{t: time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC)}
	h := NewHistoryRecorder(st, "g", "cfg", 0, 0)
	h.SetClock(clock.now)
	for i := 0; i < 100; i++ {
		clock.t = clock.t.Add(time.Second)
		h.Record("zoom.us", HistoryEvent{Node: "hk", Network: HistoryNetworkUDP, OK: i%10 != 0}, HistoryRecordOptions{})
	}
	chunks := h.TargetChunks("zoom.us")
	if len(chunks) != 1 {
		t.Fatalf("%d chunks", len(chunks))
	}
	for _, c := range chunks {
		agg := c.Agg["hk|udp"]
		if len(c.Events) != HistoryMaxEventsPerChunk || c.Dropped != 100-HistoryMaxEventsPerChunk ||
			agg.Count != 100 || agg.Failure != 10 {
			t.Fatalf("events %d dropped %d agg %+v", len(c.Events), c.Dropped, agg)
		}
		if c.Events[0].TS != time.Date(2026, 11, 2, 9, 0, 1, 0, time.UTC).Unix() || c.Events[0].Node != "hk" {
			t.Fatalf("events keep their timestamp and node: %+v", c.Events[0])
		}
	}
}

func TestHistoryPruneAppliesRetentionAndChunkCap(t *testing.T) {
	st := newTestStore(t)
	clock := &fakeClock{t: time.Date(2026, 11, 2, 0, 30, 0, 0, time.UTC)}
	h := NewHistoryRecorder(st, "g", "cfg", 3*time.Hour, 3)
	h.SetClock(clock.now)
	rec := func(target string) {
		h.Record(target, HistoryEvent{Node: "hk", Network: HistoryNetworkUDP, OK: true}, HistoryRecordOptions{})
	}
	rec("a") // 00:30, older than the retention once the clock is at 05:xx
	clock.t = clock.t.Add(4 * time.Hour)
	rec("a") // 04:30
	clock.t = clock.t.Add(30 * time.Minute)
	rec("a") // 05:00
	rec("b")
	rec("c") // 05:00: 4 chunks inside the retention, cap 3
	if n := h.Prune(); n != 2 {
		t.Fatalf("pruned %d, want 2 (one past retention, one past the cap)", n)
	}
	if got := len(h.TargetChunks("a")); got != 1 {
		t.Fatalf("target a keeps %d chunks, want only the newest", got)
	}
	if got := h.HistoryTargets(); len(got) != 3 {
		t.Fatalf("targets %v", got)
	}
}

// a target key may contain '/' (IP-CIDR rules); it must not swallow another target's chunks
func TestHistoryTargetsWithSlashes(t *testing.T) {
	st := newTestStore(t)
	h := NewHistoryRecorder(st, "g", "cfg", 0, 0)
	h.Record("IP-CIDR [10.0.0.0/8]", HistoryEvent{Node: "hk", Network: HistoryNetworkUDP, OK: true}, HistoryRecordOptions{})
	h.Record("IP-CIDR [10.0.0.0/8]/x", HistoryEvent{Node: "hk", Network: HistoryNetworkUDP, OK: true}, HistoryRecordOptions{})
	if n := len(h.TargetChunks("IP-CIDR [10.0.0.0/8]")); n != 1 {
		t.Fatalf("%d chunks", n)
	}
}
