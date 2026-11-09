package smart

import (
	"math"
	"testing"
	"time"
)

// feed sends one packet every period and returns a reply for each, delayed by delay(i), unless
// lost(i). Arrivals are fed in time order.
func feed(m *UDPMeter, start time.Time, n int, period time.Duration, lost func(int) bool, delay func(int) time.Duration) time.Time {
	type ev struct {
		at   time.Time
		send bool
	}
	var evs []ev
	for i := 0; i < n; i++ {
		at := start.Add(time.Duration(i) * period)
		evs = append(evs, ev{at, true})
		if lost == nil || !lost(i) {
			d := time.Duration(0)
			if delay != nil {
				d = delay(i)
			}
			evs = append(evs, ev{at.Add(d + time.Millisecond), false})
		}
	}
	// stable insertion sort by time
	for i := 1; i < len(evs); i++ {
		for j := i; j > 0 && evs[j].at.Before(evs[j-1].at); j-- {
			evs[j], evs[j-1] = evs[j-1], evs[j]
		}
	}
	for _, e := range evs {
		if e.send {
			m.OnSend(160, e.at)
		} else {
			m.OnReceive(160, e.at)
		}
	}
	return start.Add(time.Duration(n) * period)
}

func TestUDPMeterCountsAndCuts(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0)
	m := NewUDPMeter(t0)
	end := feed(m, t0, 50, 20*time.Millisecond, func(i int) bool { return i%5 == 4 }, nil)
	w := m.Cut(end)
	if w.OutPkts != 50 || w.InPkts != 40 || w.OutBytes != 50*160 || w.InBytes != 40*160 {
		t.Fatalf("window counts: %+v", w)
	}
	if !w.EverReplied || math.Abs(w.ReplyRatio()-0.8) > 1e-9 {
		t.Fatalf("ratio %.3f replied %v", w.ReplyRatio(), w.EverReplied)
	}
	if w2 := m.Cut(end.Add(time.Second)); w2.OutPkts != 0 || w2.InPkts != 0 || !w2.Start.Equal(end) {
		t.Fatalf("cut must reset the counters: %+v", w2)
	}
}

func TestUDPMeterJitter(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0)
	clean := NewUDPMeter(t0)
	end := feed(clean, t0, 100, 20*time.Millisecond, nil, nil)
	if w := clean.Cut(end); w.JitterMs > 0.01 || w.JitterSamples < 90 {
		t.Fatalf("evenly spaced replies: jitter %.3f ms over %d samples", w.JitterMs, w.JitterSamples)
	}
	jittery := NewUDPMeter(t0)
	end = feed(jittery, t0, 100, 20*time.Millisecond, nil, func(i int) time.Duration {
		return time.Duration(i%2) * 15 * time.Millisecond // 0 / 15 ms alternating
	})
	if w := jittery.Cut(end); w.JitterMs < 20 {
		t.Fatalf("alternating 0/15 ms delays: jitter %.2f ms, want ~30", w.JitterMs)
	}
	// a long silence (muted call) is a gap, not jitter
	quiet := NewUDPMeter(t0)
	next := feed(quiet, t0, 20, 20*time.Millisecond, nil, nil)
	end = feed(quiet, next.Add(3*time.Second), 20, 20*time.Millisecond, nil, nil)
	if w := quiet.Cut(end); w.JitterMs > 0.01 {
		t.Fatalf("a silence counted as jitter: %.2f ms", w.JitterMs)
	}
}

func TestUDPMeterOpenGap(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0)
	m := NewUDPMeter(t0)
	feed(m, t0, 5, 20*time.Millisecond, nil, nil)
	for i := 0; i < 30; i++ { // keeps sending, nothing comes back
		m.OnSend(160, t0.Add(100*time.Millisecond+time.Duration(i)*20*time.Millisecond))
	}
	if w := m.Cut(t0.Add(800 * time.Millisecond)); w.MaxGapMs < 700 {
		t.Fatalf("open gap %d ms, want >= 700", w.MaxGapMs)
	}
}

func TestUDPReplyReferenceJudge(t *testing.T) {
	ref := NewUDPReplyReference(0)
	end := time.Unix(1_700_000_000, 0)
	win := func(out, in int64, replied bool) UDPWindow {
		return UDPWindow{End: end, OutPkts: out, InPkts: in, EverReplied: replied}
	}
	if loss, ok := ref.Judge("zoom", win(100, 100, true)); !ok || loss != 0 {
		t.Fatalf("clean window: loss %.3f judged %v", loss, ok)
	}
	if loss, ok := ref.Judge("zoom", win(100, 75, true)); !ok || math.Abs(loss-0.25) > 0.01 {
		t.Fatalf("75%% replies against a 100%% reference: loss %.3f judged %v", loss, ok)
	}
	if loss, ok := ref.Judge("zoom", win(100, 99, true)); !ok || loss != 0 {
		t.Fatalf("1%% is noise: loss %.3f", loss)
	}
	if _, ok := ref.Judge("zoom", win(UDPMinWindowPackets-1, 0, true)); ok {
		t.Fatal("a quiet window must not be judged")
	}
	if loss, ok := ref.Judge("zoom", win(50, 0, true)); !ok || loss != 1 {
		t.Fatalf("replies stopped: loss %.3f judged %v", loss, ok)
	}
	if _, ok := ref.Judge("one-way", win(50, 0, false)); ok {
		t.Fatal("a session that never got a reply is left to the close path")
	}
	// another app talks 2:1; its own reference applies
	if loss, ok := ref.Judge("dns-like", win(100, 50, true)); !ok || loss != 0 {
		t.Fatalf("first window of a target sets its own reference: loss %.3f", loss)
	}
}

func TestUDPReplyReferenceIsBounded(t *testing.T) {
	ref := NewUDPReplyReference(3)
	t0 := time.Unix(1_700_000_000, 0)
	for i := 0; i < 10; i++ {
		ref.Touch(string(rune('a'+i)), t0.Add(time.Duration(i)*time.Second))
	}
	if ref.Len() != 3 {
		t.Fatalf("len %d, want 3", ref.Len())
	}
	if got := ref.Targets(t0); len(got) != 3 || got[0] != "j" {
		t.Fatalf("most recent first, oldest evicted: %v", got)
	}
}

func TestUDPLiveFactor(t *testing.T) {
	if f := UDPLiveFactor(0, 0); f != 1 {
		t.Fatalf("clean path factor %v", f)
	}
	prev := 1.0
	for _, loss := range []float64{0.02, 0.05, 0.1, 0.2, 1} {
		f := UDPLiveFactor(loss, 0)
		if f >= prev {
			t.Fatalf("factor must fall with loss: %.3f at %.2f", f, loss)
		}
		prev = f
	}
	if f := UDPLiveFactor(0.2, 0); f > 0.5 {
		t.Fatalf("20%% loss should at least halve the weight, factor %.3f", f)
	}
	if UDPLiveFactor(0, 60) >= UDPLiveFactor(0, 10) {
		t.Fatal("factor must fall with jitter")
	}
	if f := UDPLiveFactor(1, 1e6); f < 0.27 || f > 0.29 {
		t.Fatalf("floor %.3f, want 0.28", f)
	}
}

// BenchmarkUDPMeterPacket measures the per-packet cost the live sampler adds to a UDP session.
func BenchmarkUDPMeterPacket(b *testing.B) {
	m := NewUDPMeter(time.Now())
	now := time.Now()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		now = now.Add(20 * time.Millisecond)
		if i%2 == 0 {
			m.OnSend(160, now)
		} else {
			m.OnReceive(160, now)
		}
	}
}
