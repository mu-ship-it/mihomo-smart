package smart

import (
	"math"
	"testing"
	"time"
)

// knownBug: see adapter/outboundgroup/smart_test.go. Skips while the confirmed defect
// reproduces, fails once it no longer does so the marker gets removed with the fix.
func knownBug(t *testing.T, id string, reproduced bool, evidence string) {
	t.Helper()
	if reproduced {
		t.Skipf("KNOWN BUG %s reproduced (expected until fixed): %s", id, evidence)
	}
	t.Fatalf("KNOWN BUG %s no longer reproduces: remove the knownBug marker and assert the fixed behaviour", id)
}

// baseline is a healthy, recently used short web connection.
func baseline() ModelInput {
	return ModelInput{
		Success:            20,
		Failure:            0,
		ConnectTime:        80,
		Latency:            120,
		UploadTotal:        0.05,
		DownloadTotal:      0.8,
		MaxuploadRate:      40,
		MaxdownloadRate:    600,
		ConnectionDuration: 0.5,
		LastUsed:           time.Now().Unix(),
		IsTCP:              true,
	}
}

func weightOf(t *testing.T, in ModelInput, priority float64) float64 {
	t.Helper()
	w, _ := CalculateWeight(&in, priority)
	if math.IsNaN(w) || math.IsInf(w, 0) {
		t.Fatalf("weight is %v for input %+v", w, in)
	}
	return w
}

func TestCalculateWeightNeedsMinimumSamples(t *testing.T) {
	for _, tt := range []struct {
		success, failure int64
		want             bool
	}{
		{0, 0, false}, {1, 0, false}, {0, 1, false}, {1, 1, true}, {2, 0, true},
	} {
		in := baseline()
		in.Success, in.Failure = tt.success, tt.failure
		w, _ := CalculateWeight(&in, 1)
		if (w > 0) != tt.want {
			t.Fatalf("success=%d failure=%d: weight %v, want scored=%v", tt.success, tt.failure, w, tt.want)
		}
	}
}

// Each row changes one input of the baseline in a direction that is unambiguously worse;
// the weight must not go up.
func TestCalculateWeightMonotonic(t *testing.T) {
	tests := []struct {
		name   string
		better func(*ModelInput)
		worse  func(*ModelInput)
	}{
		{"more failures", func(m *ModelInput) { m.Failure = 0 }, func(m *ModelInput) { m.Failure = 10 }},
		{"slower connect", func(m *ModelInput) { m.ConnectTime = 50 }, func(m *ModelInput) { m.ConnectTime = 3000 }},
		{"higher latency", func(m *ModelInput) { m.Latency = 50 }, func(m *ModelInput) { m.Latency = 3000 }},
		{"packet loss", func(m *ModelInput) {}, func(m *ModelInput) { m.LossRate, m.CumulLossRate, m.EmaLossRate = 0.2, 0.2, 0.2 }},
		{"stale history", func(m *ModelInput) {}, func(m *ModelInput) { m.LastUsed = time.Now().Add(-40 * 24 * time.Hour).Unix() }},
		{"udp more failures", func(m *ModelInput) { m.IsUDP, m.IsTCP = true, false }, func(m *ModelInput) { m.IsUDP, m.IsTCP, m.Failure = true, false, 10 }},
		{"udp higher latency", func(m *ModelInput) { m.IsUDP, m.IsTCP, m.Latency = true, false, 40 }, func(m *ModelInput) { m.IsUDP, m.IsTCP, m.Latency = true, false, 2500 }},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			b, w := baseline(), baseline()
			tt.better(&b)
			tt.worse(&w)
			wb, ww := weightOf(t, b, 1), weightOf(t, w, 1)
			if ww > wb {
				t.Fatalf("worse input scored higher: better=%.4f worse=%.4f", wb, ww)
			}
			if ww == wb {
				t.Logf("note: no difference (%.4f) — insensitive input", wb)
			}
		})
	}
}

func TestCalculateWeightPriorityFactorScalesLinearly(t *testing.T) {
	in := baseline()
	w1 := weightOf(t, in, 1)
	for _, f := range []float64{0.5, 1.5, 2} {
		if got := weightOf(t, in, f); math.Abs(got-w1*f) > 1e-9 {
			t.Fatalf("priority %.1f: got %.6f, want %.6f", f, got, w1*f)
		}
	}
}

// Degenerate inputs that a real connection can produce (zero duration, zero traffic,
// upload-only, download-only) must never yield NaN, Inf or a negative weight.
func TestCalculateWeightDegenerateInputsStayFinite(t *testing.T) {
	cases := []func(*ModelInput){
		func(m *ModelInput) { m.ConnectionDuration = 0 },
		func(m *ModelInput) { m.UploadTotal, m.DownloadTotal = 0, 0 },
		func(m *ModelInput) { m.UploadTotal, m.DownloadTotal, m.ConnectionDuration = 0, 0, 0 },
		func(m *ModelInput) { m.UploadTotal = 0; m.ConnectionDuration = 5 },
		func(m *ModelInput) { m.DownloadTotal = 0; m.ConnectionDuration = 5 },
		func(m *ModelInput) { m.MaxuploadRate, m.MaxdownloadRate = 0, 0 },
		func(m *ModelInput) { m.IsUDP, m.IsTCP, m.ConnectionDuration = true, false, 0 },
		func(m *ModelInput) {
			m.Success, m.Failure = 0, 50
			m.ConnectionFailed = true
			m.ConnectTime, m.Latency = 0, 0
		},
		func(m *ModelInput) { m.LossRate, m.CumulLossRate, m.EmaLossRate = 1, 1, 1 },
		func(m *ModelInput) { m.LastUsed = 1 },
		func(m *ModelInput) {
			m.UploadTotal, m.DownloadTotal, m.MaxuploadRate, m.MaxdownloadRate = 1e6, 1e6, 1e7, 1e7
		},
	}
	for i, mut := range cases {
		in := baseline()
		mut(&in)
		if w := weightOf(t, in, 1); w < 0 {
			t.Fatalf("case %d: negative weight %v", i, w)
		}
	}
}

func TestGetTimeDecayWithCache(t *testing.T) {
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC).Unix()
	h := int64(3600)
	tests := []struct {
		ago      int64
		minDecay float64
		want     float64
	}{
		{0, 0.1, 1.0},
		{24 * h, 0.1, 1.0},
		{48 * h, 0.1, 0.9},
		{72 * h, 0.1, 0.8},
		{120 * h, 0.1, 0.65},
		{168 * h, 0.1, 0.5},
		{720 * h, 0.1, 0.3},
		{2000 * h, 0.1, 0.1},
		{2000 * h, 0.3, 0.3}, // floor
	}
	for _, tt := range tests {
		got := GetTimeDecayWithCache(now-tt.ago, now, tt.minDecay)
		if math.Abs(got-tt.want) > 1e-9 {
			t.Fatalf("ago=%dh min=%.1f: got %.4f want %.4f", tt.ago/h, tt.minDecay, got, tt.want)
		}
	}
	// monotonic non-increasing with age
	prev := 2.0
	for ago := int64(0); ago <= 1000*h; ago += 6 * h {
		d := GetTimeDecayWithCache(now-ago, now, 0.1)
		if d > prev+1e-12 {
			t.Fatalf("decay increased with age at %dh", ago/h)
		}
		prev = d
	}
}

func TestIdentifyConnectionScene(t *testing.T) {
	tests := []struct {
		name                                  string
		udp                                   bool
		latency                               int64
		upMB, downMB, maxUpKB, maxDownKB, min float64
		want                                  sceneKind
	}{
		{"zoom-like udp call", true, 60, 30, 60, 400, 600, 20, sceneInteractive},
		{"video stream", false, 80, 2, 80, 50, 6000, 10, sceneStreaming},
		// a >100 MB stream at >5 MB/min is classified as transfer: the transfer check runs first
		{"heavy stream counts as transfer", false, 80, 2, 300, 50, 6000, 10, sceneTransfer},
		{"big download", false, 80, 1, 2000, 100, 20000, 2, sceneTransfer},
		{"short web", false, 80, 0.01, 0.2, 10, 300, 0.1, sceneWeb},
		{"zero duration", false, 80, 0.01, 0.2, 10, 300, 0, sceneWeb},
		{"no traffic", true, 0, 0, 0, 0, 0, 0, sceneWeb},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := identifyConnectionScene(tt.udp, tt.latency, tt.upMB, tt.downMB, tt.maxUpKB, tt.maxDownKB, tt.min)
			if got != tt.want {
				t.Fatalf("scene = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestCalculateTrafficFactorBounded(t *testing.T) {
	for _, mb := range []float64{0, 0.0005, 0.003, 0.02, 0.3, 2, 50, 400, 5000, 1e6} {
		for _, rate := range []float64{0, 10, 300, 3000, 50000, 1e6} {
			for _, hist := range []float64{0, 1, 1000, 1e6} {
				f := calculateTrafficFactor(mb, rate, 2, hist, mb*3, 5, false)
				if math.IsNaN(f) || f < 0 || f > 1.25 {
					t.Fatalf("mb=%v rate=%v hist=%v: factor %v out of [0,1.25]", mb, rate, hist, f)
				}
			}
		}
	}
}

// Known bug W1: a connection that never received a byte has no first-read latency (0) and
// is scored as if it had 1 ms, i.e. better than any measured node. (A connect time of 0 is
// different: it is a real sub-millisecond dial on multiplexed transports and stays fast.)
func TestCalculateWeightMissingLatencyIsNotBest(t *testing.T) {
	missing := baseline()
	missing.Latency = 0
	wx := weightOf(t, missing, 1)
	// 20 ms also earns the low-latency bonus; 200 ms does not, so it checks the score itself
	for _, ms := range []int64{20, 200} {
		measured := baseline()
		measured.Latency = ms
		if wm := weightOf(t, measured, 1); wx >= wm {
			t.Fatalf("regression W1 (missing latency = 1 ms): node with no measured latency weighs %.4f >= a measured %d ms node (%.4f)", wx, ms, wm)
		}
	}
	// but an unanswered connection is not scored like a failed one either
	failed := baseline()
	failed.Latency, failed.ConnectionFailed = 0, true
	if wf := weightOf(t, failed, 1); wf >= wx {
		t.Fatalf("unknown latency (%.4f) should score above a failed connection (%.4f)", wx, wf)
	}
}

// Known bug (assessment §4): latency score is clamped at 0.8, so every latency below ~335 ms
// scores the same and a 30 ms node is indistinguishable from a 300 ms one (apart from a flat
// +0.1 bonus under 100 ms, which cannot separate 120 ms from 330 ms).
func TestCalculateWeightDistinguishesLatencyBelow335ms(t *testing.T) {
	// interactive UDP range: each step must cost a visible amount of weight (>= 1%)
	steps := []int64{30, 120, 330}
	prev := -1.0
	for _, ms := range steps {
		in := baseline()
		in.IsUDP, in.IsTCP, in.Latency = true, false, ms
		w := weightOf(t, in, 1)
		if prev > 0 && (prev-w)/prev < 0.01 {
			t.Fatalf("regression W2 (latency clamp): UDP %d ms weighs %.4f, not meaningfully below the previous step (%.4f)", ms, w, prev)
		}
		prev = w
	}
}
