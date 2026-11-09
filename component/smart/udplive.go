package smart

import (
	"math"
	"sort"
	"sync"
	"time"
)

// Live UDP quality (Phase 4).
//
// A UDP session used to be scored once, when it closed, from one first-reply latency. A call
// that degraded half way through was therefore invisible to the selector until the call
// ended. A UDPMeter sits on the session's packet path and counts packets and bytes each way;
// the group cuts it into windows on a short interval and turns each window into a loss and a
// jitter estimate that feed the node's UDP weight while the session is still open.
//
// UDP carries no sequence numbers the proxy can read, so loss is estimated from the reply
// ratio (packets in / packets out) of a window, measured against the best ratio recently seen
// for the same target on any node of the group (UDPReplyReference). A node that answers the
// same application less often than another node does is losing packets. Jitter is the RFC
// 3550 style smoothed variation of reply inter-arrival times.

const (
	// DefaultUDPSampleInterval is how often a live UDP session is cut into a window.
	DefaultUDPSampleInterval = 2 * time.Second
	// UDPMinWindowPackets is the least number of packets sent in a window for its reply ratio
	// to be judged; quieter windows only feed jitter.
	UDPMinWindowPackets = 8
	// udpGapLimit splits jitter from silence: an inter-arrival longer than this is a gap (a
	// muted microphone, a paused stream) and is not jitter.
	udpGapLimit = 500 * time.Millisecond
	// udpLossDeadZone ignores reply ratio noise below 2%.
	udpLossDeadZone = 0.02
	// udpRefDecay lets the per-target reference ratio forget a burst of extra replies slowly
	// (per judged window).
	udpRefDecay = 0.995
	// MaxUDPReferenceTargets bounds the per-group reference table.
	MaxUDPReferenceTargets = 2048

	// UDP live quality score: weight *= (1 - lossWeight*lossPenalty) * (1 - jitterWeight*jitterPenalty)
	udpLossScale       = 12.0 // 2% -> 0.21, 5% -> 0.45, 10% -> 0.70, 20% -> 0.91
	udpJitterScaleMs   = 50.0 // 10 ms -> 0.18, 30 ms -> 0.45, 60 ms -> 0.70
	udpLossWeight      = 0.6
	udpJitterWeight    = 0.3
	udpQualityEMAAlpha = 0.5
)

// UDPWindow is one cut of a live UDP session.
type UDPWindow struct {
	Start    time.Time
	End      time.Time
	OutPkts  int64
	InPkts   int64
	OutBytes int64
	InBytes  int64
	// MaxGapMs is the longest time without a reply inside the window, including the tail up
	// to the cut while packets were still being sent.
	MaxGapMs int64
	// JitterMs is the smoothed inter-arrival jitter at the cut (RFC 3550 §6.4.1 estimator
	// applied to arrival spacing).
	JitterMs float64
	// JitterSamples is how many inter-arrivals fed the jitter estimate in this window.
	JitterSamples int64
	// EverReplied reports whether the session had received any packet before or during the
	// window.
	EverReplied bool
}

// ReplyRatio is packets in per packet out, 0 when nothing was sent.
func (w UDPWindow) ReplyRatio() float64 {
	if w.OutPkts == 0 {
		return 0
	}
	return float64(w.InPkts) / float64(w.OutPkts)
}

// UDPMeter counts one UDP session. It is safe for concurrent use; the per-packet cost is one
// uncontended lock and a clock read.
type UDPMeter struct {
	mu sync.Mutex

	start    time.Time
	outPkts  int64
	inPkts   int64
	outBytes int64
	inBytes  int64
	maxGap   time.Duration

	lastSend     time.Time
	lastArrival  time.Time
	lastIA       time.Duration
	jitter       float64 // ms
	jitterN      int64
	everReplied  bool
	sentSinceArr bool
}

func NewUDPMeter(now time.Time) *UDPMeter {
	return &UDPMeter{start: now}
}

// OnSend records one packet sent by the client.
func (m *UDPMeter) OnSend(n int, now time.Time) {
	m.mu.Lock()
	m.outPkts++
	m.outBytes += int64(n)
	m.lastSend = now
	m.sentSinceArr = true
	m.mu.Unlock()
}

// OnReceive records one packet received from the far end.
func (m *UDPMeter) OnReceive(n int, now time.Time) {
	m.mu.Lock()
	m.inPkts++
	m.inBytes += int64(n)
	if !m.lastArrival.IsZero() {
		ia := now.Sub(m.lastArrival)
		if ia > m.maxGap && m.sentSinceArr {
			m.maxGap = ia
		}
		if ia <= udpGapLimit {
			if m.lastIA > 0 {
				d := math.Abs(float64(ia-m.lastIA)) / float64(time.Millisecond)
				m.jitter += (d - m.jitter) / 16
				m.jitterN++
			}
			m.lastIA = ia
		} else {
			// a silence restarts the spacing; it is not jitter
			m.lastIA = 0
		}
	}
	m.lastArrival = now
	m.everReplied = true
	m.sentSinceArr = false
	m.mu.Unlock()
}

// Cut closes the current window at now and starts the next one. The jitter estimator keeps
// its state across windows, as RFC 3550 does.
func (m *UDPMeter) Cut(now time.Time) UDPWindow {
	m.mu.Lock()
	defer m.mu.Unlock()

	gap := m.maxGap
	// a sender that is still talking but hearing nothing back has an open gap
	if m.sentSinceArr && m.outPkts > 0 {
		since := m.start
		if !m.lastArrival.IsZero() && m.lastArrival.After(since) {
			since = m.lastArrival
		}
		if open := now.Sub(since); open > gap {
			gap = open
		}
	}
	w := UDPWindow{
		Start:         m.start,
		End:           now,
		OutPkts:       m.outPkts,
		InPkts:        m.inPkts,
		OutBytes:      m.outBytes,
		InBytes:       m.inBytes,
		MaxGapMs:      gap.Milliseconds(),
		JitterMs:      m.jitter,
		JitterSamples: m.jitterN,
		EverReplied:   m.everReplied,
	}
	m.start = now
	m.outPkts, m.inPkts, m.outBytes, m.inBytes = 0, 0, 0, 0
	m.maxGap = 0
	m.jitterN = 0
	return w
}

// UDPReplyReference keeps, per target, the best reply ratio recently seen on any node, so a
// node can be judged against what the same application gets elsewhere. It is bounded; the
// least recently used target is dropped first.
type UDPReplyReference struct {
	mu      sync.Mutex
	entries map[string]*udpRefEntry
	max     int
}

type udpRefEntry struct {
	ratio    float64
	lastSeen time.Time
}

func NewUDPReplyReference(max int) *UDPReplyReference {
	if max <= 0 {
		max = MaxUDPReferenceTargets
	}
	return &UDPReplyReference{entries: make(map[string]*udpRefEntry), max: max}
}

// Judge estimates the loss of one window and folds the window into the target's reference.
// judged is false when the window carries too little traffic to say anything about loss.
func (r *UDPReplyReference) Judge(target string, w UDPWindow) (loss float64, judged bool) {
	if w.OutPkts < UDPMinWindowPackets {
		return 0, false
	}
	if w.InPkts == 0 {
		// a session that used to get replies and now gets none is an outage; one that never
		// got any is left to the close path (U1), it may be one-way by design
		if w.EverReplied {
			return 1, true
		}
		return 0, false
	}

	ratio := w.ReplyRatio()

	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.entries[target]
	if e == nil {
		if len(r.entries) >= r.max {
			r.evictLocked()
		}
		e = &udpRefEntry{}
		r.entries[target] = e
	}
	e.lastSeen = w.End
	ref := e.ratio * udpRefDecay
	if ratio > ref {
		ref = ratio
	}
	e.ratio = ref

	loss = 1 - ratio/ref
	if loss < udpLossDeadZone {
		loss = 0
	}
	return math.Min(1, loss), true
}

// Targets returns the targets seen since the given time, most recent first.
func (r *UDPReplyReference) Targets(since time.Time) []string {
	r.mu.Lock()
	type kv struct {
		target string
		seen   time.Time
	}
	list := make([]kv, 0, len(r.entries))
	for t, e := range r.entries {
		if !e.lastSeen.Before(since) {
			list = append(list, kv{t, e.lastSeen})
		}
	}
	r.mu.Unlock()
	sort.Slice(list, func(i, j int) bool {
		if !list[i].seen.Equal(list[j].seen) {
			return list[i].seen.After(list[j].seen)
		}
		return list[i].target < list[j].target
	})
	out := make([]string, len(list))
	for i, e := range list {
		out[i] = e.target
	}
	return out
}

// Len is the number of targets held.
func (r *UDPReplyReference) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

// Touch marks a target as seen without judging a window (a probe hint, a quiet session).
func (r *UDPReplyReference) Touch(target string, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.entries[target]
	if e == nil {
		if len(r.entries) >= r.max {
			r.evictLocked()
		}
		e = &udpRefEntry{}
		r.entries[target] = e
	}
	e.lastSeen = now
}

func (r *UDPReplyReference) evictLocked() {
	var oldest string
	var oldestSeen time.Time
	first := true
	for t, e := range r.entries {
		if first || e.lastSeen.Before(oldestSeen) {
			oldest, oldestSeen, first = t, e.lastSeen, false
		}
	}
	delete(r.entries, oldest)
}

// UDPLiveFactor scales a UDP weight by the measured loss and jitter: 1 for a clean path,
// down to (1-0.6)*(1-0.3) = 0.28 for a path that loses everything with large jitter.
func UDPLiveFactor(loss, jitterMs float64) float64 {
	if loss <= 0 && jitterMs <= 0 {
		return 1
	}
	loss = math.Max(0, math.Min(1, loss))
	jitterMs = math.Max(0, jitterMs)
	lossPenalty := 1 - math.Exp(-loss*udpLossScale)
	jitterPenalty := 1 - math.Exp(-jitterMs/udpJitterScaleMs)
	return (1 - udpLossWeight*lossPenalty) * (1 - udpJitterWeight*jitterPenalty)
}

// UDPQualityEMA folds a new measurement into a running loss or jitter value. Unlike the
// connection EMA it treats a stored 0 as a real (clean) measurement once one was taken.
func UDPQualityEMA(old, sample float64, hadSample bool) float64 {
	if !hadSample {
		return sample
	}
	return old + udpQualityEMAAlpha*(sample-old)
}
