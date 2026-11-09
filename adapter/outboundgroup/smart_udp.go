package outboundgroup

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/component/resolver"
	"github.com/metacubex/mihomo/component/smart"
	"github.com/metacubex/mihomo/component/smart/lightgbm"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

// Phase 4: live UDP quality, active STUN probing and timestamped history for the Smart group.
// See docs/SMART-ASSESSMENT.md §12 for the design, knobs and defaults.

const (
	// maxLiveUDPSessions bounds how many open UDP sessions one group samples; sessions past
	// the cap are still scored at close, as before.
	maxLiveUDPSessions = 1024
	// udpJitterMinSamples is the least number of inter-arrivals for a window's jitter to count.
	udpJitterMinSamples = 4
	// a node is no longer pinned for a target's UDP once its live quality is this bad
	udpDegradeLoss     = 0.10
	udpDegradeJitterMs = 60.0
	// an outcome with this much loss counts as a failed probe or a failed live window
	udpFailLoss = 0.5

	historyFlushInterval = time.Minute
	historyPruneInterval = time.Hour

	defaultProbeDuration = 5 * time.Second
	minProbeDuration     = 200 * time.Millisecond
	maxProbeDuration     = 30 * time.Second
	probeNodeConcurrency = 4
	probeListenTimeout   = 5 * time.Second

	// automatic probing (off unless udp-probe-budget > 0)
	autoProbeInterval    = 10 * time.Minute
	autoProbeMaxNodes    = 3
	autoProbeTargetSince = 24 * time.Hour
)

// autoProbeDuration is how long an automatic probe runs per node (a var for tests).
var autoProbeDuration = 3 * time.Second

var (
	ErrProbeBusy      = errors.New("a probe is already running for this group")
	ErrProbeNoTarget  = errors.New("target is required")
	ErrProbeDuration  = errors.New("duration must be between 200ms and 30s")
	ErrProbeNoServers = errors.New("no usable STUN server")
)

// ---------------------------------------------------------------- live sampling

type udpLiveSession struct {
	meter    *smart.UDPMeter
	metadata *C.Metadata
	target   string
	proxy    C.Proxy
}

// udpMeterPacketConn feeds every packet of a session into its meter. It never reports itself
// replaceable, so the copy loop keeps it on the path.
type udpMeterPacketConn struct {
	C.PacketConn
	meter *smart.UDPMeter
}

func (c *udpMeterPacketConn) WriteTo(b []byte, addr net.Addr) (n int, err error) {
	n, err = c.PacketConn.WriteTo(b, addr)
	if err == nil {
		c.meter.OnSend(n, time.Now())
	}
	return
}

func (c *udpMeterPacketConn) ReadFrom(b []byte) (n int, addr net.Addr, err error) {
	n, addr, err = c.PacketConn.ReadFrom(b)
	if err == nil {
		c.meter.OnReceive(n, time.Now())
	}
	return
}

func (c *udpMeterPacketConn) WaitReadFrom() (data []byte, put func(), addr net.Addr, err error) {
	data, put, addr, err = c.PacketConn.WaitReadFrom()
	if err == nil {
		c.meter.OnReceive(len(data), time.Now())
	}
	return
}

func (c *udpMeterPacketConn) Upstream() any           { return c.PacketConn }
func (c *udpMeterPacketConn) ReaderReplaceable() bool { return false }
func (c *udpMeterPacketConn) WriterReplaceable() bool { return false }

var _ C.PacketConn = (*udpMeterPacketConn)(nil)

// startLiveUDP wraps a new UDP session with a meter and registers it for sampling. It returns
// the conn unchanged when sampling is off or the group is at its session cap.
func (s *Smart) startLiveUDP(pc C.PacketConn, proxy C.Proxy, metadata *C.Metadata) (C.PacketConn, *udpLiveSession) {
	// keyed by the session itself: the tunnel assigns metadata.UUID only after this wrap
	if s.udpSampleInterval <= 0 || metadata.SmartTarget == "" {
		return pc, nil
	}
	s.udpLiveMu.Lock()
	defer s.udpLiveMu.Unlock()
	if s.udpLive == nil {
		s.udpLive = make(map[*udpLiveSession]struct{})
	}
	if len(s.udpLive) >= maxLiveUDPSessions {
		return pc, nil
	}
	sess := &udpLiveSession{
		meter:    smart.NewUDPMeter(time.Now()),
		metadata: metadata,
		target:   metadata.SmartTarget,
		proxy:    proxy,
	}
	s.udpLive[sess] = struct{}{}
	return &udpMeterPacketConn{PacketConn: pc, meter: sess.meter}, sess
}

// stopLiveUDP unregisters a closing session and scores its last partial window.
func (s *Smart) stopLiveUDP(sess *udpLiveSession) {
	if sess == nil {
		return
	}
	s.udpLiveMu.Lock()
	delete(s.udpLive, sess)
	s.udpLiveMu.Unlock()
	s.applyUDPWindow(sess, sess.meter.Cut(time.Now()))
}

// LiveUDPSessions reports how many UDP sessions are being sampled.
func (s *Smart) LiveUDPSessions() int {
	s.udpLiveMu.Lock()
	defer s.udpLiveMu.Unlock()
	return len(s.udpLive)
}

// sampleLiveUDP cuts every live session into a window and scores it.
func (s *Smart) sampleLiveUDP(now time.Time) {
	s.udpLiveMu.Lock()
	sessions := make([]*udpLiveSession, 0, len(s.udpLive))
	for sess := range s.udpLive {
		sessions = append(sessions, sess)
	}
	s.udpLiveMu.Unlock()
	for _, sess := range sessions {
		s.applyUDPWindow(sess, sess.meter.Cut(now))
	}
}

func (s *Smart) applyUDPWindow(sess *udpLiveSession, w smart.UDPWindow) {
	if w.OutPkts == 0 && w.InPkts == 0 {
		return
	}
	s.udpRef.Touch(sess.target, w.End)
	loss, judged := s.udpRef.Judge(sess.target, w)
	jitter := -1.0
	if w.JitterSamples >= udpJitterMinSamples {
		jitter = w.JitterMs
	}
	if !judged && jitter < 0 {
		return
	}
	if !judged {
		loss = -1
	}
	s.recordUDPQuality(sess.target, sess.proxy, sess.metadata, udpQualitySample{
		loss:     loss,
		jitterMs: jitter,
		ok:       !judged || loss < udpFailLoss,
		source:   smart.HistorySourceLiveUDP,
		at:       w.End,
	})
}

// liveUDPLoop drives sampling, the history flush and the history prune for this group.
func (s *Smart) liveUDPLoop() {
	defer s.wg.Done()
	interval := s.udpSampleInterval
	if interval <= 0 {
		interval = smart.DefaultUDPSampleInterval
	}
	sample := time.NewTicker(interval)
	flush := time.NewTicker(historyFlushInterval)
	prune := time.NewTicker(historyPruneInterval)
	defer sample.Stop()
	defer flush.Stop()
	defer prune.Stop()
	for {
		select {
		case now := <-sample.C:
			if s.udpSampleInterval > 0 {
				s.sampleLiveUDP(now)
			}
		case <-flush.C:
			s.history.Flush()
		case <-prune.C:
			if n := s.history.Prune(); n > 0 {
				log.Debugln("[Smart] Group [%s] history pruned %d chunk(s)", s.Name(), n)
			}
		case <-s.ctx.Done():
			return
		}
	}
}

// ---------------------------------------------------------------- recording

type udpQualitySample struct {
	loss         float64 // < 0: not measured
	jitterMs     float64 // < 0: not measured
	rttMs        int64   // 0: not measured
	countOutcome bool    // a probe is a sample of its own; a live window is part of a session
	ok           bool
	source       string
	at           time.Time
}

// recordUDPQuality folds one live window or probe result into the node's UDP record for the
// target, recomputes its UDP weight (TCP weight is untouched) and writes the history event.
// It returns the UDP weight before and after.
func (s *Smart) recordUDPQuality(target string, proxy C.Proxy, metadata *C.Metadata, q udpQualitySample) (before, after float64) {
	if target == "" || proxy == nil {
		return 0, 0
	}
	proxyName := proxy.Name()
	cacheKey := smart.FormatDBKey(smart.KeyTypeStats, s.configName, s.Name(), target, proxyName)

	lock := smart.GetTargetNodeLock(target, s.Name(), proxyName)
	lock.Lock()
	rec := s.store.GetOrCreateAtomicRecord(cacheKey, s.Name(), s.configName, target, proxyName)
	if q.countOutcome {
		if q.ok {
			rec.AddSuccess(1)
		} else {
			rec.AddFailure(1)
		}
	}
	if q.rttMs > 0 {
		rec.SetLatency(updateEMA(rec.Latency(), q.rttMs))
	}
	rec.AddUDPQuality(q.loss, q.jitterMs)

	meta := &C.Metadata{NetWork: C.UDP}
	if metadata != nil {
		meta = metadata.Clone()
		meta.NetWork = C.UDP
	}
	input := lightgbm.CreateModelInputFromStatsRecord(rec, meta, 0, 0, 0, 0, rec.Duration(), meta.WildcardTarget, 0, 0)
	input.ConnectionFailed = q.countOutcome && !q.ok
	// an open session is itself evidence the path works, so a target first seen in this
	// session is scored instead of waiting for its first close
	if total := input.Success + input.Failure; total < smart.DefaultMinSampleCount {
		input.Success += smart.DefaultMinSampleCount - total
	}
	priority := s.getPriorityFactor(proxyName)
	weight, _ := smart.CalculateWeight(input, priority)
	loss, jitter, _ := rec.UDPQuality()
	weight *= smart.UDPLiveFactor(loss, jitter)

	before = rec.GetWeight(smart.WeightTypeUDP)
	after = updateEMA(before, weight)
	rec.SetWeight(smart.WeightTypeUDP, after)
	rec.SetLastUsed(time.Now().Unix())
	snapshot := rec.CreateStatsSnapshot(cacheKey)
	lock.Unlock()

	s.saveStatsRecord(target, proxy, snapshot)

	if loss >= udpDegradeLoss || jitter >= udpDegradeJitterMs {
		s.unpinDegradedUDP(target, proxyName)
	}

	at := q.at
	if at.IsZero() {
		at = time.Now()
	}
	ev := smart.HistoryEvent{
		TS:        at.Unix(),
		Node:      proxyName,
		Network:   smart.HistoryNetworkUDP,
		OK:        q.ok,
		LatencyMs: q.rttMs,
		Source:    q.source,
	}
	opt := smart.HistoryRecordOptions{HasLoss: q.loss >= 0, HasJitter: q.jitterMs >= 0}
	if opt.HasLoss {
		ev.Loss = q.loss
	}
	if opt.HasJitter {
		ev.JitterMs = q.jitterMs
	}
	s.history.Record(target, ev, opt)
	return before, after
}

// unpinDegradedUDP drops the target's UDP pin (and its prefetched ranking) when it points at
// the degraded node, so the next UDP flow is placed on the current ranking.
func (s *Smart) unpinDegradedUDP(target, proxyName string) {
	key := smart.NetScopedTarget(target, true)
	if names, _ := s.store.GetUnwrapResult(s.Name(), s.configName, key); len(names) > 0 && names[0] == proxyName {
		s.store.DeleteUnwrapResult(s.Name(), s.configName, key)
		s.store.AppendToGlobalQueue(smart.StoreOperation{
			Type: smart.OpDeleteData, KeyType: smart.KeyTypePrefetch,
			Group: s.Name(), Config: s.configName, Target: target,
		})
		log.Debugln("[Smart] Group [%s] - Node [%s] - Target [%s] UDP quality degraded, pin dropped", s.Name(), proxyName, target)
	}
}

// recordHistory writes one closed connection to the history.
func (s *Smart) recordHistory(metadata *C.Metadata, proxyName string, latency int64, err error) {
	if s.history == nil || metadata.SmartTarget == "" {
		return
	}
	network := smart.HistoryNetworkTCP
	if metadata.NetWork == C.UDP {
		network = smart.HistoryNetworkUDP
	}
	s.history.Record(metadata.SmartTarget, smart.HistoryEvent{
		Node:      proxyName,
		Network:   network,
		OK:        err == nil,
		LatencyMs: latency,
		Source:    smart.HistorySourceConnection,
	}, smart.HistoryRecordOptions{})
}

// History returns the group's history recorder.
func (s *Smart) History() *smart.HistoryRecorder {
	return s.history
}

// ---------------------------------------------------------------- active probe

// UDPProbeRequest is one probe run across the group.
type UDPProbeRequest struct {
	Target   string        // the destination key to write results under (or a host)
	Duration time.Duration // per node
	Servers  []string      // host:port; empty = the group's udp-probe-servers
	Nodes    []string      // empty = every UDP capable node
}

// UDPProbeNodeResult is one node's line of the report.
type UDPProbeNodeResult struct {
	Node string `json:"node"`
	smart.UDPProbeResult
	WeightBefore float64 `json:"weight_before"`
	WeightAfter  float64 `json:"weight_after"`
	Skipped      string  `json:"skipped,omitempty"`
}

// UDPProbeReport is the result of ProbeUDP.
type UDPProbeReport struct {
	Group      string               `json:"group"`
	Target     string               `json:"target"`
	DurationMs int64                `json:"duration_ms"`
	Servers    []string             `json:"servers"`
	Results    []UDPProbeNodeResult `json:"results"`
}

// ProbeTargetKey maps a probe's target hint to the key weights are stored under: an existing
// key is used as is, a host is mapped as a connection to it without a matching rule would be.
func (s *Smart) ProbeTargetKey(hint string) string {
	hint = strings.TrimSpace(hint)
	if hint == "" || strings.ContainsAny(hint, " [") {
		return hint
	}
	if h, _, err := net.SplitHostPort(hint); err == nil {
		hint = h
	}
	if addr, err := netip.ParseAddr(hint); err == nil {
		return smart.GetEffectiveTarget("", addr.String())
	}
	return smart.GetEffectiveTarget(hint, "")
}

func (s *Smart) resolveProbeServers(ctx context.Context, servers []string) ([]net.Addr, []string) {
	if len(servers) == 0 {
		servers = s.udpProbeServers
	}
	var addrs []net.Addr
	var used []string
	for _, srv := range servers {
		host, portStr, err := net.SplitHostPort(strings.TrimSpace(srv))
		if err != nil {
			continue
		}
		port, err := strconv.ParseUint(portStr, 10, 16)
		if err != nil || port == 0 {
			continue
		}
		ip, err := netip.ParseAddr(host)
		if err != nil {
			// mihomo's resolver, never the system resolver directly (see the PR #12 crash class)
			rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			ip, err = resolver.ResolveIP(rctx, host)
			cancel()
			if err != nil {
				log.Debugln("[Smart] Probe server [%s] not resolved: %v", srv, err)
				continue
			}
		}
		addrs = append(addrs, net.UDPAddrFromAddrPort(netip.AddrPortFrom(ip.Unmap(), uint16(port))))
		used = append(used, srv)
	}
	return addrs, used
}

// ProbeUDP runs a STUN probe through the group's nodes and writes the results into the
// target's UDP weights ("pre-meeting warm-up"). One probe runs per group at a time.
func (s *Smart) ProbeUDP(ctx context.Context, req UDPProbeRequest) (UDPProbeReport, error) {
	if strings.TrimSpace(req.Target) == "" {
		return UDPProbeReport{}, ErrProbeNoTarget
	}
	if req.Duration == 0 {
		req.Duration = defaultProbeDuration
	}
	if req.Duration < minProbeDuration || req.Duration > maxProbeDuration {
		return UDPProbeReport{}, ErrProbeDuration
	}
	if !s.probeRunning.CompareAndSwap(false, true) {
		return UDPProbeReport{}, ErrProbeBusy
	}
	defer s.probeRunning.Store(false)

	addrs, used := s.resolveProbeServers(ctx, req.Servers)
	if len(addrs) == 0 {
		return UDPProbeReport{}, ErrProbeNoServers
	}
	target := s.ProbeTargetKey(req.Target)
	s.udpRef.Touch(target, time.Now())

	var proxies []C.Proxy
	want := make(map[string]bool, len(req.Nodes))
	for _, n := range req.Nodes {
		want[n] = true
	}
	for _, p := range s.GetProxies(false) {
		if len(want) == 0 || want[p.Name()] {
			proxies = append(proxies, p)
		}
	}

	report := UDPProbeReport{
		Group:      s.Name(),
		Target:     target,
		DurationMs: req.Duration.Milliseconds(),
		Servers:    used,
		Results:    make([]UDPProbeNodeResult, len(proxies)),
	}
	count := int(req.Duration / smart.DefaultUDPProbeInterval)
	sem := make(chan struct{}, probeNodeConcurrency)
	var wg sync.WaitGroup
	for i, p := range proxies {
		wg.Add(1)
		go func(i int, p C.Proxy) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			report.Results[i] = s.probeNode(ctx, p, target, addrs, count)
		}(i, p)
	}
	wg.Wait()
	sort.Slice(report.Results, func(i, j int) bool { return report.Results[i].Node < report.Results[j].Node })
	return report, nil
}

func (s *Smart) probeNode(ctx context.Context, p C.Proxy, target string, addrs []net.Addr, count int) UDPProbeNodeResult {
	out := UDPProbeNodeResult{Node: p.Name()}
	switch p.Type() {
	case C.Compatible, C.Reject, C.RejectDrop, C.Pass:
		out.Skipped = "not a forwarding node"
		return out
	}
	if !p.SupportUDP() {
		out.Skipped = "node does not support UDP"
		return out
	}
	first := addrs[0].(*net.UDPAddr).AddrPort()
	metadata := &C.Metadata{
		NetWork: C.UDP,
		Type:    C.INNER,
		DstIP:   first.Addr(),
		DstPort: first.Port(),
	}
	lctx, cancel := context.WithTimeout(ctx, probeListenTimeout)
	pc, err := p.ListenPacketContext(lctx, metadata)
	cancel()
	if err != nil {
		out.UDPProbeResult = smart.UDPProbeResult{Loss: 1, Error: err.Error(), StartedAt: time.Now().Unix()}
	} else {
		out.UDPProbeResult = smart.RunSTUNProbe(ctx, pc, addrs, count, smart.DefaultUDPProbeInterval, smart.UDPProbeReplyWait)
		_ = pc.Close()
	}
	if ctx.Err() != nil && out.Sent == 0 {
		out.Skipped = "cancelled"
		return out
	}

	jitter := -1.0
	if out.Received >= 2 {
		jitter = out.JitterMs
	}
	var rtt int64
	if out.Received > 0 {
		rtt = int64(out.RTTMs + 0.5)
		if rtt == 0 {
			rtt = 1
		}
	}
	out.WeightBefore, out.WeightAfter = s.recordUDPQuality(target, p, nil, udpQualitySample{
		loss:         out.Loss,
		jitterMs:     jitter,
		rttMs:        rtt,
		countOutcome: true,
		ok:           out.Received > 0 && out.Loss < udpFailLoss,
		source:       smart.HistorySourceProbe,
	})
	return out
}

// ---------------------------------------------------------------- automatic probing

// probeBudget allows at most perHour node probes in each clock hour.
type probeBudget struct {
	mu      sync.Mutex
	perHour int
	hour    int64
	used    int
}

// take reserves up to n probes and returns how many were granted.
func (b *probeBudget) take(n int, now time.Time) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.perHour <= 0 || n <= 0 {
		return 0
	}
	if h := now.Unix() / 3600; h != b.hour {
		b.hour, b.used = h, 0
	}
	left := b.perHour - b.used
	if n > left {
		n = left
	}
	if n < 0 {
		n = 0
	}
	b.used += n
	return n
}

// autoProbeOnce probes the most recently active UDP target on its best few nodes, within the
// hourly budget. It returns how many nodes were probed.
func (s *Smart) autoProbeOnce(ctx context.Context, now time.Time) int {
	targets := s.udpRef.Targets(now.Add(-autoProbeTargetSince))
	if len(targets) == 0 {
		return 0
	}
	target := targets[0]
	var nodes []string
	if names, _, err := s.store.GetBestProxyForTarget(s.Name(), s.configName, target, true); err == nil {
		nodes = names
	}
	if len(nodes) == 0 {
		for _, p := range s.GetProxies(false) {
			if p.SupportUDP() {
				nodes = append(nodes, p.Name())
			}
		}
	}
	if len(nodes) > autoProbeMaxNodes {
		nodes = nodes[:autoProbeMaxNodes]
	}
	granted := s.autoProbe.take(len(nodes), now)
	if granted == 0 {
		return 0
	}
	report, err := s.ProbeUDP(ctx, UDPProbeRequest{Target: target, Duration: autoProbeDuration, Nodes: nodes[:granted]})
	if err != nil {
		log.Debugln("[Smart] Group [%s] auto probe skipped: %v", s.Name(), err)
		return 0
	}
	probed := 0
	for _, r := range report.Results {
		if r.Skipped == "" {
			probed++
		}
	}
	return probed
}

// initUDPQuality sets up the Phase 4 state; called from InitSmart.
func (s *Smart) initUDPQuality(opt SmartOption) {
	switch {
	case opt.UDPSampleInterval < 0:
		s.udpSampleInterval = 0
	case opt.UDPSampleInterval == 0:
		s.udpSampleInterval = smart.DefaultUDPSampleInterval
	default:
		s.udpSampleInterval = time.Duration(opt.UDPSampleInterval) * time.Millisecond
	}
	s.udpProbeServers = smart.DefaultSTUNServers
	if len(opt.UDPProbeServers) > 0 {
		s.udpProbeServers = opt.UDPProbeServers
	}
	s.autoProbe.perHour = opt.UDPProbeBudget
	s.udpRef = smart.NewUDPReplyReference(smart.MaxUDPReferenceTargets)
	retention := time.Duration(opt.HistoryRetentionHours) * time.Hour
	s.history = smart.NewHistoryRecorder(s.store, s.Name(), s.configName, retention, opt.HistoryMaxChunks)
}

// startUDPQuality starts the sampling/history loop and, with a budget, automatic probing.
func (s *Smart) startUDPQuality() {
	s.wg.Add(1)
	go s.liveUDPLoop()
	if s.autoProbe.perHour > 0 {
		s.startTimedTask(autoProbeInterval, autoProbeInterval, "Group UDP auto probe", func() {
			if n := s.autoProbeOnce(s.ctx, time.Now()); n > 0 {
				log.Debugln("[Smart] Group [%s] auto probed %d node(s)", s.Name(), n)
			}
		}, false)
	}
}
