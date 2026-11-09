package outboundgroup

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/adapter/provider"
	"github.com/metacubex/mihomo/component/smart"
	"github.com/metacubex/mihomo/component/smart/stuntest"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

// ---------------------------------------------------------------- fake UDP links

// echoLink is an in-memory UDP path: every packet written comes back as a reply after
// delay(i), unless drop(i). Loss and jitter are deterministic functions of the packet index.
type echoLink struct {
	replies chan []byte
	closed  chan struct{}
	once    sync.Once
	n       atomic.Int64
	drop    func(int64) bool
	delay   func(int64) time.Duration
	local   net.Addr
	remote  atomic.Value // net.Addr

	mu       sync.Mutex
	deadline time.Time
}

func newEchoLink(drop func(int64) bool, delay func(int64) time.Duration) *echoLink {
	return &echoLink{
		replies: make(chan []byte, 1024),
		closed:  make(chan struct{}),
		drop:    drop,
		delay:   delay,
		local:   &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 40000},
	}
}

func (l *echoLink) WriteTo(b []byte, addr net.Addr) (int, error) {
	select {
	case <-l.closed:
		return 0, net.ErrClosed
	default:
	}
	l.remote.Store(addr)
	i := l.n.Add(1) - 1
	if l.drop != nil && l.drop(i) {
		return len(b), nil
	}
	cp := append([]byte(nil), b...)
	d := time.Duration(0)
	if l.delay != nil {
		d = l.delay(i)
	}
	time.AfterFunc(d, func() {
		select {
		case l.replies <- cp:
		case <-l.closed:
		}
	})
	return len(b), nil
}

func (l *echoLink) ReadFrom(b []byte) (int, net.Addr, error) {
	l.mu.Lock()
	dl := l.deadline
	l.mu.Unlock()
	var timeout <-chan time.Time
	if !dl.IsZero() {
		t := time.NewTimer(time.Until(dl))
		defer t.Stop()
		timeout = t.C
	}
	select {
	case p := <-l.replies:
		addr, _ := l.remote.Load().(net.Addr)
		return copy(b, p), addr, nil
	case <-l.closed:
		return 0, nil, net.ErrClosed
	case <-timeout:
		return 0, nil, os.ErrDeadlineExceeded
	}
}

func (l *echoLink) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}
func (l *echoLink) LocalAddr() net.Addr { return l.local }
func (l *echoLink) SetDeadline(t time.Time) error {
	return l.SetReadDeadline(t)
}
func (l *echoLink) SetReadDeadline(t time.Time) error {
	l.mu.Lock()
	l.deadline = t
	l.mu.Unlock()
	return nil
}
func (l *echoLink) SetWriteDeadline(time.Time) error { return nil }

// linkNode is an outbound whose UDP sessions run over echo links with the node's loss and
// jitter pattern, and whose TCP dials always succeed.
type linkNode struct {
	*outbound.Base
	drop  func(int64) bool
	delay func(int64) time.Duration
}

func newLinkNode(name string, drop func(int64) bool, delay func(int64) time.Duration) *linkNode {
	return &linkNode{
		Base:  outbound.NewBase(outbound.BaseOption{Name: name, Type: C.Vmess, UDP: true}),
		drop:  drop,
		delay: delay,
	}
}

func (n *linkNode) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	a, b := net.Pipe()
	go func() { _, _ = b.Read(make([]byte, 1)); _ = b.Close() }()
	return outbound.NewConn(a, n), nil
}

func (n *linkNode) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	return outbound.NewPacketConn(newEchoLink(n.drop, n.delay), n), nil
}

// lossyUDPNode is a real local UDP socket that silently drops every dropEvery-th packet it
// sends (a lossy uplink), for the STUN probe tests.
type lossyUDPNode struct {
	*outbound.Base
	dropEvery int64
}

type dropWritesConn struct {
	net.PacketConn
	n     atomic.Int64
	every int64
}

func (c *dropWritesConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	if c.every > 0 && c.n.Add(1)%c.every == 0 {
		return len(b), nil
	}
	return c.PacketConn.WriteTo(b, addr)
}

func (n *lossyUDPNode) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	return nil, errFakeDial
}

func (n *lossyUDPNode) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	return outbound.NewPacketConn(&dropWritesConn{PacketConn: pc, every: n.dropEvery}, n), nil
}

func newLossyUDPNode(name string, dropEvery int64) *lossyUDPNode {
	return &lossyUDPNode{Base: outbound.NewBase(outbound.BaseOption{Name: name, Type: C.Vmess, UDP: true}), dropEvery: dropEvery}
}

func newSmartWith(t *testing.T, opt SmartOption, nodes ...C.ProxyAdapter) *Smart {
	t.Helper()
	proxies := make([]C.Proxy, 0, len(nodes))
	for _, n := range nodes {
		proxies = append(proxies, adapter.NewProxy(n))
	}
	pd, err := provider.NewCompatibleProvider("smart-udp", proxies, provider.NewHealthCheck(proxies, "", 0, 0, true, nil))
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("smart-udp-%s-%d", t.Name(), testGroupSeq.Add(1))
	s, err := NewSmart(GroupCommonOption{Name: name}, opt, adapter.NewProxy(outbound.NewCompatible()), []P.ProxyProvider{pd})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func (s *Smart) testRecord(target, node string) *smart.AtomicStatsRecord {
	key := smart.FormatDBKey(smart.KeyTypeStats, s.configName, s.Name(), target, node)
	return s.store.GetOrCreateAtomicRecord(key, s.Name(), s.configName, target, node)
}

// ---------------------------------------------------------------- live sampling

// A Zoom-like call runs on two nodes at once: same target, same send rate, but one path loses
// every 4th reply and its delay swings between 5 and 35 ms every 8 packets (queue build-up). While the calls are still open, the lossy
// node's UDP weight must fall below the clean node's within a few sample intervals, the clean
// node must lead the UDP ranking, and the TCP weights must not move.
func TestSmartLiveUDPSamplingDemotesLossyNodeWhileCallRuns(t *testing.T) {
	const (
		interval = 100 * time.Millisecond
		within   = 3 * time.Second // N: 30 sample intervals
	)
	clean := newLinkNode("clean", nil, func(int64) time.Duration { return 5 * time.Millisecond })
	lossy := newLinkNode("lossy",
		func(i int64) bool { return i%4 == 3 },
		func(i int64) time.Duration { return time.Duration(5+30*((i/8)%2)) * time.Millisecond })
	s := newSmartWith(t, SmartOption{UDPSampleInterval: int(interval / time.Millisecond)}, clean, lossy)

	probe := udpMeta("zoom.us", 60000)
	s.selectProxies(probe, s.GetProxies(true))
	target := probe.SmartTarget

	// identical TCP history on both nodes
	for _, name := range []string{"clean", "lossy"} {
		for i := 0; i < 3; i++ {
			m := tcpMeta("zoom.us")
			s.selectProxies(m, s.GetProxies(true))
			s.recordConnectionStats(m, proxyByName(s, name), 30, 40, 4096, 65536, 0, 0, 60000, nil, nil, nil)
		}
	}
	tcpClean := s.testRecord(target, "clean").GetWeight(smart.WeightTypeTCP)
	tcpLossy := s.testRecord(target, "lossy").GetWeight(smart.WeightTypeTCP)
	if tcpClean <= 0 || tcpClean != tcpLossy {
		t.Fatalf("setup: TCP weights %v / %v", tcpClean, tcpLossy)
	}

	// one call per node, opened through the group's wrapping and tracked like the tunnel does
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	var trackers []C.PacketConn
	for i, name := range []string{"clean", "lossy"} {
		m := udpMeta("zoom.us", uint16(60100+i))
		p := proxyByName(s, name)
		s.selectProxies(m, s.GetProxies(true))
		pc, err := p.ListenPacketContext(ctx, m)
		if err != nil {
			t.Fatal(err)
		}
		tr := statistic.NewUDPTracker(s.WrapPacketConnWithMetric(pc, p, m, 0), statistic.DefaultManager, m, nil, 0, 0, false)
		dst := &net.UDPAddr{IP: net.IPv4(170, 114, 52, 2), Port: 8801}
		wg.Add(2)
		go func() { // 200 packets/s of media
			defer wg.Done()
			tick := time.NewTicker(5 * time.Millisecond)
			defer tick.Stop()
			payload := make([]byte, 160)
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					_, _ = tr.WriteTo(payload, dst)
				}
			}
		}()
		go func() {
			defer wg.Done()
			buf := make([]byte, 2048)
			for {
				if _, _, err := tr.ReadFrom(buf); err != nil {
					return
				}
			}
		}()
		trackers = append(trackers, tr)
	}
	defer func() {
		cancel()
		for _, tr := range trackers {
			_ = tr.Close()
		}
		wg.Wait()
	}()
	if got := s.LiveUDPSessions(); got != 2 {
		t.Fatalf("setup: %d live sessions sampled, want 2", got)
	}

	start := time.Now()
	var udpClean, udpLossy float64
	for {
		udpClean = s.testRecord(target, "clean").GetWeight(smart.WeightTypeUDP)
		udpLossy = s.testRecord(target, "lossy").GetWeight(smart.WeightTypeUDP)
		lossL, _, nL := s.testRecord(target, "lossy").UDPQuality()
		if udpClean > 0 && udpLossy > 0 && udpLossy < udpClean && nL >= 3 && lossL > 0.1 {
			break
		}
		if time.Since(start) > within {
			lossC, jitC, _ := s.testRecord(target, "clean").UDPQuality()
			lossL, jitL, _ := s.testRecord(target, "lossy").UDPQuality()
			t.Fatalf("after %v: UDP weight clean %.3f (loss %.3f jitter %.1f) lossy %.3f (loss %.3f jitter %.1f)",
				within, udpClean, lossC, jitC, udpLossy, lossL, jitL)
		}
		time.Sleep(20 * time.Millisecond)
	}
	elapsed := time.Since(start)
	t.Logf("lossy node demoted after %v (clean %.3f, lossy %.3f)", elapsed.Round(10*time.Millisecond), udpClean, udpLossy)

	lossC, jitC, _ := s.testRecord(target, "clean").UDPQuality()
	lossL, jitL, _ := s.testRecord(target, "lossy").UDPQuality()
	t.Logf("estimates: clean loss %.3f jitter %.1f ms, lossy loss %.3f jitter %.1f ms", lossC, jitC, lossL, jitL)
	if lossC > 0.1 || lossL < 0.15 || lossL > 0.4 || jitL <= jitC {
		t.Fatalf("estimates: clean loss %.3f jitter %.1f ms, lossy loss %.3f jitter %.1f ms", lossC, jitC, lossL, jitL)
	}
	names, _, err := s.store.GetBestProxyForTarget(s.Name(), s.configName, target, true)
	if err != nil || len(names) == 0 || names[0] != "clean" {
		t.Fatalf("UDP ranking %v (%v), want clean first", names, err)
	}
	if c, l := s.testRecord(target, "clean").GetWeight(smart.WeightTypeTCP), s.testRecord(target, "lossy").GetWeight(smart.WeightTypeTCP); c != tcpClean || l != tcpLossy {
		t.Fatalf("TCP weights moved: clean %v -> %v, lossy %v -> %v", tcpClean, c, tcpLossy, l)
	}
	if h := s.History().Report(target, smart.HistoryNetworkUDP, "lossy", time.UTC); h.Events == 0 {
		t.Fatal("live windows were not written to the history")
	}
}

// Sampling off: sessions are not wrapped or registered.
func TestSmartLiveUDPSamplingCanBeTurnedOff(t *testing.T) {
	s := newSmartWith(t, SmartOption{UDPSampleInterval: -1}, newLinkNode("a", nil, nil))
	m := udpMeta("zoom.us", 60200)
	s.selectProxies(m, s.GetProxies(true))
	p := proxyByName(s, "a")
	pc, _ := p.ListenPacketContext(context.Background(), m)
	wpc := s.WrapPacketConnWithMetric(pc, p, m, 0)
	defer wpc.Close()
	if s.LiveUDPSessions() != 0 {
		t.Fatal("a session was registered with sampling off")
	}
}

// Closing a session unregisters it, so the live table cannot leak.
func TestSmartLiveUDPSessionsAreReleasedOnClose(t *testing.T) {
	s := newSmartWith(t, SmartOption{}, newLinkNode("a", nil, nil))
	p := proxyByName(s, "a")
	for i := 0; i < 50; i++ {
		m := udpMeta("zoom.us", uint16(61000+i))
		s.selectProxies(m, s.GetProxies(true))
		pc, _ := p.ListenPacketContext(context.Background(), m)
		tr := statistic.NewUDPTracker(s.WrapPacketConnWithMetric(pc, p, m, 0), statistic.DefaultManager, m, nil, 0, 0, false)
		_ = tr.Close()
	}
	if n := s.LiveUDPSessions(); n != 0 {
		t.Fatalf("%d sessions left after every session closed", n)
	}
}

// ---------------------------------------------------------------- active probe

func TestSmartProbeUDPWritesResultsIntoUDPWeights(t *testing.T) {
	srv := stuntest.Start(t, nil, nil)
	clean := newLossyUDPNode("clean", 0)
	lossy := newLossyUDPNode("lossy", 4) // every 4th request never leaves
	s := newSmartWith(t, SmartOption{UDPProbeServers: []string{srv.Addr()}}, clean, lossy, newFakeNode("tcp-only-ish"))

	rep, err := s.ProbeUDP(context.Background(), UDPProbeRequest{Target: "zoom.us", Duration: 400 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Target != s.ProbeTargetKey("zoom.us") || len(rep.Results) != 3 || rep.Servers[0] != srv.Addr() {
		t.Fatalf("report header: %+v", rep)
	}
	byNode := map[string]UDPProbeNodeResult{}
	for _, r := range rep.Results {
		byNode[r.Node] = r
	}
	c, l := byNode["clean"], byNode["lossy"]
	if c.Sent != 20 || c.Received != 20 || c.Loss != 0 || c.Mapped == "" {
		t.Fatalf("clean: %+v", c)
	}
	if l.Loss < 0.2 || l.Loss > 0.3 {
		t.Fatalf("lossy: loss %.3f (%+v)", l.Loss, l)
	}
	if !(l.WeightAfter < c.WeightAfter) || c.WeightAfter <= 0 {
		t.Fatalf("weights after probe: clean %.3f lossy %.3f", c.WeightAfter, l.WeightAfter)
	}
	if w := s.testRecord(rep.Target, "lossy").GetWeight(smart.WeightTypeUDP); w != l.WeightAfter {
		t.Fatalf("stored UDP weight %.3f != reported %.3f", w, l.WeightAfter)
	}
	if s.testRecord(rep.Target, "clean").GetWeight(smart.WeightTypeTCP) != 0 {
		t.Fatal("a UDP probe wrote a TCP weight")
	}
	if h := s.History().Report(rep.Target, smart.HistoryNetworkUDP, "", time.UTC); h.Events != 3 {
		t.Fatalf("history events %d, want one per probed node", h.Events)
	}
}

func TestSmartProbeUDPRejectsBadRequests(t *testing.T) {
	srv := stuntest.Start(t, nil, func(int64) time.Duration { return 50 * time.Millisecond })
	s := newSmartWith(t, SmartOption{UDPProbeServers: []string{srv.Addr()}}, newLossyUDPNode("a", 0))
	ctx := context.Background()
	if _, err := s.ProbeUDP(ctx, UDPProbeRequest{}); err != ErrProbeNoTarget {
		t.Fatalf("no target: %v", err)
	}
	if _, err := s.ProbeUDP(ctx, UDPProbeRequest{Target: "x", Duration: time.Minute}); err != ErrProbeDuration {
		t.Fatalf("too long: %v", err)
	}
	if _, err := s.ProbeUDP(ctx, UDPProbeRequest{Target: "x", Servers: []string{"not-an-address"}}); err != ErrProbeNoServers {
		t.Fatalf("bad server: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = s.ProbeUDP(ctx, UDPProbeRequest{Target: "x", Duration: 500 * time.Millisecond})
	}()
	time.Sleep(100 * time.Millisecond)
	if _, err := s.ProbeUDP(ctx, UDPProbeRequest{Target: "x", Duration: 200 * time.Millisecond}); err != ErrProbeBusy {
		t.Fatalf("concurrent probe: %v", err)
	}
	<-done
}

func TestSmartAutoProbeStaysWithinHourlyBudget(t *testing.T) {
	old := autoProbeDuration
	autoProbeDuration = 200 * time.Millisecond
	defer func() { autoProbeDuration = old }()

	srv := stuntest.Start(t, nil, nil)
	s := newSmartWith(t, SmartOption{UDPProbeServers: []string{srv.Addr()}, UDPProbeBudget: 2},
		newLossyUDPNode("a", 0), newLossyUDPNode("b", 0), newLossyUDPNode("c", 0))
	now := time.Date(2026, 11, 9, 9, 5, 0, 0, time.UTC)

	if n := s.autoProbeOnce(context.Background(), now); n != 0 {
		t.Fatalf("no UDP target seen yet, probed %d", n)
	}
	s.udpRef.Touch("zoom.us", now)
	if n := s.autoProbeOnce(context.Background(), now); n != 2 {
		t.Fatalf("first run probed %d nodes, want the budget of 2", n)
	}
	if n := s.autoProbeOnce(context.Background(), now.Add(30*time.Minute)); n != 0 {
		t.Fatalf("same hour probed %d more", n)
	}
	if n := s.autoProbeOnce(context.Background(), now.Add(time.Hour)); n != 2 {
		t.Fatalf("next hour probed %d, want 2", n)
	}
	off := newSmartWith(t, SmartOption{UDPProbeServers: []string{srv.Addr()}}, newLossyUDPNode("a", 0))
	off.udpRef.Touch("zoom.us", now)
	if n := off.autoProbeOnce(context.Background(), now); n != 0 {
		t.Fatalf("budget 0 (default) must not probe, probed %d", n)
	}
}

func TestProbeBudget(t *testing.T) {
	b := probeBudget{perHour: 5}
	t0 := time.Date(2026, 11, 9, 9, 0, 0, 0, time.UTC)
	if b.take(3, t0) != 3 || b.take(3, t0.Add(time.Minute)) != 2 || b.take(1, t0.Add(59*time.Minute)) != 0 {
		t.Fatal("budget exceeded within the hour")
	}
	if b.take(10, t0.Add(time.Hour)) != 5 {
		t.Fatal("budget did not renew in the next hour")
	}
	if (&probeBudget{}).take(1, t0) != 0 {
		t.Fatal("a zero budget granted a probe")
	}
}
