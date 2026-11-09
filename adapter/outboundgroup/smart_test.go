package outboundgroup

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/adapter/provider"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

// knownBug marks a test that exercises a confirmed, not-yet-fixed defect (see
// docs/SMART-ASSESSMENT.md). While the defect reproduces the test is skipped with the
// evidence; the moment the code stops reproducing it the test FAILS, so whoever fixes the
// bug has to delete the knownBug call and turn the check into a normal assertion.
func knownBug(t *testing.T, id string, reproduced bool, evidence string) {
	t.Helper()
	if reproduced {
		t.Skipf("KNOWN BUG %s reproduced (expected until fixed): %s", id, evidence)
	}
	t.Fatalf("KNOWN BUG %s no longer reproduces: remove the knownBug marker and assert the fixed behaviour", id)
}

// ---------------------------------------------------------------- fake nodes

type closeRecorderConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *closeRecorderConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

type closeRecorderPacketConn struct {
	net.PacketConn
	closed atomic.Bool
}

func (c *closeRecorderPacketConn) Close() error {
	c.closed.Store(true)
	return c.PacketConn.Close()
}

// fakeNode is an outbound whose TCP/UDP dial results are scripted by the test.
type fakeNode struct {
	*outbound.Base
	tcpFail  atomic.Bool
	udpFail  atomic.Bool
	tcpDelay time.Duration
	noRecord bool // do not keep conns (long-running memory tests)
	tcpDials atomic.Int64
	udpDials atomic.Int64
	mu       sync.Mutex
	tcpConns []*closeRecorderConn
	udpConns []*closeRecorderPacketConn
}

func newFakeNode(name string) *fakeNode {
	return &fakeNode{Base: outbound.NewBase(outbound.BaseOption{Name: name, Type: C.Vmess, UDP: true})}
}

var errFakeDial = errors.New("fake node: dial refused")

func (n *fakeNode) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	n.tcpDials.Add(1)
	if n.tcpDelay > 0 {
		select {
		case <-time.After(n.tcpDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if n.tcpFail.Load() {
		return nil, errFakeDial
	}
	a, b := net.Pipe()
	go func() { _, _ = b.Read(make([]byte, 1)); _ = b.Close() }()
	rc := &closeRecorderConn{Conn: a}
	if !n.noRecord {
		n.mu.Lock()
		n.tcpConns = append(n.tcpConns, rc)
		n.mu.Unlock()
	}
	return outbound.NewConn(rc, n), nil
}

func (n *fakeNode) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	n.udpDials.Add(1)
	if n.udpFail.Load() {
		return nil, errFakeDial
	}
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	rc := &closeRecorderPacketConn{PacketConn: pc}
	n.mu.Lock()
	n.udpConns = append(n.udpConns, rc)
	n.mu.Unlock()
	return outbound.NewPacketConn(rc, n), nil
}

var testGroupSeq atomic.Int64

func newTestSmart(t *testing.T, nodes ...*fakeNode) *Smart {
	t.Helper()
	proxies := make([]C.Proxy, 0, len(nodes))
	for _, n := range nodes {
		proxies = append(proxies, adapter.NewProxy(n))
	}
	hc := provider.NewHealthCheck(proxies, "", 0, 0, true, nil)
	pd, err := provider.NewCompatibleProvider("smart-test", proxies, hc)
	if err != nil {
		t.Fatal(err)
	}
	// Smart keeps learned state in a process-global store keyed by group name, so every
	// instance gets a unique name (go test -count=N reruns tests in the same process).
	name := fmt.Sprintf("smart-test-%s-%d", t.Name(), testGroupSeq.Add(1))
	s, err := NewSmart(GroupCommonOption{Name: name}, SmartOption{}, adapter.NewProxy(outbound.NewCompatible()), []P.ProxyProvider{pd})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func tcpMeta(host string) *C.Metadata {
	return &C.Metadata{
		NetWork: C.TCP,
		Type:    C.HTTP,
		Host:    host,
		DstPort: 443,
		SrcIP:   netip.MustParseAddr("192.168.1.10"),
		SrcPort: 50000,
	}
}

func udpMeta(host string, srcPort uint16) *C.Metadata {
	return &C.Metadata{
		NetWork: C.UDP,
		Type:    C.TUN,
		Host:    host,
		DstPort: 8801, // Zoom media
		SrcIP:   netip.MustParseAddr("192.168.1.10"),
		SrcPort: srcPort,
	}
}

func lastNode(chains C.Chain) string {
	if len(chains) == 0 {
		return ""
	}
	return chains[0]
}

// ---------------------------------------------------------------- pure helpers

func TestSmartDialBatchBounds(t *testing.T) {
	type span struct{ begin, end int }
	tests := []struct {
		name   string
		total  int
		pinned bool
		want   []span // per iteration 0..maxRetries-1
	}{
		{"empty", 0, false, []span{{0, 0}, {0, 0}, {0, 0}, {0, 0}, {0, 0}}},
		{"single node is retried", 1, false, []span{{0, 1}, {0, 1}, {0, 1}, {0, 1}, {0, 1}}},
		{"two nodes", 2, false, []span{{0, 1}, {1, 2}, {0, 0}, {0, 0}, {0, 0}}},
		{"prefix then parallel batches", 20, false, []span{{0, 1}, {1, 2}, {2, 3}, {3, 8}, {8, 13}}},
		{"last batch truncated", 10, false, []span{{0, 1}, {1, 2}, {2, 3}, {3, 8}, {8, 10}}},
		{"pinned goes one at a time", 20, true, []span{{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4, 5}}},
		{"pinned stops at end", 2, true, []span{{0, 1}, {1, 2}, {0, 0}, {0, 0}, {0, 0}}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < maxRetries; i++ {
				b, e := smartDialBatchBounds(tt.total, i, tt.pinned)
				if (span{b, e}) != tt.want[i] {
					t.Fatalf("iteration %d: got [%d,%d), want [%d,%d)", i, b, e, tt.want[i].begin, tt.want[i].end)
				}
				if e > tt.total || b > e {
					t.Fatalf("iteration %d: invalid span [%d,%d) for total %d", i, b, e, tt.total)
				}
			}
		})
	}
}

// ---------------------------------------------------------------- parallel dialing

func TestSmartParallelDialContext(t *testing.T) {
	a, b, c := newFakeNode("a"), newFakeNode("b"), newFakeNode("c")
	s := newTestSmart(t, a, b, c)
	proxies := s.GetProxies(true)
	byName := map[string]*fakeNode{"a": a, "b": b, "c": c}

	type outcome struct {
		delay time.Duration
		fail  bool
	}
	tests := []struct {
		name       string
		outcomes   map[string]outcome
		ctxTimeout time.Duration
		wantWinner string
		wantErr    bool
	}{
		{"fastest success wins", map[string]outcome{"a": {30 * time.Millisecond, false}, "b": {1 * time.Millisecond, false}, "c": {60 * time.Millisecond, false}}, time.Second, "b", false},
		{"failures are skipped", map[string]outcome{"a": {0, true}, "b": {0, true}, "c": {20 * time.Millisecond, false}}, time.Second, "c", false},
		{"all fail returns joined error", map[string]outcome{"a": {0, true}, "b": {0, true}, "c": {0, true}}, time.Second, "", true},
		{"deadline returns ctx error", map[string]outcome{"a": {time.Second, false}, "b": {time.Second, false}, "c": {time.Second, false}}, 50 * time.Millisecond, "", true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var mu sync.Mutex
			var conns []*closeRecorderConn
			dial := func(ctx context.Context, p C.Proxy, m *C.Metadata, start time.Time) (C.Conn, int64, error) {
				o := tt.outcomes[p.Name()]
				select {
				case <-time.After(o.delay):
				case <-ctx.Done():
					// a late success after cancel still hands a conn back; the group must close it
					if o.fail {
						return nil, 0, ctx.Err()
					}
				}
				if o.fail {
					return nil, 0, errFakeDial
				}
				x, y := net.Pipe()
				_ = y.Close()
				rc := &closeRecorderConn{Conn: x}
				mu.Lock()
				conns = append(conns, rc)
				mu.Unlock()
				return outbound.NewConn(rc, byName[p.Name()]), time.Since(start).Milliseconds(), nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), tt.ctxTimeout)
			defer cancel()
			p, conn, _, err := s.ParallelDialContext(ctx, proxies, tcpMeta("example.com"), time.Now(), dial)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got winner %v", p.Name())
				}
				if tt.name == "all fail returns joined error" && !errors.Is(err, errFakeDial) {
					t.Fatalf("error should wrap every node error, got %v", err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if p.Name() != tt.wantWinner {
					t.Fatalf("winner = %s, want %s", p.Name(), tt.wantWinner)
				}
				_ = conn.Close()
			}
			// every losing connection must be closed (no leak)
			deadline := time.Now().Add(2 * time.Second)
			for {
				open := 0
				mu.Lock()
				for _, rc := range conns {
					if !rc.closed.Load() {
						open++
					}
				}
				mu.Unlock()
				if open == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("%d connection(s) leaked after the race", open)
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}

// ---------------------------------------------------------------- node selection

func TestSmartDialContextFailsOverToHealthyNode(t *testing.T) {
	tests := []struct {
		name    string
		failing []string
		wantErr bool
		healthy []string
	}{
		{"one bad node", []string{"a"}, false, []string{"b", "c"}},
		{"two bad nodes", []string{"a", "b"}, false, []string{"c"}},
		{"all nodes failing", []string{"a", "b", "c"}, true, nil},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			nodes := map[string]*fakeNode{"a": newFakeNode("a"), "b": newFakeNode("b"), "c": newFakeNode("c")}
			for _, n := range tt.failing {
				nodes[n].tcpFail.Store(true)
			}
			s := newTestSmart(t, nodes["a"], nodes["b"], nodes["c"])

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := s.DialContext(ctx, tcpMeta("example.com"))
			if tt.wantErr {
				if err == nil {
					_ = conn.Close()
					t.Fatal("expected an error when every node fails")
				}
				for name, n := range nodes {
					if n.tcpDials.Load() == 0 {
						t.Fatalf("node %s was never tried before giving up", name)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("dial failed although healthy nodes exist: %v", err)
			}
			defer conn.Close()
			got := lastNode(conn.Chains())
			ok := false
			for _, h := range tt.healthy {
				ok = ok || got == h
			}
			if !ok {
				t.Fatalf("connection went through %q (chains %v), want one of %v", got, conn.Chains(), tt.healthy)
			}
			if !strings.Contains(strings.Join(conn.Chains(), ","), s.Name()) {
				t.Fatalf("group %s missing from chains %v", s.Name(), conn.Chains())
			}
		})
	}
}

func TestSmartListenPacketContextFailsOver(t *testing.T) {
	a, b := newFakeNode("a"), newFakeNode("b")
	a.udpFail.Store(true)
	s := newTestSmart(t, a, b)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pc, err := s.ListenPacketContext(ctx, udpMeta("zoom.us", 50001))
	if err != nil {
		t.Fatalf("UDP listen failed although node b is healthy: %v", err)
	}
	defer pc.Close()
	if got := lastNode(pc.Chains()); got != "b" {
		t.Fatalf("UDP session went through %q, want b", got)
	}

	b.udpFail.Store(true)
	if pc2, err := s.ListenPacketContext(ctx, udpMeta("zoom.us", 50002)); err == nil {
		_ = pc2.Close()
		t.Fatal("expected an error when every node refuses UDP")
	}
}

// ---------------------------------------------------------------- bug B: cross-node close

// openTrackedUDP opens a UDP session through the group and registers it with the global
// connection manager the way the tunnel does, so closeSameConnection can see it.
func openTrackedUDP(t *testing.T, s *Smart, srcPort uint16) (C.PacketConn, *C.Metadata) {
	t.Helper()
	m := udpMeta("zoom.us", srcPort)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pc, err := s.ListenPacketContext(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	tracker := statistic.NewUDPTracker(pc, statistic.DefaultManager, m, nil, 0, 0, false)
	t.Cleanup(func() { _ = tracker.Close() })
	return pc, m
}

// listenUntilNode opens new UDP flows to the call's target until one lands on the wanted
// node (at most 3 tries); flows that fail are part of the scenario.
func listenUntilNode(t *testing.T, s *Smart, node string, srcPort uint16) C.PacketConn {
	t.Helper()
	for i := uint16(0); i < 3; i++ {
		m := udpMeta("zoom.us", srcPort+i)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		pc, err := s.ListenPacketContext(ctx, m)
		cancel()
		if err != nil {
			continue
		}
		tracker := statistic.NewUDPTracker(pc, statistic.DefaultManager, m, nil, 0, 0, false)
		t.Cleanup(func() { _ = tracker.Close() })
		if lastNode(pc.Chains()) == node {
			return pc
		}
	}
	t.Fatalf("setup: no new flow landed on %s", node)
	return nil
}

// smart.go adoptUnwrapWinner -> closeSameConnection(force=false): a new flow to the same
// target that wins on a different node closes every live flow of that target on other
// nodes, even though nothing was wrong with them. For a Zoom call that is the media flow.
func TestSmartNewFlowOnOtherNodeMustNotKillHealthyUDPSession(t *testing.T) {
	a, b := newFakeNode("a"), newFakeNode("b")
	s := newTestSmart(t, a, b)

	// the call's media flow lands on a (b refuses at that moment)
	b.udpFail.Store(true)
	call, callMeta := openTrackedUDP(t, s, 50010)
	if got := lastNode(call.Chains()); got != "a" {
		t.Fatalf("setup: call on %q, want a", got)
	}
	if callMeta.SmartTarget == "" {
		t.Fatal("setup: SmartTarget not assigned")
	}

	// While the call keeps working, node a refuses new UDP dials for a moment. With the
	// target pinned to a, the first new flow fails and drops the pin; the next new flow is
	// unpinned and wins on b.
	a.udpFail.Store(true)
	b.udpFail.Store(false)
	_ = listenUntilNode(t, s, "b", 50011)

	a.mu.Lock()
	callClosed := a.udpConns[0].closed.Load()
	a.mu.Unlock()

	knownBug(t, "B (smart.go:518 cross-node close)", callClosed,
		"healthy UDP session on node a was closed because another flow to "+callMeta.SmartTarget+" succeeded on node b")
	// after the fix: the call must still be open
}

// Same-node siblings are never closed today; this guards the part that already works.
func TestSmartNewFlowOnSameNodeKeepsSiblings(t *testing.T) {
	a, b := newFakeNode("a"), newFakeNode("b")
	b.udpFail.Store(true)
	s := newTestSmart(t, a, b)

	first, _ := openTrackedUDP(t, s, 50020)
	_, _ = openTrackedUDP(t, s, 50021)
	if lastNode(first.Chains()) != "a" {
		t.Fatalf("setup: first flow not on a")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.udpConns[0].closed.Load() {
		t.Fatal("a sibling flow on the same node was closed")
	}
}

// TCP variant of bug B (the PR #15 symptom: concurrent page loads kill each other).
// Driven at the unit level so the close can only come from smart.go:518: no dial fails in
// this test, so the failure path (recordConnectionStats -> closeSameConnection force=true)
// cannot fire.
func TestSmartNewFlowOnOtherNodeMustNotKillHealthyTCPConn(t *testing.T) {
	a, b := newFakeNode("a"), newFakeNode("b")
	s := newTestSmart(t, a, b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m1 := tcpMeta("example.com")
	c1, err := s.DialContext(ctx, m1)
	if err != nil {
		t.Fatal(err)
	}
	tr := statistic.NewTCPTracker(c1, statistic.DefaultManager, m1, nil, 0, 0, false)
	t.Cleanup(func() { _ = tr.Close() })

	first, other := a, b
	if lastNode(c1.Chains()) == "b" {
		first, other = b, a
	}
	var otherProxy C.Proxy
	for _, p := range s.GetProxies(true) {
		if p.Name() == other.Name() {
			otherProxy = p
		}
	}

	// a second connection to the same target is won by the other node
	m2 := tcpMeta("example.com")
	m2.SrcPort = 50001
	m2.SmartTarget = m1.SmartTarget
	m2.WildcardTarget = m1.WildcardTarget
	m2.UUID = "second-connection"
	s.adoptUnwrapWinner(m2, otherProxy)

	first.mu.Lock()
	closed := first.tcpConns[0].closed.Load()
	first.mu.Unlock()
	if a.tcpDials.Load()+b.tcpDials.Load() != 1 {
		t.Fatalf("setup: expected exactly one dial, got a=%d b=%d", a.tcpDials.Load(), b.tcpDials.Load())
	}
	knownBug(t, "B (smart.go:518 cross-node close, TCP)", closed,
		"healthy TCP conn on node "+first.Name()+" was closed because another conn to "+m1.SmartTarget+" was won by node "+other.Name())
}
