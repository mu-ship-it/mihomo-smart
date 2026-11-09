package outboundgroup

// Phase 3 bug-hunt repros. Each test reproduces one ranked finding from
// docs/SMART-ASSESSMENT.md §10. See knownBug in smart_test.go for the marker contract.

import (
	"bytes"
	"context"
	"errors"
	"net"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/adapter/provider"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

func proxyByName(s *Smart, name string) C.Proxy {
	for _, p := range s.GetProxies(true) {
		if p.Name() == name {
			return p
		}
	}
	return nil
}

// ------------------------------------------------------------------ R2

// R2: a confirmed failure of node b for a target force-closes the target's connections on
// every node (smart.go:1638 -> closeSameConnection force=true, which ignored proxyName),
// including a healthy UDP call on node a. Fixed: closeNodeConnections is scoped to the failing
// node and to the failure's network.
func TestHuntR2FailureOnOneNodeMustNotCloseOtherNodes(t *testing.T) {
	a, b := newFakeNode("a"), newFakeNode("b")
	s := newTestSmart(t, a, b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// healthy TCP conn and UDP call on a
	b.tcpFail.Store(true)
	b.udpFail.Store(true)
	mt := tcpMeta("zoom.us")
	ct, err := s.DialContext(ctx, mt)
	if err != nil || lastNode(ct.Chains()) != "a" {
		t.Fatalf("setup tcp: %v %v", err, ct)
	}
	tt := statistic.NewTCPTracker(ct, statistic.DefaultManager, mt, nil, 0, 0, false)
	t.Cleanup(func() { _ = tt.Close() })
	call, _ := openTrackedUDP(t, s, 50100)
	if lastNode(call.Chains()) != "a" {
		t.Fatal("setup: call not on a")
	}
	// let the async stats of the setup dials settle
	time.Sleep(100 * time.Millisecond)

	// node b now fails repeatedly for the same target (driven synchronously)
	pb := proxyByName(s, "b")
	for i := 0; i < 12; i++ {
		m := tcpMeta("zoom.us")
		m.SrcPort = uint16(51000 + i)
		s.selectProxies(m, s.GetProxies(true))
		s.recordConnectionStats(m, pb, 30, 0, 0, 0, 0, 0, 0, nil, nil, errors.New("b: connection refused"))
	}

	a.mu.Lock()
	tcpClosed := a.tcpConns[0].closed.Load()
	udpClosed := a.udpConns[0].closed.Load()
	a.mu.Unlock()
	if tcpClosed || udpClosed {
		t.Fatal("regression " + "R2 (forced close not scoped to failing node)" + ": " + "node b failing closed node a's healthy connections (tcp closed=" + boolStr(tcpClosed) + ", udp call closed=" + boolStr(udpClosed) + ")")
	}
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// ------------------------------------------------------------------ U1

// U1: a node that accepts UDP sessions but never returns a packet (UDP blackhole, the
// "Zoom on a congested node" failure in its extreme form) is scored as a good node: the
// session closes without error, latency is 0 and is then treated as 1 ms.
func TestHuntU1UDPBlackholeMustNotScoreAsHealthy(t *testing.T) {
	a, b := newFakeNode("a"), newFakeNode("b")
	b.udpFail.Store(true) // b refuses UDP: a is the only node that ever accepts a session
	s := newTestSmart(t, a, b)

	target := ""
	sessions := 0
	for i := 0; i < 6; i++ {
		m := udpMeta("zoom.us", uint16(52000+i))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		pc, err := s.ListenPacketContext(ctx, m)
		cancel()
		target = m.SmartTarget
		if err != nil {
			// a never refuses, so a failed listen means Smart stopped offering a: fixed behaviour
			break
		}
		sessions++
		tr := statistic.NewUDPTracker(pc, statistic.DefaultManager, m, nil, 0, 0, false)
		// the client sends media for 10 s and never receives anything back
		_, _ = tr.WriteTo(bytes.Repeat([]byte{1}, 1200), &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9})
		tr.Info().Start = time.Now().Add(-10 * time.Second)
		_ = tr.Close()
		time.Sleep(20 * time.Millisecond)
	}
	if sessions == 0 {
		t.Fatal("setup: no session was ever opened on a")
	}
	// close callbacks record asynchronously: judge only once every session is in a's record
	key := smart.FormatDBKey(smart.KeyTypeStats, s.configName, s.Name(), target, "a")
	rec := s.store.GetOrCreateAtomicRecord(key, s.Name(), s.configName, target, "a")
	for deadline := time.Now().Add(5 * time.Second); rec.Success()+rec.Failure() < int64(sessions); {
		if time.Now().After(deadline) {
			t.Fatalf("setup: only %d of %d sessions recorded", rec.Success()+rec.Failure(), sessions)
		}
		time.Sleep(20 * time.Millisecond)
	}

	names, weights, err := s.store.GetBestProxyForTarget(s.Name(), s.configName, target, true)
	if err != nil && !strings.Contains(err.Error(), "no best node") {
		t.Fatal(err)
	}
	healthy := false
	for i, n := range names {
		if n == "a" && i < len(weights) && weights[i] >= smart.AllowedWeight {
			healthy = true
		}
	}
	offered := false
	cands, _, _ := s.selectProxies(udpMeta("zoom.us", 52100), s.GetProxies(true))
	for _, p := range cands {
		offered = offered || p.Name() == "a"
	}
	if healthy && offered {
		t.Fatalf("regression U1 (UDP blackhole scored healthy): node a never answered %d UDP sessions yet is still offered and ranked %s with weight >= AllowedWeight",
			sessions, strings.Join(names, ","))
	}
}

// ------------------------------------------------------------------ R1

// R1: a forced close writes another connection's Metadata.SmartBlock while that connection's
// close callback reads it. Regression test for the race detector (CI runs this package with
// -race); without -race it still exercises the path.
func TestHuntR1ForcedCloseRacesCloseCallback(t *testing.T) {
	a, b := newFakeNode("a"), newFakeNode("b")
	s := newTestSmart(t, a, b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		m := tcpMeta("race.example")
		m.SrcPort = uint16(53000 + i)
		c, err := s.DialContext(ctx, m)
		if err != nil {
			t.Fatal(err)
		}
		tr := statistic.NewTCPTracker(c, statistic.DefaultManager, m, nil, 0, 0, false)
		wg.Add(2)
		go func() { defer wg.Done(); _ = tr.Close() }()
		go func(m *C.Metadata, node string) {
			defer wg.Done()
			s.closeNodeConnections(&C.Metadata{UUID: "other", NetWork: C.TCP}, node, m.SmartTarget)
		}(m, lastNode(c.Chains()))
	}
	wg.Wait()
	time.Sleep(200 * time.Millisecond)
}

// ------------------------------------------------------------------ LC

func countTimedTasks() int {
	var buf bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&buf, 2)
	return strings.Count(buf.String(), "outboundgroup.runTimedTask.func1(")
}

func newNamedSmart(t *testing.T, nodes ...*fakeNode) *Smart {
	t.Helper()
	proxies := make([]C.Proxy, 0, len(nodes))
	for _, n := range nodes {
		proxies = append(proxies, adapter.NewProxy(n))
	}
	pd, err := provider.NewCompatibleProvider("smart-lc", proxies, provider.NewHealthCheck(proxies, "", 0, 0, true, nil))
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewSmart(GroupCommonOption{Name: "smart-lc-" + t.Name() + "-" + time.Now().Format("150405.000000000")},
		SmartOption{}, adapter.NewProxy(outbound.NewCompatible()), []P.ProxyProvider{pd})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func waitTasks(want int) int {
	deadline := time.Now().Add(2 * time.Second)
	for {
		n := countTimedTasks()
		if n == want || time.Now().After(deadline) {
			return n
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// LC: a config reload parses the new config (NewSmart -> InitSmart) before ApplyConfig closes
// the old groups (hub/route/configs.go -> executor.ApplyConfig -> closeSmart). The
// process-global tasks (queue flush, cache tuning, orphan clean-up) were started under the
// old group's context, so they die with it and never restart.
func TestHuntLCGlobalTasksSurviveConfigReload(t *testing.T) {
	base := countTimedTasks()
	old := newNamedSmart(t, newFakeNode("a"))
	withGlobals := countTimedTasks() - base
	fresh := newNamedSmart(t, newFakeNode("a")) // the new config, parsed before the old one closes
	defer fresh.Close()
	groupOnly := countTimedTasks() - base - withGlobals
	_ = old.Close() // ApplyConfig -> closeSmart on the old groups

	remaining := waitTasks(base+withGlobals) - base
	if withGlobals <= groupOnly {
		t.Fatalf("setup: expected the first group to also own the global tasks (%d vs %d)", withGlobals, groupOnly)
	}
	if remaining == groupOnly {
		t.Fatal("regression " + "LC (global Smart tasks die on config reload)" + ": " + "after reload only the group's own tasks run (" + itoa(remaining) + "), the " + itoa(withGlobals-groupOnly) + " global tasks are gone")
	}
}

// LC: only the first Close in the process lifetime flushes the queued learning
// (flushQueueOnce is never reset), so every later reload or shutdown drops it.
func TestHuntLCEveryCloseFlushesQueuedLearning(t *testing.T) {
	first := newNamedSmart(t, newFakeNode("a"))
	_ = first.Close() // consumes the once (if an earlier test has not already)

	// as in a reload, another group stays alive while this one closes: only Close's own flush
	// (not the last-group shutdown flush) can persist the queue here
	keep := newNamedSmart(t, newFakeNode("a"))
	defer keep.Close()
	s := newNamedSmart(t, newFakeNode("a"))
	// start from an empty queue: ops left by earlier tests could otherwise hit the batch
	// threshold on the append below and be saved before the check
	s.store.FlushQueue(true)
	s.store.AppendToGlobalQueue(smart.StoreOperation{
		Type: smart.OpSaveRanking, Group: s.Name(), Config: s.configName, Data: []byte(`{}`),
	})
	if smart.QueuedOperations() != 1 {
		t.Fatalf("setup: expected exactly 1 queued operation, got %d", smart.QueuedOperations())
	}
	_ = s.Close()
	if smart.QueuedOperations() > 0 {
		t.Fatal("regression " + "LC (queued learning not flushed on later closes)" + ": " + "queue still holds " + itoa(smart.QueuedOperations()) + " operation(s) after Close")
	}
}

func itoa(n int) string {
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
		if n == 0 {
			break
		}
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// The other half of R2: scoping must not stop a failing node's own connections from being
// closed, and a TCP failure must not close a UDP flow on the same node.
func TestSmartFailingNodeOwnConnectionsAreStillClosed(t *testing.T) {
	a, b := newFakeNode("a"), newFakeNode("b")
	s := newTestSmart(t, a, b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	a.tcpFail.Store(true)
	a.udpFail.Store(true)
	m := tcpMeta("zoom.us")
	c, err := s.DialContext(ctx, m)
	if err != nil || lastNode(c.Chains()) != "b" {
		t.Fatalf("setup tcp on b: %v", err)
	}
	tr := statistic.NewTCPTracker(c, statistic.DefaultManager, m, nil, 0, 0, false)
	t.Cleanup(func() { _ = tr.Close() })
	call, _ := openTrackedUDP(t, s, 50200)
	if lastNode(call.Chains()) != "b" {
		t.Fatal("setup: udp not on b")
	}

	s.closeNodeConnections(&C.Metadata{UUID: "failure", NetWork: C.TCP}, "b", m.SmartTarget)

	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.tcpConns[0].closed.Load() {
		t.Fatal("the failing node's TCP connection to the target was not closed")
	}
	if b.udpConns[0].closed.Load() {
		t.Fatal("a TCP failure closed the node's UDP flow")
	}
}
