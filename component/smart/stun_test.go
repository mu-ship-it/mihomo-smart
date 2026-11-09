package smart_test

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/smart"
	"github.com/metacubex/mihomo/component/smart/stuntest"
)

func TestSTUNMessagesRoundTrip(t *testing.T) {
	var tx [12]byte
	copy(tx[:], "0123456789ab")
	req := smart.NewSTUNBindingRequest(tx)
	if got, ok := smart.ParseSTUNBindingRequest(req); !ok || got != tx {
		t.Fatal("request does not parse back")
	}
	for _, mapped := range []netip.AddrPort{
		netip.MustParseAddrPort("203.0.113.7:54321"),
		netip.MustParseAddrPort("[2001:db8::1]:3478"),
	} {
		got, ap, ok := smart.ParseSTUNBindingResponse(smart.NewSTUNBindingResponse(tx, mapped))
		if !ok || got != tx || ap != mapped {
			t.Fatalf("response %v: ok=%v tx=%v mapped=%v", mapped, ok, got == tx, ap)
		}
	}
	if _, _, ok := smart.ParseSTUNBindingResponse(req); ok {
		t.Fatal("a request must not parse as a response")
	}
	if _, _, ok := smart.ParseSTUNBindingResponse([]byte{1, 1, 0}); ok {
		t.Fatal("a short packet must not parse")
	}
}

func TestRunSTUNProbeMeasuresLossRTTAndJitter(t *testing.T) {
	srv := stuntest.Start(t,
		func(n int64) bool { return n%4 == 3 }, // 25% loss
		func(n int64) time.Duration { return time.Duration(n%2) * 20 * time.Millisecond },
	)
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	addr, _ := net.ResolveUDPAddr("udp", srv.Addr())

	res := smart.RunSTUNProbe(context.Background(), pc, []net.Addr{addr}, 40, 5*time.Millisecond, 300*time.Millisecond)
	if res.Sent != 40 || res.Received != 30 {
		t.Fatalf("sent %d received %d, want 40/30 (%s)", res.Sent, res.Received, res.Error)
	}
	if res.Loss < 0.24 || res.Loss > 0.26 {
		t.Fatalf("loss %.3f, want 0.25", res.Loss)
	}
	if res.JitterMs < 5 || res.RTTMs < res.MinRTTMs || res.MinRTTMs <= 0 {
		t.Fatalf("rtt %.2f min %.2f jitter %.2f", res.RTTMs, res.MinRTTMs, res.JitterMs)
	}
	if res.Mapped != pc.LocalAddr().String() {
		t.Fatalf("mapped %q, want %q", res.Mapped, pc.LocalAddr())
	}
}

func TestRunSTUNProbeAllLost(t *testing.T) {
	srv := stuntest.Start(t, func(int64) bool { return true }, nil)
	pc, _ := net.ListenPacket("udp", "127.0.0.1:0")
	defer pc.Close()
	addr, _ := net.ResolveUDPAddr("udp", srv.Addr())
	res := smart.RunSTUNProbe(context.Background(), pc, []net.Addr{addr}, 5, 2*time.Millisecond, 100*time.Millisecond)
	if res.Loss != 1 || res.Received != 0 || res.Error == "" {
		t.Fatalf("%+v", res)
	}
	if res := smart.RunSTUNProbe(context.Background(), pc, nil, 5, time.Millisecond, time.Millisecond); res.Loss != 1 || res.Error == "" {
		t.Fatalf("no server: %+v", res)
	}
}
