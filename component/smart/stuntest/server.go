// Package stuntest runs a local STUN binding server with scripted loss and delay, for tests
// of the Smart group's UDP probe.
package stuntest

import (
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/smart"
)

// Server answers STUN binding requests on 127.0.0.1.
type Server struct {
	conn     net.PacketConn
	Requests atomic.Int64
	Replies  atomic.Int64
	wg       sync.WaitGroup

	// Drop reports whether the n-th request (0-based) gets no answer.
	Drop func(n int64) bool
	// Delay returns how long the n-th answer is held back.
	Delay func(n int64) time.Duration
}

// Start listens on a random local port; the server stops at the end of the test.
func Start(t testing.TB, drop func(int64) bool, delay func(int64) time.Duration) *Server {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{conn: pc, Drop: drop, Delay: delay}
	s.wg.Add(1)
	go s.serve()
	t.Cleanup(func() {
		_ = pc.Close()
		s.wg.Wait()
	})
	return s
}

// Addr is the server's host:port.
func (s *Server) Addr() string { return s.conn.LocalAddr().String() }

func (s *Server) serve() {
	defer s.wg.Done()
	buf := make([]byte, 1500)
	for {
		n, from, err := s.conn.ReadFrom(buf)
		if err != nil {
			return
		}
		txid, ok := smart.ParseSTUNBindingRequest(buf[:n])
		if !ok {
			continue
		}
		seq := s.Requests.Add(1) - 1
		if s.Drop != nil && s.Drop(seq) {
			continue
		}
		var mapped netip.AddrPort
		if ua, ok := from.(*net.UDPAddr); ok {
			mapped = ua.AddrPort()
		}
		resp := smart.NewSTUNBindingResponse(txid, mapped)
		var delay time.Duration
		if s.Delay != nil {
			delay = s.Delay(seq)
		}
		send := func() {
			if _, err := s.conn.WriteTo(resp, from); err == nil {
				s.Replies.Add(1)
			}
		}
		if delay <= 0 {
			send()
		} else {
			time.AfterFunc(delay, send)
		}
	}
}
