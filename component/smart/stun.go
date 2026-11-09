package smart

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"math"
	"net"
	"net/netip"
	"sync"
	"time"
)

// Active UDP probe (Phase 4): RFC 5389 STUN binding requests sent through one node to a list
// of public STUN servers. Every request carries a random transaction id, so replies are
// matched one by one, which gives a real loss figure and per-packet RTTs (and so jitter),
// unlike the live estimate that has to infer loss from reply ratios.

const (
	stunHeaderSize   = 20
	stunMagicCookie  = 0x2112A442
	stunBindingReq   = 0x0001
	stunBindingResp  = 0x0101
	stunAttrXORMappd = 0x0020
	stunAttrMapped   = 0x0001

	// DefaultUDPProbeInterval spaces the requests of one probe (50 packets a second, the
	// rate of an audio stream).
	DefaultUDPProbeInterval = 20 * time.Millisecond
	// UDPProbeReplyWait is how long a probe waits for stragglers after its last request.
	UDPProbeReplyWait = 1 * time.Second
	// MaxUDPProbePackets bounds one probe.
	MaxUDPProbePackets = 1500
)

// DefaultSTUNServers is used when a group sets no udp-probe-servers.
var DefaultSTUNServers = []string{"stun.l.google.com:19302", "stun.cloudflare.com:3478"}

// UDPProbeResult is one node's probe outcome.
type UDPProbeResult struct {
	Sent      int     `json:"sent"`
	Received  int     `json:"received"`
	Loss      float64 `json:"loss"`
	RTTMs     float64 `json:"rtt_ms"`
	MinRTTMs  float64 `json:"min_rtt_ms"`
	JitterMs  float64 `json:"jitter_ms"`
	Mapped    string  `json:"mapped,omitempty"` // public address the STUN server saw (the exit)
	Error     string  `json:"error,omitempty"`
	StartedAt int64   `json:"started_at"`
}

// NewSTUNBindingRequest builds a binding request with no attributes.
func NewSTUNBindingRequest(txid [12]byte) []byte {
	b := make([]byte, stunHeaderSize)
	binary.BigEndian.PutUint16(b[0:2], stunBindingReq)
	binary.BigEndian.PutUint16(b[2:4], 0)
	binary.BigEndian.PutUint32(b[4:8], stunMagicCookie)
	copy(b[8:20], txid[:])
	return b
}

// NewSTUNBindingResponse builds a binding success response carrying an XOR-MAPPED-ADDRESS
// (used by the probe tests' fake server; a real server sends the same shape).
func NewSTUNBindingResponse(txid [12]byte, mapped netip.AddrPort) []byte {
	var attr []byte
	if mapped.IsValid() {
		ip := mapped.Addr().Unmap()
		var raw []byte
		family := byte(0x01)
		if ip.Is4() {
			a := ip.As4()
			raw = a[:]
		} else {
			a := ip.As16()
			raw = a[:]
			family = 0x02
		}
		cookieAndTx := make([]byte, 16)
		binary.BigEndian.PutUint32(cookieAndTx[0:4], stunMagicCookie)
		copy(cookieAndTx[4:], txid[:])
		val := make([]byte, 4+len(raw))
		val[1] = family
		binary.BigEndian.PutUint16(val[2:4], mapped.Port()^uint16(stunMagicCookie>>16))
		for i := range raw {
			val[4+i] = raw[i] ^ cookieAndTx[i]
		}
		attr = make([]byte, 4+len(val))
		binary.BigEndian.PutUint16(attr[0:2], stunAttrXORMappd)
		binary.BigEndian.PutUint16(attr[2:4], uint16(len(val)))
		copy(attr[4:], val)
	}
	b := make([]byte, stunHeaderSize+len(attr))
	binary.BigEndian.PutUint16(b[0:2], stunBindingResp)
	binary.BigEndian.PutUint16(b[2:4], uint16(len(attr)))
	binary.BigEndian.PutUint32(b[4:8], stunMagicCookie)
	copy(b[8:20], txid[:])
	copy(b[20:], attr)
	return b
}

// ParseSTUNBindingRequest returns the transaction id of a binding request.
func ParseSTUNBindingRequest(b []byte) (txid [12]byte, ok bool) {
	if len(b) < stunHeaderSize || binary.BigEndian.Uint16(b[0:2]) != stunBindingReq ||
		binary.BigEndian.Uint32(b[4:8]) != stunMagicCookie {
		return txid, false
	}
	copy(txid[:], b[8:20])
	return txid, true
}

// ParseSTUNBindingResponse validates a binding success response and returns its transaction
// id and, when present, the mapped address.
func ParseSTUNBindingResponse(b []byte) (txid [12]byte, mapped netip.AddrPort, ok bool) {
	if len(b) < stunHeaderSize {
		return txid, mapped, false
	}
	if binary.BigEndian.Uint16(b[0:2]) != stunBindingResp || binary.BigEndian.Uint32(b[4:8]) != stunMagicCookie {
		return txid, mapped, false
	}
	length := int(binary.BigEndian.Uint16(b[2:4]))
	if length%4 != 0 || stunHeaderSize+length > len(b) {
		return txid, mapped, false
	}
	copy(txid[:], b[8:20])

	attrs := b[stunHeaderSize : stunHeaderSize+length]
	for len(attrs) >= 4 {
		typ := binary.BigEndian.Uint16(attrs[0:2])
		alen := int(binary.BigEndian.Uint16(attrs[2:4]))
		if 4+alen > len(attrs) {
			break
		}
		val := attrs[4 : 4+alen]
		switch typ {
		case stunAttrXORMappd:
			if ap, ok := parseSTUNAddress(val, true, b[4:20]); ok {
				mapped = ap
			}
		case stunAttrMapped:
			if !mapped.IsValid() {
				if ap, ok := parseSTUNAddress(val, false, nil); ok {
					mapped = ap
				}
			}
		}
		padded := (alen + 3) &^ 3
		if 4+padded > len(attrs) {
			break
		}
		attrs = attrs[4+padded:]
	}
	return txid, mapped, true
}

func parseSTUNAddress(v []byte, xor bool, cookieAndTx []byte) (netip.AddrPort, bool) {
	if len(v) < 4 {
		return netip.AddrPort{}, false
	}
	family := v[1]
	port := binary.BigEndian.Uint16(v[2:4])
	if xor {
		port ^= uint16(stunMagicCookie >> 16)
	}
	var ip []byte
	switch family {
	case 0x01:
		if len(v) < 8 {
			return netip.AddrPort{}, false
		}
		ip = append([]byte(nil), v[4:8]...)
	case 0x02:
		if len(v) < 20 {
			return netip.AddrPort{}, false
		}
		ip = append([]byte(nil), v[4:20]...)
	default:
		return netip.AddrPort{}, false
	}
	if xor {
		for i := range ip {
			ip[i] ^= cookieAndTx[i]
		}
	}
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return netip.AddrPort{}, false
	}
	return netip.AddrPortFrom(addr.Unmap(), port), true
}

// RunSTUNProbe sends count binding requests over pc, spaced by interval and spread round
// robin over servers, then waits up to replyWait for the last replies. pc is closed by the
// caller. RTT jitter is the mean absolute difference of consecutive RTTs (in send order),
// which is what a jitter buffer has to absorb.
func RunSTUNProbe(ctx context.Context, pc net.PacketConn, servers []net.Addr, count int, interval, replyWait time.Duration) UDPProbeResult {
	res := UDPProbeResult{StartedAt: time.Now().Unix()}
	if len(servers) == 0 {
		res.Error = "no STUN server"
		res.Loss = 1
		return res
	}
	if count <= 0 {
		count = 1
	}
	if count > MaxUDPProbePackets {
		count = MaxUDPProbePackets
	}

	type pending struct {
		seq  int
		sent time.Time
	}
	var (
		mu       sync.Mutex
		inflight = make(map[[12]byte]pending, count)
		rtts     = make([]float64, count) // by send order, -1 = lost
		mapped   netip.AddrPort
	)
	for i := range rtts {
		rtts[i] = -1
	}

	// the reader stops when the caller closes pc; it is not waited for, since not every
	// proxied packet conn honours read deadlines
	go func() {
		buf := make([]byte, 1500)
		for {
			n, _, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			now := time.Now()
			txid, ap, ok := ParseSTUNBindingResponse(buf[:n])
			if !ok {
				continue
			}
			mu.Lock()
			if p, found := inflight[txid]; found {
				delete(inflight, txid)
				rtts[p.seq] = float64(now.Sub(p.sent)) / float64(time.Millisecond)
				if ap.IsValid() {
					mapped = ap
				}
			}
			mu.Unlock()
		}
	}()

	sent := 0
	var sendErr error
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
sendLoop:
	for i := 0; i < count; i++ {
		var txid [12]byte
		_, _ = rand.Read(txid[:])
		mu.Lock()
		inflight[txid] = pending{seq: i, sent: time.Now()}
		mu.Unlock()
		if _, err := pc.WriteTo(NewSTUNBindingRequest(txid), servers[i%len(servers)]); err != nil {
			mu.Lock()
			delete(inflight, txid)
			mu.Unlock()
			sendErr = err
			break
		}
		sent++
		if i == count-1 {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			break sendLoop
		}
	}

	// wait for stragglers, but stop early once every request is answered
	deadline := time.NewTimer(replyWait)
	poll := time.NewTicker(10 * time.Millisecond)
wait:
	for {
		mu.Lock()
		left := len(inflight)
		mu.Unlock()
		if left == 0 {
			break
		}
		select {
		case <-deadline.C:
			break wait
		case <-poll.C:
		case <-ctx.Done():
			break wait
		}
	}
	deadline.Stop()
	poll.Stop()
	_ = pc.SetReadDeadline(time.Now())

	mu.Lock()
	defer mu.Unlock()
	// late replies no longer count
	for k := range inflight {
		delete(inflight, k)
	}
	res.Sent = sent
	if mapped.IsValid() {
		res.Mapped = mapped.String()
	}
	var sum, jitterSum float64
	var jitterN int
	minRTT := math.Inf(1)
	prev := -1.0
	for i := 0; i < sent; i++ {
		r := rtts[i]
		if r < 0 {
			continue
		}
		res.Received++
		sum += r
		if r < minRTT {
			minRTT = r
		}
		if prev >= 0 {
			jitterSum += math.Abs(r - prev)
			jitterN++
		}
		prev = r
	}
	if sent > 0 {
		res.Loss = 1 - float64(res.Received)/float64(sent)
	} else {
		res.Loss = 1
	}
	if res.Received > 0 {
		res.RTTMs = sum / float64(res.Received)
		res.MinRTTMs = minRTT
	}
	if jitterN > 0 {
		res.JitterMs = jitterSum / float64(jitterN)
	}
	if sendErr != nil {
		res.Error = sendErr.Error()
	} else if res.Received == 0 {
		res.Error = errSTUNNoReply.Error()
	}
	return res
}

var errSTUNNoReply = errors.New("no STUN reply")
