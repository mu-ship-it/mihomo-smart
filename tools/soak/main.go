// Command soak drives the side-by-side soak of two mihomo instances (Phase 5): our Smart build
// and a stock url-test build, both listening on loopback only. It sends a small, fixed load
// through each instance alternately, records what happened per request (instance, node chosen,
// success, latency, UDP loss and jitter), samples the units' CPU, memory, restarts and traffic,
// enforces a daily traffic budget, watches that Home Assistant on the same host stays
// responsive, and writes a daily Markdown comparison.
//
//	soak run    -config /opt/soak/loadgen.json   long-running load generator
//	soak report -config /opt/soak/loadgen.json   rebuild report.md from the recorded data
//	soak ha     -config /opt/soak/loadgen.json   measure Home Assistant now (and save a baseline with -baseline)
//	soak call   -config ... -inst smart -dur 20s one short UDP call, printed (setup check)
//	soak fetch  -config ... -inst smart          one round of HTTP fetches, printed (setup check)
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	_ "time/tzdata"

	"github.com/metacubex/mihomo/component/smart"
	"github.com/metacubex/mihomo/transport/socks5"
)

type Instance struct {
	Name       string `json:"name"`       // "smart" or "stock"
	Proxy      string `json:"proxy"`      // mixed port, host:port
	Controller string `json:"controller"` // http://127.0.0.1:port
	Secret     string `json:"secret"`
	Group      string `json:"group"`
	Unit       string `json:"unit"`
}

type Config struct {
	Instances     []Instance `json:"instances"`
	Sites         []string   `json:"sites"`
	HTTPEvery     string     `json:"http_every"`      // e.g. "5m"
	HTTPMaxBody   int64      `json:"http_max_body"`   // bytes read per fetch
	STUNEvery     string     `json:"stun_every"`      // e.g. "15m"
	STUNServers   []string   `json:"stun_servers"`    // host:port
	STUNPackets   int        `json:"stun_packets"`    // per short session
	CallTimes     []string   `json:"call_times"`      // "09:00" local
	CallDuration  string     `json:"call_duration"`   // "2m"
	CallKbps      int        `json:"call_kbps"`       // upstream rate of the simulated call
	CallServer    string     `json:"call_server"`     // STUN target of the call
	BudgetMBDay   float64    `json:"budget_mb_day"`   // both instances combined
	TZ            string     `json:"tz"`              // "Asia/Shanghai"
	DataDir       string     `json:"data_dir"`        // jsonl per local day + state
	Report        string     `json:"report"`          // report.md
	DailyLog      string     `json:"daily_log"`       // one line per finished day
	FinalFile     string     `json:"final_file"`      // written once after Days
	Days          int        `json:"days"`            // soak length
	HAURL         string     `json:"ha_url"`          // http://127.0.0.1:8123/
	HAEvery       string     `json:"ha_every"`        // "15m"
	HAFloorMs     float64    `json:"ha_floor_ms"`     // degradation must also exceed this
	StopScript    string     `json:"stop_script"`     // stops the soak (HA guard, completion)
	LoadgenUnit   string     `json:"loadgen_unit"`    // own unit, for the sys sample
	ReportEvery   string     `json:"report_every"`    // "1h"
	SysEvery      string     `json:"sys_every"`       // "1m"
	CallWindowSec int        `json:"call_window_sec"` // per-window stats inside a call
}

func (c *Config) dur(s string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(s); err == nil && d > 0 {
		return d
	}
	return def
}

func loadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c := &Config{}
	if err := json.Unmarshal(b, c); err != nil {
		return nil, err
	}
	if c.HTTPMaxBody <= 0 {
		c.HTTPMaxBody = 16 << 10
	}
	if c.STUNPackets <= 0 {
		c.STUNPackets = 50
	}
	if c.CallKbps <= 0 {
		c.CallKbps = 100
	}
	if c.Days <= 0 {
		c.Days = 7
	}
	if c.CallWindowSec <= 0 {
		c.CallWindowSec = 10
	}
	if c.HAFloorMs <= 0 {
		c.HAFloorMs = 25
	}
	if c.TZ == "" {
		c.TZ = "Asia/Shanghai"
	}
	return c, nil
}

// Rec is one JSONL record. Kinds: http, stun, call, callwin, sys, ha, event.
type Rec struct {
	TS     int64  `json:"ts"` // unix ms
	Kind   string `json:"kind"`
	Inst   string `json:"inst,omitempty"`
	Target string `json:"target,omitempty"`
	Node   string `json:"node,omitempty"`
	OK     bool   `json:"ok"`
	Err    string `json:"err,omitempty"`

	Status  int     `json:"status,omitempty"`
	TTFBMs  float64 `json:"ttfb_ms,omitempty"`
	TotalMs float64 `json:"total_ms,omitempty"`
	Bytes   int64   `json:"bytes,omitempty"`

	Sent      int     `json:"sent,omitempty"`
	Recv      int     `json:"recv,omitempty"`
	Loss      float64 `json:"loss,omitempty"`
	JitterMs  float64 `json:"jitter_ms,omitempty"`
	RTTMs     float64 `json:"rtt_ms,omitempty"`
	RTTP95Ms  float64 `json:"rtt_p95_ms,omitempty"`
	MaxBurst  int     `json:"max_burst,omitempty"`
	WorstLoss float64 `json:"worst_win_loss,omitempty"`
	Window    int     `json:"win,omitempty"`
	CallID    string  `json:"call,omitempty"`

	CPUPct    float64 `json:"cpu_pct,omitempty"`
	MemMB     float64 `json:"mem_mb,omitempty"`
	MemPeakMB float64 `json:"mem_peak_mb,omitempty"`
	Restarts  int     `json:"restarts,omitempty"`
	Active    string  `json:"active,omitempty"`
	GroupNow  string  `json:"group_now,omitempty"`
	IPBytes   int64   `json:"ip_bytes,omitempty"` // unit counter (in+out), cumulative

	ExtMBDay float64 `json:"ext_mb_day,omitempty"` // budget estimate so far today
	HAms     float64 `json:"ha_ms,omitempty"`
	Msg      string  `json:"msg,omitempty"`
}

// ---------------------------------------------------------------- recorder

type recorder struct {
	mu  sync.Mutex
	dir string
	loc *time.Location
}

func (r *recorder) add(rec Rec) {
	if rec.TS == 0 {
		rec.TS = time.Now().UnixMilli()
	}
	b, _ := json.Marshal(rec)
	day := time.UnixMilli(rec.TS).In(r.loc).Format("2006-01-02")
	r.mu.Lock()
	defer r.mu.Unlock()
	f, err := os.OpenFile(filepath.Join(r.dir, day+".jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		fmt.Fprintln(os.Stderr, "record:", err)
		return
	}
	_, _ = f.Write(append(b, '\n'))
	_ = f.Close()
}

// ---------------------------------------------------------------- loopback byte counting

// loopBytes counts what the load generator itself moves over loopback to the instances (and
// their controllers). The units' IP accounting sees that traffic too, so the external traffic
// estimate is unitBytes - loopBytes (an over-estimate by the loopback packet headers).
var loopBytes atomic.Int64

type countConn struct{ net.Conn }

func (c countConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	loopBytes.Add(int64(n))
	return n, err
}

func (c countConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	loopBytes.Add(int64(n))
	return n, err
}

var loopDialer = &net.Dialer{Timeout: 10 * time.Second}

func dialCounted(ctx context.Context, network, addr string) (net.Conn, error) {
	c, err := loopDialer.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	return countConn{c}, nil
}

// ---------------------------------------------------------------- controller queries

var ctlClient = &http.Client{
	Timeout:   5 * time.Second,
	Transport: &http.Transport{Proxy: nil, DialContext: dialCounted, DisableKeepAlives: true},
}

func ctlGet(ctx context.Context, in Instance, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, in.Controller+path, nil)
	if err != nil {
		return err
	}
	if in.Secret != "" {
		req.Header.Set("Authorization", "Bearer "+in.Secret)
	}
	resp, err := ctlClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("controller %s: %s", path, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

type ctlConns struct {
	Connections []struct {
		Metadata struct {
			Network string          `json:"network"`
			SrcPort json.RawMessage `json:"sourcePort"`
			Host    string          `json:"host"`
		} `json:"metadata"`
		Chains []string `json:"chains"`
	} `json:"connections"`
}

// nodeFor finds the node (first element of the chain) of the instance's connection that
// came from our local port.
func nodeFor(ctx context.Context, in Instance, network string, srcPort int) (string, error) {
	var cs ctlConns
	if err := ctlGet(ctx, in, "/connections", &cs); err != nil {
		return "", err
	}
	want := strconv.Itoa(srcPort)
	var nodes []string
	for _, c := range cs.Connections {
		if c.Metadata.Network != network || strings.Trim(string(c.Metadata.SrcPort), `"`) != want {
			continue
		}
		if len(c.Chains) > 0 {
			nodes = append(nodes, c.Chains[0])
		}
	}
	if len(nodes) == 0 {
		return "", errors.New("connection not found")
	}
	sort.Strings(nodes)
	return strings.Join(uniq(nodes), "|"), nil
}

func groupNow(ctx context.Context, in Instance) string {
	var g struct {
		Now string `json:"now"`
	}
	if err := ctlGet(ctx, in, "/proxies/"+url.PathEscape(in.Group), &g); err != nil {
		return ""
	}
	return g.Now
}

func uniq(s []string) []string {
	out := s[:0]
	for i, v := range s {
		if i == 0 || v != s[i-1] {
			out = append(out, v)
		}
	}
	return out
}

// ---------------------------------------------------------------- HTTP fetch

func fetch(ctx context.Context, in Instance, site string, maxBody int64) Rec {
	rec := Rec{Kind: "http", Inst: in.Name, Target: site}
	proxyURL := &url.URL{Scheme: "http", Host: in.Proxy}
	var localPort atomic.Int64
	tr := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := dialCounted(ctx, network, addr)
			if err == nil {
				if ta, ok := c.LocalAddr().(*net.TCPAddr); ok {
					localPort.Store(int64(ta.Port))
				}
			}
			return c, err
		},
		DisableKeepAlives:     true,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 20 * time.Second,
	}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 40 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	start := time.Now()
	var ttfb time.Duration
	trace := &httptrace.ClientTrace{GotFirstResponseByte: func() { ttfb = time.Since(start) }}
	req, _ := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), http.MethodGet, "https://"+site+"/", nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15")
	resp, err := client.Do(req)
	if err != nil {
		rec.TS = start.UnixMilli()
		rec.TotalMs = ms(time.Since(start))
		rec.Err = short(err.Error())
		// the connection is usually gone already; try anyway, it tells which node failed
		if p := localPort.Load(); p != 0 {
			lctx, lcancel := context.WithTimeout(context.Background(), 2*time.Second)
			rec.Node, _ = nodeFor(lctx, in, "tcp", int(p))
			lcancel()
		}
		return rec
	}
	headersAt := time.Since(start)
	lookupStart := time.Now()
	lctx, lcancel := context.WithTimeout(ctx, 3*time.Second)
	rec.Node, _ = nodeFor(lctx, in, "tcp", int(localPort.Load()))
	lcancel()
	lookup := time.Since(lookupStart)
	n, rerr := io.Copy(io.Discard, io.LimitReader(resp.Body, maxBody))
	_ = resp.Body.Close()
	rec.TS = start.UnixMilli()
	rec.Status = resp.StatusCode
	rec.Bytes = n
	if ttfb == 0 {
		ttfb = headersAt
	}
	rec.TTFBMs = ms(ttfb)
	rec.TotalMs = ms(time.Since(start) - lookup)
	rec.OK = true // any HTTP answer from the far side means the path works
	if rerr != nil && !errors.Is(rerr, io.EOF) {
		rec.Err = short("body: " + rerr.Error())
	}
	return rec
}

// ---------------------------------------------------------------- UDP over SOCKS5

type socksUDP struct {
	ctrl  net.Conn
	pc    *net.UDPConn
	relay *net.UDPAddr
}

func dialSocksUDP(ctx context.Context, proxy string) (*socksUDP, error) {
	ctrl, err := dialCounted(ctx, "tcp", proxy)
	if err != nil {
		return nil, err
	}
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		ctrl.Close()
		return nil, err
	}
	_ = ctrl.SetDeadline(time.Now().Add(10 * time.Second))
	bind, err := socks5.ClientHandshake(ctrl, socks5.ParseAddrToSocksAddr(pc.LocalAddr()), socks5.CmdUDPAssociate, nil)
	if err != nil {
		ctrl.Close()
		pc.Close()
		return nil, fmt.Errorf("socks udp associate: %w", err)
	}
	_ = ctrl.SetDeadline(time.Time{})
	relay := bind.UDPAddr()
	if relay == nil {
		ctrl.Close()
		pc.Close()
		return nil, errors.New("socks udp associate: no relay address")
	}
	if relay.IP.IsUnspecified() {
		host, _, _ := net.SplitHostPort(proxy)
		relay.IP = net.ParseIP(host)
	}
	return &socksUDP{ctrl: ctrl, pc: pc, relay: relay}, nil
}

func (s *socksUDP) localPort() int { return s.pc.LocalAddr().(*net.UDPAddr).Port }

func (s *socksUDP) send(dst socks5.Addr, payload []byte) error {
	pkt, err := socks5.EncodeUDPPacket(dst, payload)
	if err != nil {
		return err
	}
	n, err := s.pc.WriteToUDP(pkt, s.relay)
	loopBytes.Add(int64(n))
	return err
}

func (s *socksUDP) recv(buf []byte) ([]byte, error) {
	n, _, err := s.pc.ReadFromUDP(buf)
	if err != nil {
		return nil, err
	}
	loopBytes.Add(int64(n))
	_, payload, err := socks5.DecodeUDPPacket(buf[:n])
	return payload, err
}

func (s *socksUDP) Close() {
	_ = s.pc.Close()
	_ = s.ctrl.Close()
}

// stunRequest builds a binding request, padded with a SOFTWARE attribute (comprehension
// optional, so servers ignore it) up to size bytes so that a session can carry a call-like rate.
func stunRequest(txid [12]byte, size int) []byte {
	b := smart.NewSTUNBindingRequest(txid)
	if size <= len(b)+4 {
		return b
	}
	plen := (size - len(b) - 4) &^ 3
	if plen > 760 {
		plen = 760
	}
	attr := make([]byte, 4+plen)
	attr[0], attr[1] = 0x80, 0x22
	attr[2], attr[3] = byte(plen>>8), byte(plen)
	for i := 4; i < len(attr); i++ {
		attr[i] = 'a'
	}
	b = append(b, attr...)
	b[2], b[3] = byte((4+plen)>>8), byte(4+plen)
	return b
}

type udpWindow struct {
	Sent, Recv int
	Jitter     float64
}

type udpResult struct {
	Sent, Recv      int
	Loss            float64
	RTTMean, RTTP95 float64
	Jitter          float64
	MaxBurst        int
	Windows         []udpWindow
	WorstWindowLoss float64
	Node            string
	Err             string
}

// udpSession sends pps STUN requests per second for dur through the instance, matching every
// reply by transaction id, and returns loss, RTT, jitter (mean |delta RTT| in send order) and
// per-window stats.
func udpSession(ctx context.Context, in Instance, server string, pps int, dur time.Duration, size int, window time.Duration) udpResult {
	var res udpResult
	host, portStr, err := net.SplitHostPort(server)
	if err != nil {
		res.Err = err.Error()
		return res
	}
	port, _ := strconv.Atoi(portStr)
	dst := socks5.ParseAddr(net.JoinHostPort(host, strconv.Itoa(port)))
	if dst == nil {
		res.Err = "bad server " + server
		return res
	}
	s, err := dialSocksUDP(ctx, in.Proxy)
	if err != nil {
		res.Err = short(err.Error())
		return res
	}
	defer s.Close()

	count := int(dur.Seconds() * float64(pps))
	if count < 1 {
		count = 1
	}
	interval := time.Second / time.Duration(pps)
	type pend struct {
		seq  int
		sent time.Time
	}
	var mu sync.Mutex
	inflight := make(map[[12]byte]pend, count)
	sentAt := make([]time.Time, count)
	rtts := make([]float64, count)
	for i := range rtts {
		rtts[i] = -1
	}

	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		buf := make([]byte, 2048)
		for {
			payload, err := s.recv(buf)
			if err != nil {
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
					return
				}
				if errors.Is(err, net.ErrClosed) {
					return
				}
				continue
			}
			now := time.Now()
			txid, _, ok := smart.ParseSTUNBindingResponse(payload)
			if !ok {
				continue
			}
			mu.Lock()
			if p, found := inflight[txid]; found {
				delete(inflight, txid)
				rtts[p.seq] = ms(now.Sub(p.sent))
			}
			mu.Unlock()
		}
	}()

	nodeCh := make(chan string, 1)
	go func() {
		select {
		case <-time.After(1500 * time.Millisecond):
		case <-ctx.Done():
		}
		lctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		n, _ := nodeFor(lctx, in, "udp", s.localPort())
		nodeCh <- n
	}()

	sent := 0
	next := time.Now()
	for i := 0; i < count; i++ {
		var txid [12]byte
		_, _ = rand.Read(txid[:])
		now := time.Now()
		mu.Lock()
		inflight[txid] = pend{seq: i, sent: now}
		mu.Unlock()
		sentAt[i] = now
		if err := s.send(dst, stunRequest(txid, size)); err != nil {
			res.Err = short(err.Error())
			break
		}
		sent++
		next = next.Add(interval)
		if d := time.Until(next); d > 0 {
			select {
			case <-time.After(d):
			case <-ctx.Done():
			}
		}
		if ctx.Err() != nil {
			break
		}
	}
	// stragglers
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		left := len(inflight)
		mu.Unlock()
		if left == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = s.pc.SetReadDeadline(time.Now())
	<-readerDone
	res.Node = <-nodeCh

	mu.Lock()
	defer mu.Unlock()
	res.Sent = sent
	var got []float64
	var jsum float64
	var jn, burst int
	prev := -1.0
	for i := 0; i < sent; i++ {
		r := rtts[i]
		if r < 0 {
			burst++
			if burst > res.MaxBurst {
				res.MaxBurst = burst
			}
			continue
		}
		burst = 0
		got = append(got, r)
		if prev >= 0 {
			jsum += abs(r - prev)
			jn++
		}
		prev = r
	}
	res.Recv = len(got)
	if sent > 0 {
		res.Loss = 1 - float64(res.Recv)/float64(sent)
	}
	if len(got) > 0 {
		res.RTTMean = mean(got)
		res.RTTP95 = pct(got, 0.95)
	}
	if jn > 0 {
		res.Jitter = jsum / float64(jn)
	}
	if res.Recv == 0 && res.Err == "" {
		res.Err = "no reply"
	}
	if window > 0 && sent > 0 {
		start := sentAt[0]
		nw := int(sentAt[sent-1].Sub(start)/window) + 1
		res.Windows = make([]udpWindow, nw)
		prevW := make([]float64, nw)
		jn := make([]int, nw)
		for i := range prevW {
			prevW[i] = -1
		}
		for i := 0; i < sent; i++ {
			w := int(sentAt[i].Sub(start) / window)
			res.Windows[w].Sent++
			if rtts[i] < 0 {
				continue
			}
			res.Windows[w].Recv++
			if prevW[w] >= 0 {
				res.Windows[w].Jitter += abs(rtts[i] - prevW[w])
				jn[w]++
			}
			prevW[w] = rtts[i]
		}
		for w := range res.Windows {
			if jn[w] > 0 {
				res.Windows[w].Jitter /= float64(jn[w])
			}
			if res.Windows[w].Sent > 0 {
				l := 1 - float64(res.Windows[w].Recv)/float64(res.Windows[w].Sent)
				if l > res.WorstWindowLoss {
					res.WorstWindowLoss = l
				}
			}
		}
	}
	return res
}

// ---------------------------------------------------------------- unit sampling

type unitSample struct {
	cpuNs    uint64
	memCur   uint64
	memPeak  uint64
	restarts int
	active   string
	ipIn     uint64
	ipOut    uint64
	ok       bool
}

func sampleUnit(unit string) unitSample {
	out, err := exec.Command("systemctl", "show", unit,
		"-p", "CPUUsageNSec", "-p", "MemoryCurrent", "-p", "MemoryPeak", "-p", "NRestarts",
		"-p", "ActiveState", "-p", "IPIngressBytes", "-p", "IPEgressBytes").Output()
	if err != nil {
		return unitSample{}
	}
	s := unitSample{ok: true}
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	for sc.Scan() {
		k, v, _ := strings.Cut(sc.Text(), "=")
		u, _ := strconv.ParseUint(v, 10, 64)
		if u == ^uint64(0) { // [not set]
			u = 0
		}
		switch k {
		case "CPUUsageNSec":
			s.cpuNs = u
		case "MemoryCurrent":
			s.memCur = u
		case "MemoryPeak":
			s.memPeak = u
		case "NRestarts":
			s.restarts = int(u)
		case "ActiveState":
			s.active = v
		case "IPIngressBytes":
			s.ipIn = u
		case "IPEgressBytes":
			s.ipOut = u
		}
	}
	return s
}

// ---------------------------------------------------------------- HA guard

var haClient = &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}}

// haMeasure returns the median response time of n GETs (ms), or -1 if none succeeded.
func haMeasure(u string, n int) float64 {
	var t []float64
	for i := 0; i < n; i++ {
		start := time.Now()
		resp, err := haClient.Get(u)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			t = append(t, ms(time.Since(start)))
		}
		time.Sleep(200 * time.Millisecond)
	}
	if len(t) == 0 {
		return -1
	}
	return pct(t, 0.5)
}

// ---------------------------------------------------------------- state

type State struct {
	Start      int64                `json:"start"` // unix ms
	HABaseline float64              `json:"ha_baseline_ms"`
	Day        string               `json:"day"`
	DayUnit    int64                `json:"day_unit_bytes"`
	DayLoop    int64                `json:"day_loop_bytes"`
	LastUnit   map[string]int64     `json:"last_unit"`
	Paused     bool                 `json:"paused"` // budget hit for Day
	Stopped    string               `json:"stopped,omitempty"`
	LastCPU    map[string]uint64    `json:"-"`
	LastCPUAt  map[string]time.Time `json:"-"`
	DailyDone  map[string]bool      `json:"daily_done"`
	Final      bool                 `json:"final"`
}

func statePath(c *Config) string { return filepath.Join(c.DataDir, "state.json") }

func loadState(c *Config) *State {
	st := &State{}
	if b, err := os.ReadFile(statePath(c)); err == nil {
		_ = json.Unmarshal(b, st)
	}
	if st.LastUnit == nil {
		st.LastUnit = map[string]int64{}
	}
	if st.DailyDone == nil {
		st.DailyDone = map[string]bool{}
	}
	st.LastCPU = map[string]uint64{}
	st.LastCPUAt = map[string]time.Time{}
	return st
}

func saveState(c *Config, st *State) {
	b, _ := json.MarshalIndent(st, "", "  ")
	tmp := statePath(c) + ".tmp"
	if os.WriteFile(tmp, b, 0o600) == nil {
		_ = os.Rename(tmp, statePath(c))
	}
}

// ---------------------------------------------------------------- runner

type runner struct {
	c   *Config
	loc *time.Location
	rec *recorder
	mu  sync.Mutex // guards st
	st  *State
}

func (r *runner) paused() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.st.Paused || r.st.Stopped != ""
}

func (r *runner) instancesShuffled() []Instance {
	ins := append([]Instance(nil), r.c.Instances...)
	rand.Shuffle(len(ins), func(i, j int) { ins[i], ins[j] = ins[j], ins[i] })
	return ins
}

func (r *runner) httpLoop(ctx context.Context) {
	every := r.c.dur(r.c.HTTPEvery, 5*time.Minute)
	for {
		if !r.paused() {
			sites := append([]string(nil), r.c.Sites...)
			rand.Shuffle(len(sites), func(i, j int) { sites[i], sites[j] = sites[j], sites[i] })
			for _, site := range sites {
				for _, in := range r.instancesShuffled() {
					if ctx.Err() != nil || r.paused() {
						break
					}
					r.rec.add(fetch(ctx, in, site, r.c.HTTPMaxBody))
					sleepCtx(ctx, 2*time.Second)
				}
			}
		}
		if !sleepUntil(ctx, alignNext(time.Now(), every)) {
			return
		}
	}
}

func (r *runner) stunLoop(ctx context.Context) {
	every := r.c.dur(r.c.STUNEvery, 15*time.Minute)
	i := 0
	for {
		if !sleepUntil(ctx, alignNext(time.Now(), every).Add(37*time.Second)) {
			return
		}
		if r.paused() || len(r.c.STUNServers) == 0 {
			continue
		}
		server := r.c.STUNServers[i%len(r.c.STUNServers)]
		i++
		for _, in := range r.instancesShuffled() {
			res := udpSession(ctx, in, server, 50, time.Duration(r.c.STUNPackets)*20*time.Millisecond, 0, 0)
			r.rec.add(Rec{Kind: "stun", Inst: in.Name, Target: server, Node: res.Node, OK: res.Recv > 0, Err: res.Err,
				Sent: res.Sent, Recv: res.Recv, Loss: res.Loss, JitterMs: res.Jitter, RTTMs: res.RTTMean, RTTP95Ms: res.RTTP95, MaxBurst: res.MaxBurst})
			sleepCtx(ctx, 2*time.Second)
		}
	}
}

func (r *runner) nextCall(now time.Time) time.Time {
	local := now.In(r.loc)
	var best time.Time
	for d := 0; d < 2; d++ {
		for _, hm := range r.c.CallTimes {
			t, err := time.ParseInLocation("15:04", hm, r.loc)
			if err != nil {
				continue
			}
			c := time.Date(local.Year(), local.Month(), local.Day()+d, t.Hour(), t.Minute(), 0, 0, r.loc)
			if c.After(now) && (best.IsZero() || c.Before(best)) {
				best = c
			}
		}
	}
	return best
}

func (r *runner) callLoop(ctx context.Context) {
	for {
		next := r.nextCall(time.Now())
		if next.IsZero() || !sleepUntil(ctx, next) {
			return
		}
		if r.paused() {
			continue
		}
		r.runCall(ctx, next.In(r.loc).Format("2006-01-02T15:04"))
	}
}

// runCall runs the simulated call on both instances at the same time, so they see the same
// network conditions.
func (r *runner) runCall(ctx context.Context, id string) {
	dur := r.c.dur(r.c.CallDuration, 2*time.Minute)
	const pps = 50
	size := r.c.CallKbps * 1000 / 8 / pps // bytes per packet
	window := time.Duration(r.c.CallWindowSec) * time.Second
	var wg sync.WaitGroup
	for _, in := range r.c.Instances {
		wg.Add(1)
		go func(in Instance) {
			defer wg.Done()
			res := udpSession(ctx, in, r.c.CallServer, pps, dur, size, window)
			r.rec.add(Rec{Kind: "call", Inst: in.Name, Target: r.c.CallServer, Node: res.Node, OK: res.Recv > 0, Err: res.Err,
				Sent: res.Sent, Recv: res.Recv, Loss: res.Loss, JitterMs: res.Jitter, RTTMs: res.RTTMean, RTTP95Ms: res.RTTP95,
				MaxBurst: res.MaxBurst, WorstLoss: res.WorstWindowLoss, CallID: id})
			for w, win := range res.Windows {
				l := 0.0
				if win.Sent > 0 {
					l = 1 - float64(win.Recv)/float64(win.Sent)
				}
				r.rec.add(Rec{Kind: "callwin", Inst: in.Name, CallID: id, Window: w, Sent: win.Sent, Recv: win.Recv, Loss: l, JitterMs: win.Jitter, OK: win.Recv > 0})
			}
		}(in)
	}
	wg.Wait()
}

// sysLoop samples the units, keeps the daily traffic budget and rolls the day over.
func (r *runner) sysLoop(ctx context.Context) {
	every := r.c.dur(r.c.SysEvery, time.Minute)
	reportEvery := r.c.dur(r.c.ReportEvery, time.Hour)
	lastReport := time.Time{}
	lastLoop := loopBytes.Load()
	for {
		now := time.Now()
		day := now.In(r.loc).Format("2006-01-02")

		r.mu.Lock()
		if r.st.Day != day {
			prevDay := r.st.Day
			r.st.Day, r.st.DayUnit, r.st.DayLoop, r.st.Paused = day, 0, 0, false
			r.mu.Unlock()
			if prevDay != "" {
				r.finishDay(prevDay)
			}
			r.mu.Lock()
		}
		r.mu.Unlock()

		var unitDelta int64
		units := append([]Instance(nil), r.c.Instances...)
		if r.c.LoadgenUnit != "" {
			units = append(units, Instance{Name: "loadgen", Unit: r.c.LoadgenUnit})
		}
		for _, in := range units {
			s := sampleUnit(in.Unit)
			if !s.ok {
				continue
			}
			ip := int64(s.ipIn + s.ipOut)
			r.mu.Lock()
			if in.Name != "loadgen" {
				last, seen := r.st.LastUnit[in.Name]
				switch {
				case !seen:
				case ip >= last:
					unitDelta += ip - last
				default: // counter reset (unit restart)
					unitDelta += ip
				}
				r.st.LastUnit[in.Name] = ip
			}
			cpu := 0.0
			if lt, ok := r.st.LastCPUAt[in.Name]; ok && s.cpuNs >= r.st.LastCPU[in.Name] {
				cpu = float64(s.cpuNs-r.st.LastCPU[in.Name]) / float64(now.Sub(lt).Nanoseconds()) * 100
			}
			r.st.LastCPU[in.Name], r.st.LastCPUAt[in.Name] = s.cpuNs, now
			r.mu.Unlock()
			rec := Rec{TS: now.UnixMilli(), Kind: "sys", Inst: in.Name, OK: s.active == "active", Active: s.active,
				CPUPct: round2(cpu), MemMB: mb(s.memCur), MemPeakMB: mb(s.memPeak), Restarts: s.restarts, IPBytes: ip}
			if in.Group != "" {
				gctx, cancel := context.WithTimeout(ctx, 3*time.Second)
				rec.GroupNow = groupNow(gctx, in)
				cancel()
			}
			r.rec.add(rec)
		}

		lb := loopBytes.Load()
		r.mu.Lock()
		r.st.DayUnit += unitDelta
		r.st.DayLoop += lb - lastLoop
		ext := float64(r.st.DayUnit-r.st.DayLoop) / 1e6
		if ext < 0 {
			ext = 0
		}
		hit := !r.st.Paused && r.c.BudgetMBDay > 0 && ext >= r.c.BudgetMBDay
		if hit {
			r.st.Paused = true
		}
		saveState(r.c, r.st)
		r.mu.Unlock()
		lastLoop = lb
		r.rec.add(Rec{TS: now.UnixMilli(), Kind: "budget", ExtMBDay: round2(ext), OK: !hit})
		if hit {
			r.rec.add(Rec{Kind: "event", Msg: fmt.Sprintf("traffic budget hit (%.1f MB estimated today); load paused until local midnight", ext)})
		}

		if now.Sub(lastReport) >= reportEvery {
			lastReport = now
			_ = writeReport(r.c, r.loc, false)
		}
		r.checkComplete()
		if !sleepCtx(ctx, every) {
			return
		}
	}
}

func (r *runner) finishDay(day string) {
	r.mu.Lock()
	done := r.st.DailyDone[day]
	r.st.DailyDone[day] = true
	saveState(r.c, r.st)
	r.mu.Unlock()
	if done {
		return
	}
	_ = writeReport(r.c, r.loc, false)
	line := dailyLine(r.c, r.loc, day)
	if line != "" && r.c.DailyLog != "" {
		f, err := os.OpenFile(r.c.DailyLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err == nil {
			_, _ = f.WriteString(line + "\n")
			_ = f.Close()
		}
	}
}

func (r *runner) checkComplete() {
	r.mu.Lock()
	start := time.UnixMilli(r.st.Start)
	final := r.st.Final
	r.mu.Unlock()
	if final || time.Since(start) < time.Duration(r.c.Days)*24*time.Hour {
		return
	}
	// close out the last partial day, then the final comparison
	r.finishDay(time.Now().In(r.loc).Format("2006-01-02"))
	_ = writeReport(r.c, r.loc, true)
	r.mu.Lock()
	r.st.Final = true
	saveState(r.c, r.st)
	r.mu.Unlock()
	r.stop(fmt.Sprintf("soak complete after %d days", r.c.Days))
}

func (r *runner) haLoop(ctx context.Context) {
	every := r.c.dur(r.c.HAEvery, 15*time.Minute)
	for {
		if !sleepCtx(ctx, every) {
			return
		}
		r.mu.Lock()
		base := r.st.HABaseline
		r.mu.Unlock()
		m := haMeasure(r.c.HAURL, 5)
		r.rec.add(Rec{Kind: "ha", HAms: round2(m), OK: m >= 0})
		if base <= 0 || m < 0 {
			continue
		}
		if haDegraded(m, base, r.c.HAFloorMs) {
			// confirm with a larger sample before acting
			m2 := haMeasure(r.c.HAURL, 10)
			r.rec.add(Rec{Kind: "ha", HAms: round2(m2), OK: m2 >= 0, Msg: "recheck"})
			if m2 >= 0 && haDegraded(m2, base, r.c.HAFloorMs) {
				r.stop(fmt.Sprintf("Home Assistant degraded: median %.1f ms then %.1f ms vs baseline %.1f ms", m, m2, base))
				return
			}
		}
	}
}

func haDegraded(m, base, floor float64) bool {
	return m > 2*base && m > floor
}

func (r *runner) stop(reason string) {
	r.mu.Lock()
	r.st.Stopped = reason
	saveState(r.c, r.st)
	r.mu.Unlock()
	r.rec.add(Rec{Kind: "event", Msg: "STOP: " + reason})
	_ = writeReport(r.c, r.loc, false)
	if r.c.StopScript != "" {
		out, err := exec.Command(r.c.StopScript, reason).CombinedOutput()
		r.rec.add(Rec{Kind: "event", Msg: short(fmt.Sprintf("stop script: %v %s", err, out))})
	}
}

func run(c *Config) error {
	loc, err := time.LoadLocation(c.TZ)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(c.DataDir, 0o700); err != nil {
		return err
	}
	st := loadState(c)
	if st.Stopped != "" {
		fmt.Println("soak stopped earlier:", st.Stopped)
		return nil
	}
	if st.Start == 0 {
		st.Start = time.Now().UnixMilli()
	}
	if st.HABaseline <= 0 {
		if b, err := os.ReadFile(filepath.Join(c.DataDir, "ha_baseline.json")); err == nil {
			var hb struct {
				Ms float64 `json:"ms"`
			}
			if json.Unmarshal(b, &hb) == nil {
				st.HABaseline = hb.Ms
			}
		}
	}
	saveState(c, st)
	r := &runner{c: c, loc: loc, rec: &recorder{dir: c.DataDir, loc: loc}, st: st}
	r.rec.add(Rec{Kind: "event", Msg: "loadgen started"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for _, f := range []func(context.Context){r.sysLoop, r.haLoop, r.httpLoop, r.stunLoop, r.callLoop} {
		wg.Add(1)
		go func(f func(context.Context)) { defer wg.Done(); f(ctx) }(f)
	}
	// exit once stopped (HA guard or completion); the stop script disables the units
	for {
		time.Sleep(10 * time.Second)
		r.mu.Lock()
		stopped := r.st.Stopped
		r.mu.Unlock()
		if stopped != "" {
			cancel()
			wg.Wait()
			return nil
		}
	}
}

// ---------------------------------------------------------------- helpers

func ms(d time.Duration) float64 { return round2(float64(d) / float64(time.Millisecond)) }
func mb(b uint64) float64        { return round2(float64(b) / (1 << 20)) }
func round2(f float64) float64   { return float64(int64(f*100+0.5)) / 100 }

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

func mean(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	s := 0.0
	for _, x := range v {
		s += x
	}
	return s / float64(len(v))
}

// pct returns the q-quantile (nearest rank) of v; v is not modified.
func pct(v []float64, q float64) float64 {
	if len(v) == 0 {
		return 0
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	i := int(q*float64(len(s))+0.5) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(s) {
		i = len(s) - 1
	}
	return s[i]
}

func short(s string) string {
	if len(s) > 200 {
		return s[:200]
	}
	return s
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func sleepUntil(ctx context.Context, t time.Time) bool { return sleepCtx(ctx, time.Until(t)) }

func alignNext(now time.Time, every time.Duration) time.Time {
	return now.Truncate(every).Add(every)
}

// ---------------------------------------------------------------- CLI

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: soak run|report|ha|call|fetch -config file")
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	cfgPath := fs.String("config", "/opt/soak/loadgen.json", "config file")
	inst := fs.String("inst", "", "instance name (call, fetch)")
	callDur := fs.Duration("dur", 20*time.Second, "call duration (call)")
	baseline := fs.Bool("baseline", false, "save the measurement as the HA baseline (ha)")
	final := fs.Bool("final", false, "write the final comparison too (report)")
	_ = fs.Parse(os.Args[2:])
	c, err := loadConfig(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	loc, err := time.LoadLocation(c.TZ)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	pick := func() Instance {
		for _, in := range c.Instances {
			if in.Name == *inst {
				return in
			}
		}
		fmt.Fprintln(os.Stderr, "unknown -inst")
		os.Exit(2)
		return Instance{}
	}
	switch cmd {
	case "run":
		err = run(c)
	case "report":
		err = writeReport(c, loc, *final)
	case "ha":
		m := haMeasure(c.HAURL, 20)
		fmt.Printf("HA median %.2f ms\n", m)
		if *baseline && m > 0 {
			_ = os.MkdirAll(c.DataDir, 0o700)
			b, _ := json.Marshal(map[string]any{"ms": m, "at": time.Now().In(loc).Format(time.RFC3339)})
			err = os.WriteFile(filepath.Join(c.DataDir, "ha_baseline.json"), b, 0o600)
		}
	case "call":
		in := pick()
		size := c.CallKbps * 1000 / 8 / 50
		res := udpSession(context.Background(), in, c.CallServer, 50, *callDur, size, time.Duration(c.CallWindowSec)*time.Second)
		res.Windows = nil
		b, _ := json.Marshal(res)
		fmt.Println(string(b))
	case "fetch":
		in := pick()
		for _, s := range c.Sites {
			b, _ := json.Marshal(fetch(context.Background(), in, s, c.HTTPMaxBody))
			fmt.Println(string(b))
		}
	default:
		err = fmt.Errorf("unknown command %q", cmd)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
