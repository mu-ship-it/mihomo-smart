package route

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/adapter/outboundgroup"
	"github.com/metacubex/mihomo/adapter/provider"
	"github.com/metacubex/mihomo/component/profile/cachefile"
	"github.com/metacubex/mihomo/component/smart"
	"github.com/metacubex/mihomo/component/smart/stuntest"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/tunnel"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
)

var (
	smartAPISeq sync.Mutex
	smartAPIRun atomic.Int64
)

// setupSmartAPI registers a Smart group "smart" with two DIRECT nodes (d1, d2) whose STUN
// probes go to a local server, plus a plain node "d3" that is not a group.
func setupSmartAPI(t *testing.T, delay func(int64) time.Duration) (http.Handler, *outboundgroup.Smart, *stuntest.Server) {
	t.Helper()
	smartAPISeq.Lock()
	t.Cleanup(smartAPISeq.Unlock)

	srv := stuntest.Start(t, nil, delay)
	d1 := adapter.NewProxy(outbound.NewDirectWithOption(outbound.DirectOption{Name: "d1"}))
	d2 := adapter.NewProxy(outbound.NewDirectWithOption(outbound.DirectOption{Name: "d2"}))
	d3 := adapter.NewProxy(outbound.NewDirectWithOption(outbound.DirectOption{Name: "d3"}))
	nodes := []C.Proxy{d1, d2}
	pd, err := provider.NewCompatibleProvider("smart-api", nodes, provider.NewHealthCheck(nodes, "", 0, 0, true, nil))
	if err != nil {
		t.Fatal(err)
	}
	sg, err := outboundgroup.NewSmart(outboundgroup.GroupCommonOption{Name: fmt.Sprintf("smart-api-%s-%d", t.Name(), smartAPIRun.Add(1))},
		outboundgroup.SmartOption{UDPProbeServers: []string{srv.Addr()}},
		adapter.NewProxy(outbound.NewCompatible()), []P.ProxyProvider{pd})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sg.Close() })
	tunnel.UpdateProxies(map[string]C.Proxy{
		"smart": adapter.NewProxy(sg),
		"d1":    d1, "d2": d2, "d3": d3,
	}, map[string]P.ProxyProvider{})
	return groupRouter(), sg, srv
}

func do(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, path, nil)
	} else {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestGroupProbeAPI(t *testing.T) {
	h, sg, srv := setupSmartAPI(t, nil)

	rec := do(h, "POST", "/smart/probe", `{"target":"zoom.us","duration":"300ms"}`)
	if rec.Code != 200 {
		t.Fatalf("probe: %d %s", rec.Code, rec.Body)
	}
	var rep outboundgroup.UDPProbeReport
	if err := json.Unmarshal(rec.Body.Bytes(), &rep); err != nil {
		t.Fatal(err)
	}
	if rep.Group != sg.Name() || rep.Target != sg.ProbeTargetKey("zoom.us") || rep.DurationMs != 300 ||
		len(rep.Servers) != 1 || rep.Servers[0] != srv.Addr() || len(rep.Results) != 2 {
		t.Fatalf("report: %+v", rep)
	}
	for _, r := range rep.Results {
		if r.Sent != 15 || r.Received != 15 || r.Loss != 0 || r.WeightAfter <= 0 || r.Mapped == "" || r.Error != "" {
			t.Fatalf("node %s: %+v", r.Node, r)
		}
	}
	// the result is in the weights the selector reads
	names, _, err := cachefileStore(t).GetBestProxyForTarget(sg.Name(), sg.GetConfigFilename(), rep.Target, true)
	if err != nil || len(names) != 2 {
		t.Fatalf("UDP ranking after the probe: %v %v", names, err)
	}

	// duration_ms and an explicit node list
	rec = do(h, "POST", "/smart/probe", `{"target":"zoom.us","duration_ms":200,"nodes":["d2"]}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"node":"d2"`) || strings.Contains(rec.Body.String(), `"node":"d1"`) {
		t.Fatalf("node filter: %d %s", rec.Code, rec.Body)
	}
}

func TestGroupProbeAPIErrors(t *testing.T) {
	h, _, _ := setupSmartAPI(t, nil)
	cases := []struct {
		name, path, body string
		code             int
	}{
		{"unknown group", "/nope/probe", `{"target":"zoom.us"}`, 404},
		{"not a Smart group", "/d3/probe", `{"target":"zoom.us"}`, 400},
		{"invalid json", "/smart/probe", `{"target":`, 400},
		{"missing target", "/smart/probe", `{"duration":"1s"}`, 400},
		{"bad duration", "/smart/probe", `{"target":"zoom.us","duration":"soon"}`, 400},
		{"too long", "/smart/probe", `{"target":"zoom.us","duration":"31s"}`, 400},
		{"too short", "/smart/probe", `{"target":"zoom.us","duration_ms":50}`, 400},
		{"unusable server", "/smart/probe", `{"target":"zoom.us","servers":["nope"]}`, 400},
	}
	for _, c := range cases {
		rec := do(h, "POST", c.path, c.body)
		if rec.Code != c.code {
			t.Errorf("%s: got %d %s, want %d", c.name, rec.Code, rec.Body, c.code)
		}
		if c.code == 400 && !strings.Contains(rec.Body.String(), `"message"`) {
			t.Errorf("%s: error body %s", c.name, rec.Body)
		}
	}
	if rec := do(h, "GET", "/smart/probe", ""); rec.Code != 405 {
		t.Errorf("GET on probe: %d", rec.Code)
	}
}

func TestGroupProbeAPIOneAtATime(t *testing.T) {
	h, _, _ := setupSmartAPI(t, func(int64) time.Duration { return 20 * time.Millisecond })
	first := make(chan int, 1)
	go func() {
		first <- do(h, "POST", "/smart/probe", `{"target":"zoom.us","duration":"600ms"}`).Code
	}()
	time.Sleep(150 * time.Millisecond)
	if rec := do(h, "POST", "/smart/probe", `{"target":"zoom.us","duration":"200ms"}`); rec.Code != 409 {
		t.Fatalf("second probe while one runs: %d %s", rec.Code, rec.Body)
	}
	if code := <-first; code != 200 {
		t.Fatalf("first probe: %d", code)
	}
}

func TestGroupHistoryAPI(t *testing.T) {
	h, sg, _ := setupSmartAPI(t, nil)
	before := time.Now().UTC()
	if rec := do(h, "POST", "/smart/probe", `{"target":"zoom.us","duration":"200ms"}`); rec.Code != 200 {
		t.Fatalf("probe: %d %s", rec.Code, rec.Body)
	}

	rec := do(h, "GET", "/smart/history?target=zoom.us&tz=UTC", "")
	if rec.Code != 200 {
		t.Fatalf("history: %d %s", rec.Code, rec.Body)
	}
	var rep smart.HistoryReport
	if err := json.Unmarshal(rec.Body.Bytes(), &rep); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if rep.Target != sg.ProbeTargetKey("zoom.us") || rep.TimeZone != "UTC" || rep.Events != 2 || len(rep.Buckets) != 1 {
		t.Fatalf("report: %+v", rep)
	}
	b := rep.Buckets[0]
	// the probe may straddle an hour boundary
	if (b.HourOfWeek != smart.HourOfWeek(now) && b.HourOfWeek != smart.HourOfWeek(before)) || b.Weeks != 1 || len(b.Nodes) != 2 {
		t.Fatalf("bucket: %+v (now how %d)", b, smart.HourOfWeek(now))
	}
	for _, n := range b.Nodes {
		if n.Network != "udp" || n.Count != 1 || n.Success != 1 || n.AvgLoss == nil || *n.AvgLoss != 0 || n.AvgLatencyMs == nil {
			t.Fatalf("node aggregate: %+v", n)
		}
	}

	// filters and the raw body shape
	if rec := do(h, "GET", "/smart/history?target=zoom.us&network=tcp", ""); rec.Code != 200 || !strings.Contains(rec.Body.String(), `"buckets":[]`) {
		t.Fatalf("tcp filter: %d %s", rec.Code, rec.Body)
	}
	if rec := do(h, "GET", "/smart/history?target=zoom.us&node=d1&tz=UTC", ""); rec.Code != 200 || strings.Contains(rec.Body.String(), `"node":"d2"`) {
		t.Fatalf("node filter: %d %s", rec.Code, rec.Body)
	}

	for _, c := range []struct {
		path string
		code int
	}{
		{"/smart/history", 400},
		{"/smart/history?target=zoom.us&network=icmp", 400},
		{"/smart/history?target=zoom.us&tz=Mars/Olympus", 400},
		{"/d3/history?target=zoom.us", 400},
		{"/nope/history?target=zoom.us", 404},
	} {
		if rec := do(h, "GET", c.path, ""); rec.Code != c.code {
			t.Errorf("%s: got %d %s, want %d", c.path, rec.Code, rec.Body, c.code)
		}
	}
}

func cachefileStore(t *testing.T) *smart.Store {
	t.Helper()
	st := cachefile.GetSmartStore()
	if st == nil {
		t.Fatal("no Smart store")
	}
	return st
}
