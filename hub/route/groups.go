package route

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/metacubex/mihomo/adapter/outboundgroup"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/profile/cachefile"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel"

	"github.com/metacubex/chi"
	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

func groupRouter() http.Handler {
	r := chi.NewRouter()
	r.Get("/", getGroups)
	r.Get("/weights", getAllGroupWeights)

	r.Route("/{name}", func(r chi.Router) {
		r.Use(parseProxyName, findProxyByName)
		r.Get("/", getGroup)
		r.Get("/delay", getGroupDelay)
		r.Get("/weights", getGroupWeights)
		r.Post("/probe", postGroupProbe)
		r.Get("/history", getGroupHistory)
	})
	return r
}

func getGroups(w http.ResponseWriter, r *http.Request) {
	var gs []C.Proxy
	for _, p := range tunnel.Proxies() {
		if _, ok := p.Adapter().(outboundgroup.ProxyGroup); ok {
			gs = append(gs, p)
		}
	}
	render.JSON(w, r, render.M{
		"proxies": gs,
	})
}

func getGroup(w http.ResponseWriter, r *http.Request) {
	proxy := r.Context().Value(CtxKeyProxy).(C.Proxy)
	if _, ok := proxy.Adapter().(outboundgroup.ProxyGroup); ok {
		render.JSON(w, r, proxy)
		return
	}
	render.Status(r, http.StatusNotFound)
	render.JSON(w, r, ErrNotFound)
}

func getGroupDelay(w http.ResponseWriter, r *http.Request) {
	proxy := r.Context().Value(CtxKeyProxy).(C.Proxy)
	group, ok := proxy.Adapter().(outboundgroup.ProxyGroup)
	if !ok {
		render.Status(r, http.StatusNotFound)
		render.JSON(w, r, ErrNotFound)
		return
	}

	if selectAble, ok := proxy.Adapter().(outboundgroup.SelectAble); ok && proxy.Type() != C.Selector {
		selectAble.ForceSet("")
		cachefile.Cache().SetSelected(proxy.Name(), "")
	}

	query := r.URL.Query()
	url := query.Get("url")
	timeout, err := strconv.ParseInt(query.Get("timeout"), 10, 32)
	if err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, ErrBadRequest)
		return
	}

	expectedStatus, err := utils.NewUnsignedRanges[uint16](query.Get("expected"))
	if err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, ErrBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), time.Millisecond*time.Duration(timeout))
	defer cancel()

	dm, err := group.URLTest(ctx, url, expectedStatus)
	if err != nil {
		render.Status(r, http.StatusGatewayTimeout)
		render.JSON(w, r, newError(err.Error()))
		return
	}

	render.JSON(w, r, dm)
}

func getGroupWeights(w http.ResponseWriter, r *http.Request) {
	proxy := r.Context().Value(CtxKeyProxy).(C.Proxy)
	smartGroup, ok := proxy.Adapter().(*outboundgroup.Smart)
	if !ok {
		log.Debugln("[Smart] Failed to request weight ranking: Not a Smart group (actual type: %T)", proxy.Adapter())
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, render.M{
			"weights": []smart.NodeRankItem{},
			"error":   "Not a Smart group",
		})
		return
	}

	configName := smartGroup.GetConfigFilename()
	groupName := smartGroup.Name()

	smartStore := cachefile.GetSmartStore()
	if smartStore == nil {
		render.Status(r, http.StatusServiceUnavailable)
		render.JSON(w, r, render.M{
			"weights": []smart.NodeRankItem{},
			"error":   "Smart cache not available",
		})
		return
	}

	wrapper, err := smartStore.GetNodeWeightRankingCache(groupName, configName)
	if err != nil {
		log.Warnln("[Smart] Failed to get weight ranking: %s", err.Error())
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, render.M{
			"weights": []smart.NodeRankItem{},
			"error":   "Failed to get weight ranking: " + err.Error(),
		})
		return
	}
	if len(wrapper.Result) == 0 {
		log.Debugln("Policy group %s has no weight data", groupName)
		render.JSON(w, r, render.M{
			"weights": []smart.NodeRankItem{},
			"message": "No weight data available for the specified group",
		})
		return
	}

	render.JSON(w, r, render.M{
		"weights": wrapper.Result,
	})
}

func getAllGroupWeights(w http.ResponseWriter, r *http.Request) {
	smartStore := cachefile.GetSmartStore()
	if smartStore == nil {
		render.Status(r, http.StatusServiceUnavailable)
		render.JSON(w, r, render.M{
			"weights": map[string][]smart.NodeRankItem{},
			"errors":  map[string]string{},
			"error":   "Smart cache not available",
		})
		return
	}

	result := make(map[string][]smart.NodeRankItem)
	errorsMap := make(map[string]string)

	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		sem = make(chan struct{}, 5)
	)

	for _, p := range tunnel.Proxies() {
		sg, ok := p.Adapter().(*outboundgroup.Smart)
		if !ok {
			continue
		}

		configName := sg.GetConfigFilename()
		groupName := sg.Name()

		wg.Add(1)
		sem <- struct{}{}
		go func(groupName, configName string) {
			defer wg.Done()
			defer func() { <-sem }()

			wrapper, err := smartStore.GetNodeWeightRankingCache(groupName, configName)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				log.Warnln("[Smart] Failed to get weight ranking for group %s: %s", groupName, err.Error())
				errorsMap[groupName] = err.Error()
				return
			}
			result[groupName] = wrapper.Result
		}(groupName, configName)
	}

	wg.Wait()

	if len(result) == 0 && len(errorsMap) == 0 {
		render.JSON(w, r, render.M{
			"weights": map[string][]smart.NodeRankItem{},
			"message": "No Smart groups or no weight data available",
		})
		return
	}

	render.JSON(w, r, render.M{
		"weights": result,
		"errors":  errorsMap,
	})
}

// smartGroupOf returns the request's group as a Smart group, writing a 400 when it is not one.
func smartGroupOf(w http.ResponseWriter, r *http.Request) (*outboundgroup.Smart, bool) {
	proxy := r.Context().Value(CtxKeyProxy).(C.Proxy)
	sg, ok := proxy.Adapter().(*outboundgroup.Smart)
	if !ok {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("not a Smart group"))
		return nil, false
	}
	return sg, true
}

// probeRequest is the body of POST /group/{name}/probe.
type probeRequest struct {
	Target     string   `json:"target"`
	Duration   string   `json:"duration,omitempty"`    // Go duration, e.g. "5s"
	DurationMs int64    `json:"duration_ms,omitempty"` // alternative to duration
	Servers    []string `json:"servers,omitempty"`     // STUN host:port; default: the group's list
	Nodes      []string `json:"nodes,omitempty"`       // default: every UDP capable node
}

// postGroupProbe runs a STUN probe through a Smart group's nodes and writes the results into
// the target's UDP weights ("pre-meeting warm-up").
func postGroupProbe(w http.ResponseWriter, r *http.Request) {
	sg, ok := smartGroupOf(w, r)
	if !ok {
		return
	}
	var req probeRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&req); err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, ErrBadRequest)
		return
	}
	var duration time.Duration
	switch {
	case req.Duration != "":
		d, err := time.ParseDuration(req.Duration)
		if err != nil {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, newError("invalid duration"))
			return
		}
		duration = d
	case req.DurationMs != 0:
		duration = time.Duration(req.DurationMs) * time.Millisecond
	}

	// the probe outlives the reply wait of its last packet, so give it room beyond its length
	budget := duration
	if budget == 0 {
		budget = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(r.Context(), budget+15*time.Second)
	defer cancel()

	report, err := sg.ProbeUDP(ctx, outboundgroup.UDPProbeRequest{
		Target:   req.Target,
		Duration: duration,
		Servers:  req.Servers,
		Nodes:    req.Nodes,
	})
	switch {
	case err == nil:
		render.JSON(w, r, report)
	case errors.Is(err, outboundgroup.ErrProbeBusy):
		render.Status(r, http.StatusConflict)
		render.JSON(w, r, newError(err.Error()))
	case errors.Is(err, outboundgroup.ErrProbeNoTarget), errors.Is(err, outboundgroup.ErrProbeDuration), errors.Is(err, outboundgroup.ErrProbeNoServers):
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError(err.Error()))
	default:
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError(err.Error()))
	}
}

// getGroupHistory returns a Smart group's recorded outcomes for one target, aggregated per
// hour of the week. Query: target (required), network (tcp|udp), node, tz (IANA name; default
// the process's local zone).
func getGroupHistory(w http.ResponseWriter, r *http.Request) {
	sg, ok := smartGroupOf(w, r)
	if !ok {
		return
	}
	query := r.URL.Query()
	target := query.Get("target")
	if target == "" {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("target is required"))
		return
	}
	network := query.Get("network")
	if network != "" && network != smart.HistoryNetworkTCP && network != smart.HistoryNetworkUDP {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("network must be tcp or udp"))
		return
	}
	loc := time.Local
	if tz := query.Get("tz"); tz != "" {
		l, err := time.LoadLocation(tz)
		if err != nil {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, newError("unknown time zone"))
			return
		}
		loc = l
	}
	h := sg.History()
	if h == nil {
		render.Status(r, http.StatusServiceUnavailable)
		render.JSON(w, r, newError("history not available"))
		return
	}
	render.JSON(w, r, h.Report(sg.ProbeTargetKey(target), network, query.Get("node"), loc))
}
