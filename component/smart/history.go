package smart

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Timestamped outcome history (Phase 4).
//
// Every outcome the group records (a closed connection, a live UDP window, a probe) is kept
// with its timestamp and destination key, so a later phase can fit a seasonal model (for
// example Holt-Winters with a weekly season) and predict which node will be good at 9:00 on
// a Monday. Outcomes are stored in one chunk per target and UTC hour:
//
//	smart/history/<config>/<group>/<target>/h<unix hour, 12 digits>
//
// A chunk keeps up to HistoryMaxEventsPerChunk raw events plus exact per node and network
// aggregates, so a busy hour stays bounded on disk while its totals stay exact. The current
// hour lives in memory and is written through the store's write queue on Flush; the
// retention cap (age and chunk count per group) is applied by Prune.

const (
	KeyTypeHistory = "history"

	DefaultHistoryRetention   = 8 * 7 * 24 * time.Hour // two weekly seasons
	DefaultHistoryMaxChunks   = 8192                   // per group
	HistoryMaxEventsPerChunk  = 64
	historyMaxPendingChunks   = 1024
	historyHourPrefix         = "h"
	HistorySourceConnection   = "conn"
	HistorySourceLiveUDP      = "live"
	HistorySourceProbe        = "probe"
	HistoryNetworkTCP         = "tcp"
	HistoryNetworkUDP         = "udp"
	historyAggKeySeparator    = "|"
	historyTimestampFieldSize = 12
)

// HistoryEvent is one recorded outcome.
type HistoryEvent struct {
	TS        int64   `json:"t"`
	Node      string  `json:"n"`
	Network   string  `json:"k"`
	OK        bool    `json:"ok"`
	LatencyMs int64   `json:"l,omitempty"`
	Loss      float64 `json:"x,omitempty"`
	JitterMs  float64 `json:"j,omitempty"`
	Source    string  `json:"s,omitempty"`
}

// HistoryAgg holds exact sums for one node and network in one hour.
type HistoryAgg struct {
	Count      int64   `json:"c"`
	Success    int64   `json:"ok"`
	Failure    int64   `json:"f"`
	LatencySum float64 `json:"ls,omitempty"`
	LatencyN   int64   `json:"ln,omitempty"`
	LossSum    float64 `json:"xs,omitempty"`
	LossN      int64   `json:"xn,omitempty"`
	JitterSum  float64 `json:"js,omitempty"`
	JitterN    int64   `json:"jn,omitempty"`
}

func (a *HistoryAgg) add(ev HistoryEvent, hasLoss, hasJitter bool) {
	a.Count++
	if ev.OK {
		a.Success++
	} else {
		a.Failure++
	}
	if ev.LatencyMs > 0 {
		a.LatencySum += float64(ev.LatencyMs)
		a.LatencyN++
	}
	if hasLoss {
		a.LossSum += ev.Loss
		a.LossN++
	}
	if hasJitter {
		a.JitterSum += ev.JitterMs
		a.JitterN++
	}
}

func (a *HistoryAgg) merge(b *HistoryAgg) {
	a.Count += b.Count
	a.Success += b.Success
	a.Failure += b.Failure
	a.LatencySum += b.LatencySum
	a.LatencyN += b.LatencyN
	a.LossSum += b.LossSum
	a.LossN += b.LossN
	a.JitterSum += b.JitterSum
	a.JitterN += b.JitterN
}

// HistoryChunk is one target's outcomes in one UTC hour.
type HistoryChunk struct {
	Hour    int64                  `json:"h"`
	Events  []HistoryEvent         `json:"e,omitempty"`
	Dropped int64                  `json:"d,omitempty"`
	Agg     map[string]*HistoryAgg `json:"a"`
}

// HistoryRecordOptions carries the optional measurements of an event: loss and jitter are
// only aggregated when they were measured (a TCP close has neither).
type HistoryRecordOptions struct {
	HasLoss   bool
	HasJitter bool
}

// HistoryRecorder buffers one group's history and writes it through the store.
type HistoryRecorder struct {
	mu        sync.Mutex
	store     *Store
	group     string
	config    string
	pending   map[string]*HistoryChunk // key: target + "\x00" + hour
	dirty     map[string]bool
	retention time.Duration
	maxChunks int
	now       func() time.Time
}

func NewHistoryRecorder(store *Store, group, config string, retention time.Duration, maxChunks int) *HistoryRecorder {
	if retention <= 0 {
		retention = DefaultHistoryRetention
	}
	if maxChunks <= 0 {
		maxChunks = DefaultHistoryMaxChunks
	}
	return &HistoryRecorder{
		store:     store,
		group:     group,
		config:    config,
		pending:   make(map[string]*HistoryChunk),
		dirty:     make(map[string]bool),
		retention: retention,
		maxChunks: maxChunks,
		now:       time.Now,
	}
}

// SetClock replaces the recorder's clock (tests).
func (h *HistoryRecorder) SetClock(now func() time.Time) {
	h.mu.Lock()
	h.now = now
	h.mu.Unlock()
}

func historyHourStamp(hour int64) string {
	return historyHourPrefix + fmt.Sprintf("%0*d", historyTimestampFieldSize, hour)
}

func (h *HistoryRecorder) chunkKey(target string, hour int64) string {
	return FormatDBKey(KeyTypeHistory, h.config, h.group, target, historyHourStamp(hour))
}

func (h *HistoryRecorder) targetPrefix(target string) string {
	return FormatDBKey(KeyTypeHistory, h.config, h.group, target) + "/"
}

func (h *HistoryRecorder) groupPrefix() string {
	return FormatDBKey(KeyTypeHistory, h.config, h.group) + "/"
}

// Record adds one outcome. A zero TS is stamped with the recorder's clock.
func (h *HistoryRecorder) Record(target string, ev HistoryEvent, opt HistoryRecordOptions) {
	if h == nil || target == "" || ev.Node == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if ev.TS == 0 {
		ev.TS = h.now().Unix()
	}
	hour := ev.TS / 3600
	mk := target + "\x00" + strconv.FormatInt(hour, 10)
	c := h.pending[mk]
	if c == nil {
		if len(h.pending) >= historyMaxPendingChunks {
			// everything pending goes to the write queue, then memory starts over; a chunk
			// touched again is read back from the queue or the database
			h.flushLocked()
			h.pending = make(map[string]*HistoryChunk)
		}
		c = h.loadChunk(target, hour)
		h.pending[mk] = c
	}
	if len(c.Events) < HistoryMaxEventsPerChunk {
		c.Events = append(c.Events, ev)
	} else {
		c.Dropped++
	}
	ak := ev.Node + historyAggKeySeparator + ev.Network
	a := c.Agg[ak]
	if a == nil {
		a = &HistoryAgg{}
		c.Agg[ak] = a
	}
	a.add(ev, opt.HasLoss, opt.HasJitter)
	h.dirty[mk] = true
}

func (h *HistoryRecorder) loadChunk(target string, hour int64) *HistoryChunk {
	key := h.chunkKey(target, hour)
	var data []byte
	if op, ok := opQueue.get(key); ok && op.Type == OpSaveHistory {
		data = op.Data
	} else if h.store != nil {
		data, _ = h.store.DBViewGetItem(key)
	}
	if len(data) > 0 {
		var c HistoryChunk
		if json.Unmarshal(data, &c) == nil && c.Hour == hour {
			if c.Agg == nil {
				c.Agg = make(map[string]*HistoryAgg)
			}
			return &c
		}
	}
	return &HistoryChunk{Hour: hour, Agg: make(map[string]*HistoryAgg)}
}

// Flush queues every changed chunk for writing and forgets the chunks of past hours.
func (h *HistoryRecorder) Flush() {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.flushLocked()
	current := h.now().Unix() / 3600
	for mk, c := range h.pending {
		if c.Hour < current {
			delete(h.pending, mk)
		}
	}
}

func (h *HistoryRecorder) flushLocked() {
	if len(h.dirty) == 0 || h.store == nil {
		return
	}
	ops := make([]StoreOperation, 0, len(h.dirty))
	for mk := range h.dirty {
		c := h.pending[mk]
		if c == nil {
			continue
		}
		data, err := json.Marshal(c)
		if err != nil {
			continue
		}
		target, _, _ := strings.Cut(mk, "\x00")
		ops = append(ops, StoreOperation{
			Type:   OpSaveHistory,
			Group:  h.group,
			Config: h.config,
			Target: target,
			Node:   historyHourStamp(c.Hour),
			Data:   data,
		})
	}
	h.dirty = make(map[string]bool)
	h.store.AppendToGlobalQueue(ops...)
}

// chunks returns every chunk under prefix: written ones, queued ones and pending ones, the
// newest copy of each.
func (h *HistoryRecorder) chunks(prefix string) map[string]*HistoryChunk {
	out := make(map[string]*HistoryChunk)
	decode := func(key string, data []byte) {
		if !strings.HasPrefix(key, prefix) {
			return
		}
		if i := strings.LastIndexByte(key, '/'); !strings.HasPrefix(key[i+1:], historyHourPrefix) {
			return
		}
		var c HistoryChunk
		if json.Unmarshal(data, &c) == nil {
			out[key] = &c
		}
	}
	if h.store != nil {
		if stored, err := h.store.DBViewPrefixScan(prefix, -1, false); err == nil {
			for k, v := range stored {
				decode(k, v)
			}
		}
	}
	for _, op := range opQueue.snapshot() {
		if op.Type != OpSaveHistory || op.Group != h.group || op.Config != h.config {
			continue
		}
		key := formatOperationKey(&op)
		if strings.HasPrefix(key, prefix) {
			decode(key, op.Data)
		}
	}
	h.mu.Lock()
	for mk, c := range h.pending {
		target, _, _ := strings.Cut(mk, "\x00")
		key := h.chunkKey(target, c.Hour)
		if strings.HasPrefix(key, prefix) {
			data, err := json.Marshal(c)
			if err == nil {
				decode(key, data)
			}
		}
	}
	h.mu.Unlock()
	return out
}

// TargetChunks returns one target's chunks keyed by UTC hour (tests and later model fitting).
func (h *HistoryRecorder) TargetChunks(target string) map[int64]*HistoryChunk {
	prefix := h.targetPrefix(target)
	out := make(map[int64]*HistoryChunk)
	for key, c := range h.chunks(prefix) {
		if strings.Contains(strings.TrimPrefix(key, prefix), "/") {
			continue // a deeper target that shares this prefix
		}
		out[c.Hour] = c
	}
	return out
}

// Prune deletes chunks older than the retention and, past the chunk cap, the oldest hours.
// It returns how many chunks were deleted.
func (h *HistoryRecorder) Prune() int {
	if h == nil || h.store == nil {
		return 0
	}
	h.Flush()
	FlushQueueForHistory(h.store)

	prefix := h.groupPrefix()
	stored, err := h.store.DBViewPrefixScan(prefix, -1, false)
	if err != nil {
		return 0
	}
	type entry struct {
		key  string
		hour int64
	}
	entries := make([]entry, 0, len(stored))
	for key := range stored {
		i := strings.LastIndexByte(key, '/')
		if i < 0 || !strings.HasPrefix(key[i+1:], historyHourPrefix) {
			continue
		}
		hour, err := strconv.ParseInt(key[i+1+len(historyHourPrefix):], 10, 64)
		if err != nil {
			continue
		}
		entries = append(entries, entry{key, hour})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].hour != entries[j].hour {
			return entries[i].hour < entries[j].hour
		}
		return entries[i].key < entries[j].key
	})

	h.mu.Lock()
	cutoff := (h.now().Add(-h.retention).Unix()) / 3600
	maxChunks := h.maxChunks
	h.mu.Unlock()

	var drop []string
	keep := len(entries)
	for _, e := range entries {
		if e.hour < cutoff || keep > maxChunks {
			drop = append(drop, e.key)
			keep--
		}
	}
	if len(drop) > 0 {
		_ = h.store.DBBatchDeletePrefix(drop, true)
	}
	return len(drop)
}

// FlushQueueForHistory writes the queue now, so a prune sees what was recorded.
func FlushQueueForHistory(store *Store) {
	if store != nil {
		store.FlushQueue(true)
	}
}

// HistoryNodeAgg is one node and network in one hour-of-week bucket.
type HistoryNodeAgg struct {
	Node         string   `json:"node"`
	Network      string   `json:"network"`
	Count        int64    `json:"count"`
	Success      int64    `json:"success"`
	Failure      int64    `json:"failure"`
	SuccessRate  float64  `json:"success_rate"`
	AvgLatencyMs *float64 `json:"avg_latency_ms,omitempty"`
	AvgLoss      *float64 `json:"avg_loss,omitempty"`
	AvgJitterMs  *float64 `json:"avg_jitter_ms,omitempty"`
}

// HistoryBucket is one hour of the week: 0 is Monday 00:00-00:59 in the report's time zone.
type HistoryBucket struct {
	HourOfWeek int              `json:"hour_of_week"`
	Weekday    string           `json:"weekday"`
	Hour       int              `json:"hour"`
	Weeks      int              `json:"weeks"` // distinct calendar hours that contributed
	Nodes      []HistoryNodeAgg `json:"nodes"`
}

// HistoryReport is the per-hour-of-week view of one target.
type HistoryReport struct {
	Group            string          `json:"group"`
	Target           string          `json:"target"`
	TimeZone         string          `json:"timezone"`
	UTCOffsetMinutes int             `json:"utc_offset_minutes"`
	From             int64           `json:"from,omitempty"`
	To               int64           `json:"to,omitempty"`
	HoursCovered     int             `json:"hours_covered"`
	Events           int64           `json:"events"`
	Buckets          []HistoryBucket `json:"buckets"`
}

// HourOfWeek maps a time to 0..167 with Monday 00:00 as 0, in t's location.
func HourOfWeek(t time.Time) int {
	return ((int(t.Weekday())+6)%7)*24 + t.Hour()
}

// Report aggregates one target's history per hour of the week in loc. network and node
// filter when not empty.
func (h *HistoryRecorder) Report(target, network, node string, loc *time.Location) HistoryReport {
	if loc == nil {
		loc = time.Local
	}
	now := h.now().In(loc)
	zone, offset := now.Zone()
	rep := HistoryReport{
		Group:            h.group,
		Target:           target,
		TimeZone:         loc.String(),
		UTCOffsetMinutes: offset / 60,
		Buckets:          []HistoryBucket{},
	}
	if rep.TimeZone == "Local" {
		rep.TimeZone = zone
	}

	type bucketAcc struct {
		hours map[int64]bool
		nodes map[string]*HistoryAgg
	}
	acc := make(map[int]*bucketAcc)
	for hour, c := range h.TargetChunks(target) {
		used := false
		for ak, a := range c.Agg {
			n, netw, _ := strings.Cut(ak, historyAggKeySeparator)
			if (network != "" && netw != network) || (node != "" && n != node) {
				continue
			}
			t := time.Unix(hour*3600, 0).In(loc)
			how := HourOfWeek(t)
			b := acc[how]
			if b == nil {
				b = &bucketAcc{hours: make(map[int64]bool), nodes: make(map[string]*HistoryAgg)}
				acc[how] = b
			}
			b.hours[hour] = true
			sum := b.nodes[ak]
			if sum == nil {
				sum = &HistoryAgg{}
				b.nodes[ak] = sum
			}
			sum.merge(a)
			rep.Events += a.Count
			used = true
		}
		if used {
			rep.HoursCovered++
			ts := hour * 3600
			if rep.From == 0 || ts < rep.From {
				rep.From = ts
			}
			if end := ts + 3599; end > rep.To {
				rep.To = end
			}
		}
	}

	weekdays := [7]string{"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}
	hows := make([]int, 0, len(acc))
	for how := range acc {
		hows = append(hows, how)
	}
	sort.Ints(hows)
	for _, how := range hows {
		b := acc[how]
		bucket := HistoryBucket{
			HourOfWeek: how,
			Weekday:    weekdays[how/24],
			Hour:       how % 24,
			Weeks:      len(b.hours),
		}
		keys := make([]string, 0, len(b.nodes))
		for k := range b.nodes {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			a := b.nodes[k]
			n, netw, _ := strings.Cut(k, historyAggKeySeparator)
			na := HistoryNodeAgg{Node: n, Network: netw, Count: a.Count, Success: a.Success, Failure: a.Failure}
			if a.Count > 0 {
				na.SuccessRate = float64(a.Success) / float64(a.Count)
			}
			if a.LatencyN > 0 {
				v := a.LatencySum / float64(a.LatencyN)
				na.AvgLatencyMs = &v
			}
			if a.LossN > 0 {
				v := a.LossSum / float64(a.LossN)
				na.AvgLoss = &v
			}
			if a.JitterN > 0 {
				v := a.JitterSum / float64(a.JitterN)
				na.AvgJitterMs = &v
			}
			bucket.Nodes = append(bucket.Nodes, na)
		}
		rep.Buckets = append(rep.Buckets, bucket)
	}
	return rep
}

// HistoryTargets lists the targets with recorded history in this group.
func (h *HistoryRecorder) HistoryTargets() []string {
	prefix := h.groupPrefix()
	seen := make(map[string]bool)
	for key := range h.chunks(prefix) {
		rest := strings.TrimPrefix(key, prefix)
		if i := strings.LastIndexByte(rest, '/'); i > 0 {
			seen[rest[:i]] = true
		}
	}
	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}
