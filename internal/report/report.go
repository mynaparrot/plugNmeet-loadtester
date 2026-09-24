// Package report is a minimal mutex-guarded latency/counter collector with
// percentile calculation for the plugNmeet load tester.
package report

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mynaparrot/plugnmeet-loadtester/internal/resmon"
)

// Collector aggregates latency records, counters, error kinds and a
// per-event message census for the whole run.
type Collector struct {
	mu sync.Mutex

	lat map[string][]time.Duration // key: phase name

	joinedN    atomic.Int64
	roomJoined map[string]int64 // joined count per room (guarded by mu)
	roomLive   map[string]int64 // live joined users per room (guarded by mu)

	// mediaLive: live LiveKit connections per room (same pattern as roomLive).
	mediaLive map[string]int64

	// churn accounting (guarded by mu)
	churnLeaves int
	churnJoins  int
	churnPop    int
	churnPopMin int
	churnPopMax int
	churnPopSet bool

	pingOK int
	pingMi int
	msgRx  atomic.Uint64
	coreRx atomic.Uint64
	total  int

	snd   map[string]uint64
	rcv   map[string]uint64
	errs  map[string]int
	recon atomic.Uint64
	start time.Time

	// delivery-RTT registry (guarded by mu): send timestamp + expected fan-out.
	rtt map[deliveryKey]*deliverySend

	// fan-out completeness accounting per delivery kind (guarded by mu)
	fanArrived  map[string]int // total receipts across sent msgs
	fanExpected map[string]int // total expected recipients across sends

	// lastSweep tracks the last inline TTL sweep of rtt (guarded by mu)
	lastSweep time.Time

	// real-user-sync (mixed-population honest accounting, guarded by mu):
	// foreignSends counts synthetic→foreign targeted sends by kind. Foreign
	// targets are OUTSIDE this run's fan/RTT registry (they never report
	// receipts to this process's collector); req/response interactions use a
	// pending map resolved by a correlated foreign→synthetic response.
	foreignSends  map[string]int
	foreignRecv   map[string]int             // response-kind -> correlated recv from foreign peers
	foreignRTT    map[string][]time.Duration // request-kind -> direct RTT samples
	foreignPend   map[foreignPendKey]time.Time
	foreignPendAt time.Time // last inline TTL sweep of foreignPend

	// pubTracks: one record per published synthetic media track (filled by RecordRecv).
	pubTracks []*PubTrackSend

	// firstSeen: event/payload type -> arrival subject (set once)
	firstSeen map[string]string

	// resources: host CPU sampler result (nil → section/JSON field omitted)
	resources *resmon.Result

	// memRes: process+machine memory sampler result for the always-emitted
	// "resmon" block (set by the engine at run end; before that nil → block
	// is zero-filled at Snapshot time anyway).
	memRes *resmon.MemResult

	// loadtester-vs-server clock offset (HTTP Date header); omitted from JSON
	// when unmeasurable — never fabricated.
	clockOffsetMs int64
	clockSet      bool

	// media: resolved per-cohort media shape (set by main for every run;
	// mode "none" = no LiveKit connections).
	media *MediaInfo
}

// Registry lifecycle: RecordSend registers a pending send; RecordRecv tallies
// each receipt and forgets the entry once received >= expected (RTT latched on
// the first receipt, fan count applied before the delete). deliveryRTTCap is
// the last-resort bound for never-completing entries (real-browser receipts),
// which the inline TTL sweeper in RecordSend purges; their expected stays in
// the denominator.
const (
	deliveryRTTCap     = 20000
	deliverySweepEvery = 5 * time.Second
	deliveryRTTTTL     = 60 * time.Second
)

type deliveryKey struct{ kind, id string }

// foreignPendKey: one pending direct-RTT correlation — pendKey discriminates
// the request kind ("wbSyncReq"/"notepadSyncReq"); id is the foreign
// responder/recipient ("lt-<other RunID>-" users or real-user ids).
type foreignPendKey struct{ kind, id string }

const foreignPendTTL = 25 * time.Second // 2× the renamer's 2-5s backoff + margins

type deliverySend struct {
	at       time.Time
	expected int
	received int
	latched  bool // latency recorded once on the first receipt
	pt       *PubTrackSend
}

// PubTrackSend is one per-send pubTrack record for the JSON summary
// (populated only for published synthetic media tracks). SentAtMs/Expected
// are set at publish; Receipts/FirstReceiptMs are updated by RecordRecv
// (FirstReceiptMs stays -1 if no receipt was ever recorded).
type PubTrackSend struct {
	Sid            string `json:"sid"`
	Kind           string `json:"kind"` // "audio" | "video"
	User           string `json:"user"`
	SentAtMs       int64  `json:"sent_at"`
	Expected       int    `json:"expected"`
	Receipts       int    `json:"receipts"`
	FirstReceiptMs int64  `json:"first_receipt_ms"`
}

func New() *Collector {
	return &Collector{
		lat:          make(map[string][]time.Duration),
		roomJoined:   make(map[string]int64),
		roomLive:     make(map[string]int64),
		mediaLive:    make(map[string]int64),
		snd:          make(map[string]uint64),
		rcv:          make(map[string]uint64),
		errs:         make(map[string]int),
		rtt:          make(map[deliveryKey]*deliverySend),
		fanArrived:   make(map[string]int),
		fanExpected:  make(map[string]int),
		foreignSends: make(map[string]int),
		foreignRecv:  make(map[string]int),
		foreignRTT:   make(map[string][]time.Duration),
		foreignPend:  make(map[foreignPendKey]time.Time),
		firstSeen:    make(map[string]string),
		start:        time.Now(),
	}
}

// Record adds a latency sample under the given phase key.
func (c *Collector) Record(phase string, d time.Duration) {
	c.mu.Lock()
	c.lat[phase] = append(c.lat[phase], d)
	c.mu.Unlock()
}

func (c *Collector) AddError(kind string) { c.mu.Lock(); c.errs[kind]++; c.mu.Unlock() }
func (c *Collector) AddReconnect()        { c.recon.Add(1) }
func (c *Collector) Sent(ev string)       { c.mu.Lock(); c.snd[ev]++; c.mu.Unlock() }
func (c *Collector) Received(ev string)   { c.mu.Lock(); c.rcv[ev]++; c.mu.Unlock() }
func (c *Collector) AddJoined(room string) {
	c.mu.Lock()
	c.roomJoined[room]++
	c.roomLive[room]++
	c.mu.Unlock()
	c.joinedN.Add(1)
}

// RoomLive returns the live (currently connected) joined count per room.
func (c *Collector) RoomLive(room string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return int(c.roomLive[room])
}

// MediaLiveInc/Dec adjust the live LiveKit connection count of one room
// (a ChurnLeave-style floor guard keeps the gauge sane).
func (c *Collector) MediaLiveInc(room string) {
	c.mu.Lock()
	c.mediaLive[room]++
	c.mu.Unlock()
}

func (c *Collector) MediaLiveDec(room string) {
	c.mu.Lock()
	if c.mediaLive[room] > 0 {
		c.mediaLive[room]--
	}
	c.mu.Unlock()
}

// MediaLive returns the live LiveKit connection count for one room.
func (c *Collector) MediaLive(room string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return int(c.mediaLive[room])
}

// RoomJoined returns the joined-so-far count for one room.
func (c *Collector) RoomJoined(room string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return int(c.roomJoined[room])
}

// LiveUsers returns the total live joined-user count across all rooms (the
// process-memory sampler consumes this per tick; derived, not re-counted).
func (c *Collector) LiveUsers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	var n int64
	for _, v := range c.roomLive {
		n += v
	}
	return int(n)
}

// SetMemRes stores the memory-sampler result for the "resmon" summary block.
func (c *Collector) SetMemRes(r *resmon.MemResult) {
	c.mu.Lock()
	c.memRes = r
	c.mu.Unlock()
}

// ChurnLeave counts a churn depature and decrements the population gauge.
// The leave signals must be registered BEFORE this call (spec: expected is
// computed while the user is still counted in the room).
func (c *Collector) ChurnLeave(room string) {
	c.mu.Lock()
	c.churnLeaves++
	c.churnPop--
	if c.roomLive[room] > 0 {
		c.roomLive[room]-- // floor at 0 (paranoia guard; live > 0 in practice)
	}
	if !c.churnPopSet || c.churnPop < c.churnPopMin {
		c.churnPopMin = c.churnPop
	}
	c.mu.Unlock()
}

// ChurnJoin counts a churn replacement join and increments the pop gauge.
func (c *Collector) ChurnJoin() {
	c.mu.Lock()
	c.churnJoins++
	c.churnPop++
	if !c.churnPopSet || c.churnPop > c.churnPopMax {
		c.churnPopMax = c.churnPop
	}
	c.mu.Unlock()
}

// SetChurnPop seeds the population gauge once, after the initial spawn.
func (c *Collector) SetChurnPop(n int) {
	c.mu.Lock()
	c.churnPop = n
	c.churnPopMin = n
	c.churnPopMax = n
	c.churnPopSet = true
	c.mu.Unlock()
}

// SetMedia records the resolved per-cohort media shape (called unconditionally by main).
func (c *Collector) SetMedia(mi MediaInfo) {
	c.mu.Lock()
	c.media = &mi
	c.mu.Unlock()
}

// ChurnStats returns the churn counters + population gauge.
func (c *Collector) ChurnStats() (leaves, joins, popMin, popMax, pop int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.churnLeaves, c.churnJoins, c.churnPopMin, c.churnPopMax, c.churnPop
}
func (c *Collector) SetTotal(n int)     { c.mu.Lock(); c.total = n; c.mu.Unlock() }
func (c *Collector) MsgRxAdd(n uint64)  { c.msgRx.Add(n) }
func (c *Collector) CoreRxAdd(n uint64) { c.coreRx.Add(n) }
func (c *Collector) MsgRx() uint64      { return c.msgRx.Load() }
func (c *Collector) Joined() int        { return int(c.joinedN.Load()) }
func (c *Collector) Total() int         { c.mu.Lock(); defer c.mu.Unlock(); return c.total }
func (c *Collector) Pings() (ok, missed int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pingOK, c.pingMi
}

// Errors returns the number of distinct error kinds recorded so far.
func (c *Collector) Errors() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.errs)
}

// RecordSend registers a delivery-type send so later RecordRecvs compute RTT
// and fan-out completeness. Ids beyond the cap are dropped.
func (c *Collector) RecordSend(kind, id string, at time.Time, expected int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Inline TTL sweeper (see the registry-lifecycle comment above the consts).
	if at.Sub(c.lastSweep) > deliverySweepEvery {
		cut := at.Add(-deliveryRTTTTL)
		for k, v := range c.rtt {
			if v.at.Before(cut) {
				delete(c.rtt, k)
			}
		}
		c.lastSweep = at
	}
	if expected <= 0 {
		// Nothing to wait for: registering would leave a permanently
		// incomplete entry (expected contribution is 0 anyway).
		return
	}
	if len(c.rtt) >= deliveryRTTCap {
		// Hard cap reached: drop the entry WITHOUT counting expected, so the
		// fan denominator excludes sends whose receipts can never match
		// (honest accounting under pressure).
		return
	}
	c.fanExpected[kind] += expected
	c.rtt[deliveryKey{kind, id}] = &deliverySend{at: at, expected: expected}
}

// RecordPubTrackSend registers one published synthetic media track: a JSON
// summary record plus a delivery-registry entry for receipts/first-receipt.
func (c *Collector) RecordPubTrackSend(sid, kind, user string, at time.Time, expected int) {
	c.mu.Lock()
	rec := &PubTrackSend{
		Sid:            sid,
		Kind:           kind,
		User:           user,
		SentAtMs:       at.UnixMilli(),
		Expected:       expected,
		FirstReceiptMs: -1,
	}
	c.pubTracks = append(c.pubTracks, rec)
	c.mu.Unlock()
	c.RecordSend("pubTrack", sid, at, expected)
	c.mu.Lock()
	if e, ok := c.rtt[deliveryKey{"pubTrack", sid}]; ok {
		e.pt = rec
	}
	c.mu.Unlock()
}

// RecordRecv tallies a receipt: first receipt records the RTT sample, later
// ones accumulate the fan-out sum; the entry is deleted at expected receipts.
func (c *Collector) RecordRecv(kind, id string, at time.Time) {
	k := deliveryKey{kind, id}
	c.mu.Lock()
	if rec, ok := c.rtt[k]; ok {
		rec.received++
		if rec.expected > 0 {
			c.fanArrived[kind]++
		}
		if rec.pt != nil {
			rec.pt.Receipts = rec.received
		}
		if !rec.latched {
			rec.latched = true
			sample := at.Sub(rec.at)
			c.lat["delivery."+kind] = append(c.lat["delivery."+kind], sample)
			if rec.pt != nil {
				rec.pt.FirstReceiptMs = sample.Milliseconds()
			}
		}
		if rec.received >= rec.expected {
			delete(c.rtt, k)
		}
	}
	c.mu.Unlock()
}

// ForeignSend records a targeted synthetic→foreign send for the real-user-sync
// ledger (never the fan/RTT registry — see the Collector comment). With
// pendKey != "": registers/RENEWS the pending direct-RTT entry
// (pendKey, targetID), replacing any prior entry (no double-counting).
// Inline TTL sweeper for foreignPend (same pattern as the delivery registry's
// sweeper): belt-and-braces purge of pendings the foreign peer never answered
// (real users unreachable / foreign users left); expires after foreignPendTTL.
func (c *Collector) RecordForeignSend(kind, to, id string, at time.Time, pendKey string) {
	c.mu.Lock()
	c.foreignSends[kind]++
	if at.Sub(c.foreignPendAt) > deliverySweepEvery {
		cut := at.Add(-foreignPendTTL)
		for k, v := range c.foreignPend {
			if v.Before(cut) {
				delete(c.foreignPend, k)
			}
		}
		c.foreignPendAt = at
	}
	if pendKey != "" {
		c.foreignPend[foreignPendKey{pendKey, to}] = at
	}
	c.mu.Unlock()
}

// RecordForeignRecv records a foreign→synthetic response (kind = the response
// kind for the recv counter; pendKey = the correlated request's pendKey). When
// a pending entry exists it is resolved to a direct send→response RTT sample
// under the pendKey (also used as the RTT series key).
func (c *Collector) RecordForeignRecv(kind, pendKey, from string, at time.Time) {
	c.mu.Lock()
	c.foreignRecv[kind]++
	lat, has := c.foreignPend[foreignPendKey{pendKey, from}]
	delete(c.foreignPend, foreignPendKey{pendKey, from})
	c.mu.Unlock()
	if has {
		if d := at.Sub(lat); d >= 0 {
			c.mu.Lock()
			c.foreignRTT[pendKey] = append(c.foreignRTT[pendKey], d)
			c.mu.Unlock()
		}
	}
}

// FirstSeen records the arrival subject for an event/payload type once.
func (c *Collector) FirstSeen(key, subject string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.firstSeen[key]; !ok {
		c.firstSeen[key] = subject
	}
}

func (c *Collector) PingOK() { c.mu.Lock(); c.pingOK++; c.mu.Unlock() }
func (c *Collector) PingMissed(n int) {
	c.mu.Lock()
	c.pingMi += n
	c.mu.Unlock()
}

// SetClockOffsetMs stores the measured loadtester-vs-server clock offset.
// Never fabricated: only called with a real measurement.
func (c *Collector) SetClockOffsetMs(ms int64) {
	c.mu.Lock()
	c.clockOffsetMs = ms
	c.clockSet = true
	c.mu.Unlock()
}

// ClockOffsetMs returns the measured offset (local clock minus server clock)
// in milliseconds and whether it could be measured.
func (c *Collector) ClockOffsetMs() (ms int64, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clockOffsetMs, c.clockSet
}

// SetResources stores the host resource-sampler result for the summary.
// Passing nil (or an unsupported result) omits the section everywhere.
func (c *Collector) SetResources(r *resmon.Result) {
	c.mu.Lock()
	c.resources = r
	c.mu.Unlock()
}

// Snapshot is the JSON-serialisable final summary.
type Snapshot struct {
	RunDurationSec float64          `json:"run_duration_sec"`
	TotalUsers     int              `json:"total_users"`
	Joined         int              `json:"joined"`
	RoomsJoined    map[string]int64 `json:"rooms_joined,omitempty"`
	RoomLiveJoined map[string]int64 `json:"rooms_live,omitempty"`

	MediaLiveJoined map[string]int64 `json:"rooms_media_live,omitempty"`

	ChurnLeaves   int                       `json:"churn_leaves,omitempty"`
	ChurnJoins    int                       `json:"churn_joins,omitempty"`
	ChurnPopMin   int                       `json:"churn_pop_min,omitempty"`
	ChurnPopMax   int                       `json:"churn_pop_max,omitempty"`
	ChurnPop      int                       `json:"churn_pop,omitempty"`
	Errors        map[string]int            `json:"errors"`
	Reconnects    uint64                    `json:"reconnects"`
	PingsOK       int                       `json:"pings_ok"`
	PingsMissed   int                       `json:"pings_missed"`
	MessagesRx    uint64                    `json:"messages_rx"`
	CoreRx        uint64                    `json:"core_subject_messages_rx"`
	LatencyMs     map[string]*Phase         `json:"latencies_ms"`
	CensusSent    map[string]uint64         `json:"census_sent"`
	CensusRecv    map[string]uint64         `json:"census_received"`
	Delivery      map[string]*DeliveryPhase `json:"delivery_rtt"`
	PubTrackSends []*PubTrackSend           `json:"pubtrack_sends,omitempty"`
	FirstSeenSubj map[string]string         `json:"first_seen_subjects"`
	Resources     *resmon.Result            `json:"resources,omitempty"`
	// ClockOffsetMs: local clock minus server clock; omitted when unmeasurable.
	ClockOffsetMs *int64 `json:"clock_offset_ms,omitempty"`
	// Media describes the per-cohort media shape of the run.
	Media *MediaInfo `json:"media,omitempty"`
	// RealUserSync: mixed-population honest accounting (always present; zero
	// maps when no foreign peers were targeted).
	RealUserSync *RealUserSync `json:"real_user_sync"`
	// Resmon: process RSS + machine memory availability at a fixed cadence,
	// always emitted (zero-filled when the sampler produced no samples).
	Resmon *resmon.MemResult `json:"resmon"`
}

// RealUserSync is the real-user-sync summary block: synthetic→foreign send
// counts, foreign→synthetic response counts and direct-RTT percentiles
// (request-send → correlated response-receive, keyed by the request kind).
type RealUserSync struct {
	Sends map[string]int    `json:"sends_to_foreign"`
	Recv  map[string]int    `json:"recv_from_foreign"`
	RTT   map[string]*Phase `json:"direct_rtt_ms"`
}

// MediaInfo records the resolved per-cohort media counts of the run (all
// PER ROOM). Subscribers counts every user connected to LiveKit.
type MediaInfo struct {
	Mode            string `json:"mode"`
	VideoPublishers int    `json:"video_publishers"`
	AudioPublishers int    `json:"audio_publishers"`
	Subscribers     int    `json:"subscribers"`
	UsersPerRoom    int    `json:"users_per_room"`
}

// DeliveryPhase = one delivery-kind row in the delivery table.
type DeliveryPhase struct {
	P50      float64 `json:"p50"`
	P95      float64 `json:"p95"`
	Max      float64 `json:"max"`
	Count    int     `json:"count"`
	FanPct   float64 `json:"fan_completeness_pct"`
	Sent     int     `json:"sends"`    // sends with expected>0
	Arrived  int     `json:"arrivals"` // summed receipts
	Expected int     `json:"expected"` // summed recipients
}

// Phase holds millisecond percentiles.
type Phase struct {
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	Max   float64 `json:"max"`
	Count int     `json:"count"`
}

// Snapshot builds a consistent copy.
func (c *Collector) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := Snapshot{
		RunDurationSec: time.Since(c.start).Seconds(),
		TotalUsers:     c.total,
		Joined:         int(c.joinedN.Load()),
		Errors:         copyInt(c.errs),
	}
	if len(c.roomJoined) > 1 {
		s.RoomsJoined = make(map[string]int64, len(c.roomJoined))
		s.RoomLiveJoined = make(map[string]int64, len(c.roomLive))
		for k, v := range c.roomJoined {
			s.RoomsJoined[k] = v
		}
		for k, v := range c.roomLive {
			s.RoomLiveJoined[k] = v
		}
	}
	if len(c.mediaLive) > 0 {
		s.MediaLiveJoined = make(map[string]int64, len(c.mediaLive))
		for k, v := range c.mediaLive {
			s.MediaLiveJoined[k] = v
		}
	}
	if c.churnLeaves > 0 {
		s.ChurnLeaves, s.ChurnJoins = c.churnLeaves, c.churnJoins
		s.ChurnPopMin, s.ChurnPopMax, s.ChurnPop = c.churnPopMin, c.churnPopMax, c.churnPop
	}
	s.Reconnects = c.recon.Load()
	s.PingsOK = c.pingOK
	s.PingsMissed = c.pingMi
	s.MessagesRx = c.msgRx.Load()
	s.CoreRx = c.coreRx.Load()
	s.LatencyMs = make(map[string]*Phase, len(c.lat))
	s.CensusSent = copyU64(c.snd)
	s.CensusRecv = copyU64(c.rcv)
	for k, v := range c.lat {
		s.LatencyMs[k] = percentile(v)
	}
	// delivery table
	s.Delivery = make(map[string]*DeliveryPhase)
	kinds := map[string]bool{}
	for k := range c.fanExpected {
		kinds[k] = true
	}
	for k := range c.lat {
		if len(k) > len("delivery.") && k[:len("delivery.")] == "delivery." {
			kinds[k[len("delivery."):]] = true
		}
	}
	for k := range kinds {
		dp := &DeliveryPhase{}
		if p, ok := s.LatencyMs["delivery."+k]; ok {
			dp.P50, dp.P95, dp.Max, dp.Count = p.P50, p.P95, p.Max, p.Count
		}
		dp.Arrived = c.fanArrived[k]
		dp.Expected = c.fanExpected[k]
		if dp.Expected > 0 {
			dp.FanPct = 100 * float64(dp.Arrived) / float64(dp.Expected)
		}
		s.Delivery[k] = dp
	}
	if len(c.pubTracks) > 0 {
		s.PubTrackSends = make([]*PubTrackSend, len(c.pubTracks))
		copy(s.PubTrackSends, c.pubTracks)
	}
	if res := c.resources; res != nil && res.Supported {
		s.Resources = res
	}
	if c.clockSet {
		ms := c.clockOffsetMs
		s.ClockOffsetMs = &ms
	}
	s.Media = c.media
	// real-user-sync block (always emitted; zero-filled when untouched).
	kindsRUS := []string{"pchat", "wbSyncReq", "wbSyncRes", "notepadSyncReq", "notepadSyncRes"}
	mRUS := map[string]bool{}
	for _, k := range kindsRUS {
		mRUS[k] = true
	}
	for k := range c.foreignSends {
		mRUS[k] = true
	}
	ru := &RealUserSync{Sends: map[string]int{}, Recv: map[string]int{}}
	for _, k := range kindsRUS {
		ru.Sends[k] = c.foreignSends[k]
	}
	rusRecvKinds := []string{"wbSyncRes", "notepadSyncRes"}
	mRusRecv := map[string]bool{}
	for _, k := range rusRecvKinds {
		mRusRecv[k] = true
	}
	for k := range c.foreignRecv {
		mRusRecv[k] = true
	}
	for k := range mRusRecv {
		ru.Recv[k] = c.foreignRecv[k]
	}
	if len(c.foreignRTT) > 0 {
		ru.RTT = make(map[string]*Phase, len(c.foreignRTT))
		for k, v := range c.foreignRTT {
			ru.RTT[k] = percentile(v)
		}
	}
	s.RealUserSync = ru
	if c.memRes != nil {
		s.Resmon = c.memRes
	} else {
		s.Resmon = &resmon.MemResult{Series: []resmon.MemSample{}}
	}
	// firstSeen subjects
	s.FirstSeenSubj = make(map[string]string, len(c.firstSeen))
	for k, v := range c.firstSeen {
		s.FirstSeenSubj[k] = v
	}
	return s
}

func copyU64(m map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func copyInt(m map[string]int) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func percentile(list []time.Duration) *Phase {
	s := make([]float64, 0, len(list))
	for _, d := range list {
		s = append(s, float64(d.Microseconds())/1000.0)
	}
	sort.Float64s(s)
	return &Phase{
		P50:   s[len(s)/2],
		P95:   s[int(float64(len(s)-1)*0.95)],
		Max:   s[len(s)-1],
		Count: len(s),
	}
}

// WriteSummary prints the text summary to stdout and writes
// ./results/<runID>-summary.txt and .json.
func (c *Collector) WriteSummary(runID string) {
	s := c.Snapshot()
	fmt.Print("\n========== SUMMARY ==========\n")
	fmt.Printf("duration: %.0fs | users %d/%d joined | msgs rx: %d (core-subject: %d)\n",
		s.RunDurationSec, s.Joined, s.TotalUsers, s.MessagesRx, s.CoreRx)
	if len(s.RoomsJoined) > 0 {
		ks := make([]string, 0, len(s.RoomsJoined))
		for k := range s.RoomsJoined {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		var line strings.Builder
		for _, k := range ks {
			fmt.Fprintf(&line, "%s live=%d joins=%d ", k, s.RoomLiveJoined[k], s.RoomsJoined[k])
		}
		fmt.Printf("rooms: %s\n", line.String())
	}
	if s.ChurnLeaves > 0 {
		fmt.Printf("churn: leaves=%d joins=%d population min/max=%d/%d (current=%d)\n",
			s.ChurnLeaves, s.ChurnJoins, s.ChurnPopMin, s.ChurnPopMax, s.ChurnPop)
	}
	fmt.Printf("pings ok=%d missed=%d | reconnects=%d\n", s.PingsOK, s.PingsMissed, s.Reconnects)
	fmt.Println("--- errors ---")
	keys := sortedKeysInt(s.Errors)
	var errLine strings.Builder
	if len(keys) == 0 {
		fmt.Println("  none")
	}
	for _, k := range keys {
		fmt.Fprintf(&errLine, "%s=%d ", k, s.Errors[k])
	}
	if len(keys) > 0 {
		fmt.Println("  " + errLine.String())
	}
	fmt.Println("--- latency percentiles (ms) ---")
	for _, k := range sortedKeysPhase(s.LatencyMs) {
		p := s.LatencyMs[k]
		fmt.Printf("  %-24s p50=%7.1f p95=%7.1f max=%7.1f n=%d\n", k, p.P50, p.P95, p.Max, p.Count)
	}
	printDeliveryTable(s.Delivery)
	printRealUserSync(s.RealUserSync)
	printResmon(s.Resmon)
	printFirstSeen(s.FirstSeenSubj)
	fmt.Println("--- census (sent/received by event) ---")
	all := map[string]bool{}
	for k := range s.CensusSent {
		all[k] = true
	}
	for k := range s.CensusRecv {
		all[k] = true
	}
	cKeys := make([]string, 0, len(all))
	for k := range all {
		cKeys = append(cKeys, k)
	}
	sort.Strings(cKeys)
	for _, k := range cKeys {
		fmt.Printf("  %-34s sent=%-6d recv=%-6d\n", k, s.CensusSent[k], s.CensusRecv[k])
	}
	printResources(s.Resources)

	_ = os.MkdirAll("results", 0o755)
	_ = os.WriteFile("results/"+runID+"-summary.txt", []byte(textBlock(s)), 0o644)
	if bj, err := json.MarshalIndent(s, "", "  "); err == nil {
		_ = os.WriteFile("results/"+runID+"-summary.json", bj, 0o644)
	}
}

func textBlock(s Snapshot) string {
	var b strings.Builder
	fmt.Fprintf(&b, "duration: %.0fs | users %d/%d joined | msgs rx: %d (core: %d)\n",
		s.RunDurationSec, s.Joined, s.TotalUsers, s.MessagesRx, s.CoreRx)
	if len(s.RoomsJoined) > 0 {
		ks := make([]string, 0, len(s.RoomsJoined))
		for k := range s.RoomsJoined {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		b.WriteString("rooms: ")
		for _, k := range ks {
			fmt.Fprintf(&b, "%s live=%d joins=%d ", k, s.RoomLiveJoined[k], s.RoomsJoined[k])
		}
		b.WriteString("\n")
	}
	if s.ChurnLeaves > 0 {
		fmt.Fprintf(&b, "churn: leaves=%d joins=%d population min/max=%d/%d (current=%d)\n",
			s.ChurnLeaves, s.ChurnJoins, s.ChurnPopMin, s.ChurnPopMax, s.ChurnPop)
	}
	fmt.Fprintf(&b, "pings ok=%d missed=%d | reconnects=%d\n", s.PingsOK, s.PingsMissed, s.Reconnects)
	fmt.Fprintf(&b, "errors: %v\n", s.Errors)
	b.WriteString("--- latency percentiles (ms) ---\n")
	for _, k := range sortedKeysPhase(s.LatencyMs) {
		p := s.LatencyMs[k]
		fmt.Fprintf(&b, "%-24s p50=%7.1f p95=%7.1f max=%7.1f n=%d\n", k, p.P50, p.P95, p.Max, p.Count)
	}
	b.WriteString(toTextDeliveryTable(s.Delivery))
	b.WriteString(toTextRealUserSync(s.RealUserSync))
	b.WriteString(toTextResmon(s.Resmon))
	b.WriteString(toTextFirstSeen(s.FirstSeenSubj))
	b.WriteString("--- census (sent/received by event) ---\n")
	all := map[string]bool{}
	for k := range s.CensusSent {
		all[k] = true
	}
	for k := range s.CensusRecv {
		all[k] = true
	}
	cKeys := make([]string, 0, len(all))
	for k := range all {
		cKeys = append(cKeys, k)
	}
	sort.Strings(cKeys)
	for _, k := range cKeys {
		fmt.Fprintf(&b, "%-34s sent=%-6d recv=%-6d\n", k, s.CensusSent[k], s.CensusRecv[k])
	}
	b.WriteString(toTextResources(s.Resources))
	return b.String()
}

// printResources renders the host resources block (stdout variant).
func printResources(r *resmon.Result) {
	if r == nil || !r.Supported {
		return
	}
	fmt.Printf("--- resources (this machine, %ds samples, n=%d) ---\n", r.IntervalMs/1000, r.Samples)
	fmt.Printf("  machine cpu: avg %.1f%% p95 %.1f%% max %.1f%% (%d cores) | load1 avg %.1f max %.1f\n",
		r.Machine.Avg, r.Machine.P95, r.Machine.Max, r.Cores, r.Load1.Avg, r.Load1.Max)
	printProcRows(r.Procs, func(format string, args ...any) {
		fmt.Printf(format, args...)
	})
	fmt.Printf("  hint: a process above 100%% used more than one core\n")
}

// toTextResources renders the host resources block (txt-file variant).
func toTextResources(r *resmon.Result) string {
	if r == nil || !r.Supported {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "--- resources (this machine, %ds samples, n=%d) ---\n", r.IntervalMs/1000, r.Samples)
	fmt.Fprintf(&b, "machine cpu: avg %.1f%% p95 %.1f%% max %.1f%% (%d cores) | load1 avg %.1f max %.1f\n",
		r.Machine.Avg, r.Machine.P95, r.Machine.Max, r.Cores, r.Load1.Avg, r.Load1.Max)
	printProcRows(r.Procs, func(format string, args ...any) {
		fmt.Fprintf(&b, format, args...)
	})
	b.WriteString("hint: a process above 100% used more than one core\n")
	return b.String()
}

// printProcRows writes column-aligned per-process rows through the given emitter.
func printProcRows(procs []resmon.ProcStat, emit func(format string, args ...any)) {
	for _, p := range procs {
		name := p.Name
		if l := len(name); l > 24 {
			name = name[:24]
		}
		emit("  %-24s avg %6.1f%% p95 %6.1f%% max %6.1f%%  total %6.1f cpu-s  peak-pids %d\n",
			name, p.Avg, p.P95, p.Max, p.TotalCPUSec, p.PeakPIDs)
	}
}

// deliveryOrder shows the headline kinds first, then any extras in sorted
// order (pageChange/fileChange/notepadSyncReq/notepadSyncRes/userJoined ...).
var deliveryOrder = []string{"chat", "pchat", "reaction", "scene", "fullstate", "pointer", "notepadUpdate"}

func printDeliveryTable(del map[string]*DeliveryPhase) {
	if len(del) == 0 {
		return
	}
	fmt.Println("--- delivery RTT + fan-out (ms) ---")
	order := append([]string{}, deliveryOrder...)
	extra := make([]string, 0, len(del))
	for k := range del {
		if !containsStr(order, k) {
			extra = append(extra, k)
		}
	}
	sort.Strings(extra)
	order = append(order, extra...)
	for _, k := range order {
		// userJoined is a plain Record-series (shown in the percentiles block),
		// not a fan-out delivery kind — suppress its misleading fan=0.0% (0/0).
		if k == "userJoined" {
			continue
		}
		dp, ok := del[k]
		if !ok || (dp.Count == 0 && dp.Expected == 0) {
			continue
		}
		fmt.Printf("  %-14s p50=%7.1f p95=%7.1f max=%7.1f n=%-5d fan=%5.1f%% (%d/%d)\n",
			k, dp.P50, dp.P95, dp.Max, dp.Count, dp.FanPct, dp.Arrived, dp.Expected)
	}
}

func toTextDeliveryTable(s map[string]*DeliveryPhase) string {
	var b strings.Builder
	if len(s) == 0 {
		return ""
	}
	b.WriteString("--- delivery RTT + fan-out (ms) ---\n")
	order := append([]string{}, deliveryOrder...)
	extra := make([]string, 0, len(s))
	for k := range s {
		if !containsStr(order, k) {
			extra = append(extra, k)
		}
	}
	sort.Strings(extra)
	order = append(order, extra...)
	for _, k := range order {
		// userJoined is a plain Record-series (shown in the percentiles block),
		// not a fan-out delivery kind — suppress its misleading fan=0.0% (0/0).
		if k == "userJoined" {
			continue
		}
		dp, ok := s[k]
		if !ok || (dp.Count == 0 && dp.Expected == 0) {
			continue
		}
		fmt.Fprintf(&b, "%-14s p50=%7.1f p95=%7.1f max=%7.1f n=%-5d fan=%5.1f%% (%d/%d)\n",
			k, dp.P50, dp.P95, dp.Max, dp.Count, dp.FanPct, dp.Arrived, dp.Expected)
	}
	return b.String()
}

// printRealUserSync renders the real-user-sync block (stdout variant).
func printRealUserSync(ru *RealUserSync) {
	if ru == nil {
		return
	}
	fmt.Println("--- real-user-sync (foreign peers: outside this run's fan registry) ---")
	fmt.Printf("  sends to foreign: %s\n", ruKeyValueInt(ru.Sends))
	fmt.Printf("  recv from foreign: %s\n", ruKeyValueInt(ru.Recv))
	if len(ru.RTT) == 0 {
		fmt.Println("  direct rtt: n=0")
	} else {
		ks := sortedKeysPhase(ru.RTT)
		for _, k := range ks {
			p := ru.RTT[k]
			fmt.Printf("  direct rtt %-14s p50=%7.1f p95=%7.1f max=%7.1f n=%d\n",
				k, p.P50, p.P95, p.Max, p.Count)
		}
	}
}

// toTextRealUserSync renders the real-user-sync block (txt-file variant).
func toTextRealUserSync(ru *RealUserSync) string {
	if ru == nil {
		return ""
	}
	var b strings.Builder
	b.WriteString("--- real-user-sync (foreign peers: outside this run's fan registry) ---\n")
	fmt.Fprintf(&b, "sends to foreign: %s\n", ruKeyValueInt(ru.Sends))
	fmt.Fprintf(&b, "recv from foreign: %s\n", ruKeyValueInt(ru.Recv))
	if len(ru.RTT) == 0 {
		b.WriteString("direct rtt: n=0\n")
	} else {
		for _, k := range sortedKeysPhase(ru.RTT) {
			p := ru.RTT[k]
			fmt.Fprintf(&b, "direct rtt %-14s p50=%7.1f p95=%7.1f max=%7.1f n=%d\n",
				k, p.P50, p.P95, p.Max, p.Count)
		}
	}
	return b.String()
}

// printResmon renders the resmon (memory) block (stdout variant).
func printResmon(r *resmon.MemResult) {
	if r == nil {
		return
	}
	fmt.Printf("--- resmon (process RSS + machine memory, %ds interval, n=%d) ---\n", r.IntervalSec, r.Samples)
	rr := r.ProcRssMB
	fmt.Printf("  proc rss mb: start=%.1f p50=%.1f peak=%.1f end=%.1f | users@peak=%d rss/user@peak=%.2f\n",
		rr.Start, rr.P50, rr.Peak, rr.End, r.UsersAtRssPeak, r.RssPerUserAtPeak)
	if r.MemAvailMB != nil {
		fmt.Printf("  mem avail mb: start=%.0f min=%.0f end=%.0f\n", r.MemAvailMB.Start, r.MemAvailMB.Min, r.MemAvailMB.End)
	} else {
		fmt.Println("  mem avail mb: unavailable")
	}
}

// toTextResmon renders the resmon (memory) block (txt-file variant).
func toTextResmon(r *resmon.MemResult) string {
	if r == nil {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "--- resmon (process RSS + machine memory, %ds interval, n=%d) ---\n", r.IntervalSec, r.Samples)
	rr := r.ProcRssMB
	fmt.Fprintf(&b, "proc rss mb: start=%.1f p50=%.1f peak=%.1f end=%.1f | users@peak=%d rss/user@peak=%.2f\n",
		rr.Start, rr.P50, rr.Peak, rr.End, r.UsersAtRssPeak, r.RssPerUserAtPeak)
	if r.MemAvailMB != nil {
		fmt.Fprintf(&b, "mem avail mb: start=%.0f min=%.0f end=%.0f\n", r.MemAvailMB.Start, r.MemAvailMB.Min, r.MemAvailMB.End)
	} else {
		b.WriteString("mem avail mb: unavailable\n")
	}
	return b.String()
}

// ruKeyValueInt renders a small counter map as aligned "key=n" pairs.
func ruKeyValueInt(m map[string]int) string {
	ks := make([]string, 0, len(m))
	out := make([]string, 0)
	seen := map[string]bool{}
	for k := range m {
		if !seen[k] {
			seen[k] = true
			ks = append(ks, k)
		}
	}
	sort.Strings(ks)
	for _, k := range ks {
		out = append(out, fmt.Sprintf("%s=%d", k, m[k]))
	}
	if len(out) == 0 {
		return "none"
	}
	return strings.Join(out, " ")
}

func printFirstSeen(m map[string]string) {
	if len(m) == 0 {
		return
	}
	fmt.Println("--- first seen arrival subjects ---")
	for _, k := range sortedStrings(m) {
		fmt.Printf("  %-24s %s\n", k, m[k])
	}
}

func toTextFirstSeen(m map[string]string) string {
	var b strings.Builder
	if len(m) == 0 {
		return ""
	}
	b.WriteString("--- first seen arrival subjects ---\n")
	for _, k := range sortedStrings(m) {
		fmt.Fprintf(&b, "%-24s %s\n", k, m[k])
	}
	return b.String()
}

func containsStr(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func sortedStrings(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedKeysInt(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedKeysPhase(m map[string]*Phase) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
