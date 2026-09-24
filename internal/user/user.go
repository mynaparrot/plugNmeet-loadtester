package user

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mynaparrot/plugnmeet-loadtester/internal/media"
	"github.com/mynaparrot/plugnmeet-loadtester/internal/report"
	"github.com/mynaparrot/plugnmeet-loadtester/internal/transport"
	"github.com/mynaparrot/plugnmeet-protocol/plugnmeet"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Per-virtual-user lifecycle: run/joinSequence, steady-state timers, onMsg
// and its thin dispatch, publish helpers. The action loops and the pnm-client
// shape map live in actions.go/chat.go/notepad.go/whiteboard.go/presence.go.

const (
	pingInterval       = 10 * time.Second
	onlineListInterval = 30 * time.Second
	renewInterval      = 3 * time.Minute
	usersListInterval  = 60 * time.Second
	maxConsecutiveMiss = 12
	initialDataTimeout = 15 * time.Second
	mediaDataWait      = 5 * time.Second // grace for the RES_MEDIA_SERVER_DATA reply
	usersListTimeout   = 30 * time.Second
)

// Features selects which ongoing action groups run (all default true;
// subtractive via --disable). The ambient protocol (join handshake, steady
// timers, join-time session-data fetches, USER_JOINED, visibility, quality,
// analytics) is deliberately never gated: a real client always performs it.
type Features struct {
	Chat       bool // public + private chat loops
	Whiteboard bool // presenter whiteboardLoop AND viewer pointerLoop
	Notepad    bool // presenter notepadUpdate loop
	Reactions  bool // reaction loop
	Hands      bool // raise/lower-hand loop
}

// DefaultFeatures returns all-true features.
func DefaultFeatures() Features {
	return Features{true, true, true, true, true}
}

// Params is what the engine hands each virtual user.
type Params struct {
	Http     *transport.Client
	RoomID   string
	UserID   string
	Name     string
	IsAdmin  bool
	Token    string
	Rep      *report.Collector
	Features Features
	// Idx is the per-room user index (0 = presenter, checked by run-rate table).
	Idx int
	// MediaRole is this user's per-cohort media role, derived by the engine
	// from the per-room user index. RoleCore = no LiveKit connection at all.
	// Non-core roles require the RES_MEDIA_SERVER_DATA join fetch.
	MediaRole media.Role
	// RunID is the engine run id; synthetic peer ids are
	// "lt-<RunID>-<rIdx>-<uIdx>", so this is what makes isSyntheticID work.
	RunID string
}

// Run executes the full user lifecycle and blocks until the run duration
// elapses (or a fatal per-user condition triggers an early close).
func Run(ctx context.Context, p Params) error {
	u := &runner{
		p:          p,
		token:      p.Token,
		chInit:     make(chan *plugnmeet.NatsInitialData, 4),
		chUL:       make(chan struct{}, 128),
		peerSet:    make(map[string]struct{}),
		seenKinds:  make(map[plugnmeet.NatsMsgServerToClientEvents]struct{}),
		joinedSeen: make(map[string]struct{}),
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := u.run(runCtx, cancel); err != nil {
		return err
	}
	// The internal cancel only fires for the 12-consecutive-ping case; for a
	// normal expiry the parent ctx ends. In both cases run() drains cleanly.
	return u.runErr
}

type runner struct {
	p Params

	conn *nats.Conn
	cfg  transport.SessionConfig
	cc   jetstream.ConsumeContext

	// media connection state (set once by startMedia)
	media *media.Connection

	idSeq atomic.Uint64 // msg id counter (atomic: send() is called from concurrent action goroutines)
	mu    sync.Mutex
	token string

	// ping correlation (all guarded by mu)
	lastPingSent time.Time
	lastPongAt   time.Time
	consecMissed int
	renewSent    time.Time
	onlineSent   time.Time

	// media connection data from RES_MEDIA_SERVER_DATA (guarded by mu;
	// written by onMsg — read once after joinSequence to start the media
	// connection).
	mediaURL   string
	mediaToken string

	// ulSent/ulNext pair THIS user's outstanding REQ_JOINED_USERS_LIST with
	// its own RES_JOINED_USERS_LIST chunks; unsolicited broadcast lists never
	// enter the usersList latency series (still census-counted).
	ulSent  time.Time
	ulNext  int
	donorID string
	donorAt uint64
	peers   []string            // other user ids seen in users-list chunks
	peerSet map[string]struct{} // membership mirror: dedupe peers on append

	// chatSeen gates the FirstSeen("chat") report to the first message only.
	// Race-safe as a plain bool: nats.go invokes all subscription handlers of
	// one conn from a single serial dispatcher goroutine.
	chatSeen bool

	// env is the reused decode envelope (value type, only via &u.env), touched
	// only from onMsg (serialized by nats.go's Consume loop).
	env plugnmeet.NatsMsgServerToClient

	// seenKinds caches already-FirstSeen-reported event kinds (onMsg is the
	// only writer; serial dispatch makes the plain map race-safe).
	seenKinds map[plugnmeet.NatsMsgServerToClientEvents]struct{}

	// USER_JOINED latency-sample dedup key "<userId>|<joinedAtMillis>":
	// joinedAt keeps genuine rejoins while skipping re-announcement copies;
	// the Received census still counts every delivery.
	joinedSeen map[string]struct{}

	// action state (guarded by mu)
	visHidden bool // last visibility toggle value

	// realUserSeen is the STICKY hybrid-safety flag: once set (a real, i.e.
	// non-synthetic user observed among known peers) hybrid-safe minimal yjs
	// payloads are used for the rest of the run, never the random corpus.
	realUserSeen atomic.Bool

	// donor chat history for REQ_PUBLIC_CHAT_DATA replies (guarded by mu,
	// bounded at maxChatHistory entries)
	chatHist []*plugnmeet.ChatMessage

	chInit chan *plugnmeet.NatsInitialData
	chUL   chan struct{}

	// selfPresenter is this user's presenter truth
	// (gated action loops read isPresenter()).
	selfPresenter atomic.Bool

	runErr error // set only on ping-dead early close
}

// isPresenter reports whether the server currently marks this user presenter.
func (u *runner) isPresenter() bool { return u.selfPresenter.Load() }

func (u *runner) err(kind string, err error) error {
	u.p.Rep.AddError(kind)
	return fmt.Errorf("%s: %w", kind, err)
}

// errNote reports a decode failure with its truncated error text via AddError.
// recordServerStampLatency records the only server-stamped latency series
// (delivery.userJoined, sample = localNow − joinedAt): the measured clock
// offset is subtracted (clamped at 0) only when |offset|>2000ms — below that
// the 1s HTTP-Date granularity would only degrade honest samples.
func (u *runner) recordServerStampLatency(kind string, serverMillis int64) {
	sample := time.Since(time.UnixMilli(serverMillis))
	if off, ok := u.p.Rep.ClockOffsetMs(); ok && abs(off) > 2000 {
		if corrected := sample - time.Duration(off)*time.Millisecond; corrected > 0 {
			sample = corrected
		} else {
			sample = 0
		}
	}
	u.p.Rep.Record(kind, sample)
}

func abs(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

func (u *runner) errNote(kind string, err error) {
	const max = 200
	msg := err.Error()
	if len(msg) > max {
		msg = msg[:max]
	}
	u.p.Rep.AddError(kind + ": " + msg)
}

func (u *runner) run(ctx context.Context, cancel context.CancelFunc) error {
	r := u.p.Rep

	// b: verifyToken
	t0 := time.Now()
	vt, terr := u.p.Http.VerifyToken(u.p.Token)
	if terr != nil {
		return u.err("verifyToken", terr)
	}
	if !vt.GetStatus() {
		return u.err("verifyToken.rejected", fmt.Errorf("%s", vt.GetMsg()))
	}
	r.Record("verifyToken", time.Since(t0))

	natsURL, nerr := deriveNatsURL(vt.GetNatsWsUrls())
	if nerr != nil {
		return u.err("natsUrl", nerr)
	}
	u.cfg = transport.SessionConfig{
		ServerURL:      u.p.Http.ServerURL,
		ApiKey:         u.p.Http.ApiKey,
		ApiSecret:      u.p.Http.ApiSecret,
		AccessToken:    u.p.Token,
		NatsWsUrls:     []string{natsURL},
		RoomID:         vt.GetRoomId(),
		UserID:         vt.GetUserId(),
		RoomStreamName: vt.GetRoomStreamName(),
		Subjects:       vt.GetNatsSubjects(),
		OnReconnect:    r.AddReconnect,
	}
	if u.cfg.Subjects == nil || u.cfg.RoomID == "" || u.cfg.UserID == "" {
		return u.err("verifyToken.incomplete", fmt.Errorf("subjects/room/user empty"))
	}

	// c: connect + consumer
	t0 = time.Now()
	conn, cerr := transport.ConnectNats(u.cfg)
	if cerr != nil {
		return u.err("connect", cerr)
	}
	u.conn = conn
	r.Record("connect", time.Since(t0))

	js, jerr := jetstream.New(conn)
	if jerr != nil {
		return u.err("jetstream", jerr)
	}
	consumer, aerr := transport.AttachUserConsumer(ctx, js, u.cfg)
	if aerr != nil {
		return u.err("attachConsumer", aerr)
	}
	cc, cerr := consumer.Consume(u.onMsg)
	if cerr != nil {
		return u.err("consume", cerr)
	}
	u.cc = cc

	if jerr := u.joinSequence(ctx); jerr != nil {
		cc.Stop()
		_ = conn.Drain()
		return jerr
	}
	r.AddJoined(u.p.RoomID)

	// Media connection: started right after the join sequence when --media
	// is on and the RES_MEDIA_SERVER_DATA fetch delivered a LiveKit
	// url+token.
	u.startMedia(ctx)

	u.startSteadyState(ctx, cancel)
	u.startActions(ctx)

	// Run-window expiry (or internal cancel): stop the media connection
	// BEFORE cc.Stop()/conn.Drain() — the shutdownGrace window keeps the
	// NATS consumer attached so departure broadcasts are recorded.
	<-ctx.Done()
	u.stopMedia()
	cc.Stop()
	_ = conn.Drain()
	return nil
}

// startMedia starts the user's LiveKit media connection per its media role
// (core = none). Non-core roles poll briefly for the captured
// RES_MEDIA_SERVER_DATA url+token; the join runs on its own goroutine.
func (u *runner) startMedia(ctx context.Context) {
	if u.p.MediaRole == media.RoleCore {
		return
	}

	// The RES_MEDIA_SERVER_DATA reply may still be in flight when the join
	// sequence returns; poll briefly for the captured url+token.
	deadline := time.Now().Add(mediaDataWait)
	var url, token string
	for {
		u.mu.Lock()
		url, token = u.mediaURL, u.mediaToken
		u.mu.Unlock()
		if url != "" && token != "" {
			break
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			u.p.Rep.AddError("media.noServerData")
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
	u.media = &media.Connection{
		Mode:     u.p.MediaRole.Mode(),
		URL:      url,
		Token:    token,
		RoomName: u.p.RoomID,
		Label:    u.cfg.UserID,
		Report:   u.p.Rep,
	}
	conn := u.media
	go func() {
		if err := conn.Start(ctx); err != nil {
			if ctx.Err() == nil { // abort-at-close is not an error
				msg := err.Error()
				if len(msg) > 200 {
					msg = msg[:200]
				}
				u.p.Rep.AddError("media.join: " + msg)
			}
		}
	}()
}

// stopMedia stops the media connection (no-op when disabled).
func (u *runner) stopMedia() {
	if u.media != nil {
		u.media.Stop()
	}
}

// deriveNatsURL takes the first nats_ws_urls entry returned by verifyToken
// and rewrites the scheme http→ws / https→wss (mirrors the web client).
func deriveNatsURL(urls []string) (string, error) {
	if len(urls) == 0 || urls[0] == "" {
		return "", fmt.Errorf("no nats_ws_urls in verifyToken response")
	}
	s := urls[0]
	switch {
	case strings.HasPrefix(s, "https://"):
		return "wss://" + s[len("https://"):], nil
	case strings.HasPrefix(s, "http://"):
		return "ws://" + s[len("http://"):], nil
	default:
		return "", fmt.Errorf("unexpected nats url scheme: %s", s)
	}
}

// send publishes a typed NatsMsgClientToServer envelope.
func (u *runner) send(subject string, ev plugnmeet.NatsMsgClientToServerEvents, msg string, binMsg []byte) error {
	id := u.idSeq.Add(1)
	b, err := proto.Marshal(&plugnmeet.NatsMsgClientToServer{
		Id:     fmt.Sprintf("lt-%d", id),
		Event:  ev,
		Msg:    msg,
		BinMsg: binMsg,
	})
	if err != nil {
		return err
	}
	return u.conn.Publish(subject, b)
}

// rawPublish publishes a live-event payload that is already wire-format proto
// (no NatsMsgClientToServer envelope) — used on chat.{roomId} and
// dataChannel.{roomId}.
func (u *runner) rawPublish(subject string, bin []byte) error {
	return u.conn.Publish(subject, bin)
}

func (u *runner) chatSubject() string {
	return fmt.Sprintf("%s.%s", u.cfg.Subjects.GetChat(), u.cfg.RoomID)
}

func (u *runner) dataChannelSubject() string {
	return fmt.Sprintf("%s.%s", u.cfg.Subjects.GetDataChannel(), u.cfg.RoomID)
}

// sendPrivate delivers binMsg on REQ_PRIVATE_DATA_DELIVERY to a single peer
// (same envelope machinery as joinSequence step h). kind is
// PrivateDataDelivery.type (string, e.g. "CHAT" or a DataMsgBodyType name).
func (u *runner) sendPrivate(to string, kind string, bin []byte) error {
	header, err := protojson.Marshal(&plugnmeet.PrivateDataDelivery{
		ToUserId:     to,
		EchoToSender: false,
		Type:         kind,
	})
	if err != nil {
		return err
	}
	serr := u.send(u.jsWorkerSubject(), plugnmeet.NatsMsgClientToServerEvents_REQ_PRIVATE_DATA_DELIVERY, string(header), bin)
	if serr == nil {
		u.p.Rep.Sent(plugnmeet.NatsMsgClientToServerEvents_REQ_PRIVATE_DATA_DELIVERY.String())
	}
	return serr
}

// pickPeer returns a random other user id from the last users-list chunks,
// "" if there is no peer.
func (u *runner) pickPeer() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.peers) == 0 {
		return ""
	}
	return u.peers[rand.Intn(len(u.peers))]
}

func (u *runner) jsWorkerSubject() string {
	return fmt.Sprintf("%s.%s.%s", u.cfg.Subjects.SystemJsWorker, u.cfg.RoomID, u.cfg.UserID)
}

func (u *runner) coreWorkerSubject() string {
	return fmt.Sprintf("%s.%s.%s", u.cfg.Subjects.SystemCoreWorker, u.cfg.RoomID, u.cfg.UserID)
}

// joinSequence performs the exact pnm-client join order.
func (u *runner) joinSequence(ctx context.Context) error {
	subjs := u.cfg.Subjects
	roomID := u.cfg.RoomID
	r := u.p.Rep

	// d: REQ_INITIAL_DATA
	t0 := time.Now()
	if serr := u.send(u.jsWorkerSubject(), plugnmeet.NatsMsgClientToServerEvents_REQ_INITIAL_DATA, "", nil); serr != nil {
		return u.err("send.initialData", serr)
	}
	r.Sent(plugnmeet.NatsMsgClientToServerEvents_REQ_INITIAL_DATA.String())
	select {
	case initial := <-u.chInit:
		_ = initial
	case <-ctx.Done():
		return u.err("initialData.aborted", ctx.Err())
	case <-time.After(initialDataTimeout):
		return u.err("initialData.timeout", fmt.Errorf("no RES_INITIAL_DATA in 15s"))
	}
	r.Record("initialData", time.Since(t0))

	// e: REQ_JOINED_USERS_LIST (chunked)
	u.mu.Lock()
	u.ulSent = time.Now()
	u.ulNext = 0
	u.mu.Unlock()
	if serr := u.send(u.jsWorkerSubject(), plugnmeet.NatsMsgClientToServerEvents_REQ_JOINED_USERS_LIST, "", nil); serr != nil {
		return u.err("send.usersList", serr)
	}
	r.Sent(plugnmeet.NatsMsgClientToServerEvents_REQ_JOINED_USERS_LIST.String())
	select {
	case <-u.chUL: // join complete only at chunk == total-1 (signalled in onUsersList)
	case <-ctx.Done():
		return u.err("usersList.aborted", ctx.Err())
	case <-time.After(usersListTimeout):
		return u.err("usersList.timeout", fmt.Errorf("users list incomplete in 30s"))
	}
	// The usersList latency sample is recorded by the onUsersList pairing;
	// an unsolicited completion only wakes the join flow.

	// f: REQ_MEDIA_SERVER_DATA — send and ignore the reply.
	if serr := u.send(u.jsWorkerSubject(), plugnmeet.NatsMsgClientToServerEvents_REQ_MEDIA_SERVER_DATA, "", nil); serr != nil {
		return u.err("send.mediaServerData", serr)
	}
	r.Sent(plugnmeet.NatsMsgClientToServerEvents_REQ_MEDIA_SERVER_DATA.String())

	// g: core subscriptions (templates from res.nats_subjects).
	tpls := map[string]string{
		subjs.Chat:         "chat",
		subjs.Whiteboard:   "whiteboard",
		subjs.DataChannel:  "dataChannel",
		subjs.SystemPublic: "sysPublic",
	}
	for tpl, name := range tpls {
		subject := fmt.Sprintf("%s.%s", tpl, roomID)
		u.p.Rep.FirstSeen("subscriptions."+name, subject) // arrival-subject evidence
		var handler func(*nats.Msg)
		switch name {
		case "chat":
			handler = u.onChatSub
		case "whiteboard":
			handler = u.onWhiteboardSub
		case "dataChannel":
			handler = u.onDataChannelSub
		default:
			handler = func(_ *nats.Msg) { r.CoreRxAdd(1) } // sysPublic stays count-only
		}
		if _, serr := u.conn.Subscribe(subject, handler); serr != nil {
			return u.err("subscribe."+name, serr)
		}
	}

	// g2: join additions — session data (whiteboard + notepad, plain and
	// dedup ("~d") variants) and a notepad sync request to the donor.
	for _, sd := range []struct {
		dt  plugnmeet.SessionDataType
		key string
	}{
		{plugnmeet.SessionDataType_SESSION_DATA_TYPE_WHITEBOARD, "default_1"},
		{plugnmeet.SessionDataType_SESSION_DATA_TYPE_WHITEBOARD, "default_1~d"},
		{plugnmeet.SessionDataType_SESSION_DATA_TYPE_NOTEPAD, "snapshot"},
		{plugnmeet.SessionDataType_SESSION_DATA_TYPE_NOTEPAD, "snapshot~d"},
	} {
		if serr := u.sessionDataFetchRequest(sd.dt, sd.key); serr != nil {
			return u.err("send.sessionDataFetch", serr)
		}
	}
	if donor := u.pickDonor(); donor != "" {
		if serr := u.notepadSyncRequest(donor); serr != nil {
			return u.err("send.notepadSyncRequest", serr)
		}
	}

	// Join-time whiteboard sync request: same always-on protocol spot as the
	// notepad sync (ungated by --disable whiteboard). Deliberately WITHOUT:
	// the join-bootstrap donor form (we are the requester here), the 2s/
	// 3-attempt sendSyncRequest retry loop, and reverse-sync.
	for _, peer := range u.syncTargets() {
		if serr := u.whiteboardSyncRequest(peer); serr != nil {
			return u.err("send.whiteboardSyncRequest", serr)
		}
	}

	// h: REQ_PUBLIC_CHAT_DATA private delivery to the earliest-joined peer.
	if donor := u.pickDonor(); donor != "" {
		bin, berr := proto.Marshal(&plugnmeet.DataChannelMessage{
			Type: plugnmeet.DataMsgBodyType_REQ_PUBLIC_CHAT_DATA,
		})
		if berr != nil {
			return u.err("marshal.dataChannelMsg", berr)
		}
		if serr := u.sendPrivate(donor, plugnmeet.DataMsgBodyType_REQ_PUBLIC_CHAT_DATA.String(), bin); serr != nil {
			return u.err("send.privateDelivery", serr)
		}
	}
	return nil
}

// pickDonor returns the user id of the earliest-joined OTHER user
// (REQ_PUBLIC_CHAT_DATA donor), "" if there is no peer.
func (u *runner) pickDonor() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.donorID
}

// isSyntheticID reports whether id matches the engine's synthetic
// user-id format "lt-<RunID>-<rIdx>-<uIdx>".
func (u *runner) isSyntheticID(id string) bool {
	return strings.HasPrefix(id, "lt-"+u.p.RunID+"-")
}

// anyRealUser reports whether any real (non-synthetic) user was seen; the
// sticky hybrid-safety flag forces minimal VALID yjs payloads from then on.
func (u *runner) anyRealUser() bool {
	if u.realUserSeen.Load() {
		return true
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	for _, id := range u.peers {
		if !u.isSyntheticID(id) {
			u.realUserSeen.Store(true)
			return true
		}
	}
	return false
}

// syncTargets picks up to 3 targets for the join-time whiteboard sync
// request (donor first, then peers in arrival order — a simplified
// getSyncRequestTargets without per-peer admin/presenter tracking).
func (u *runner) syncTargets() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	seen := map[string]bool{u.cfg.UserID: true}
	out := make([]string, 0, 3)
	add := func(id string) {
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		out = append(out, id)
	}
	add(u.donorID)
	for _, p := range u.peers {
		if len(out) >= 3 {
			break
		}
		add(p)
	}
	return out
}

// startSteadyState launches the four periodic timers. They end with ctx.
func (u *runner) startSteadyState(ctx context.Context, cancel context.CancelFunc) {
	go u.pingLoop(ctx, cancel)
	go u.periodic(ctx, onlineListInterval, "onlineList", u.sendOnlineList)
	go u.periodic(ctx, renewInterval, "renew", u.sendRenew)
	go u.periodic(ctx, usersListInterval, "reconcile", u.sendUsersList)
}

type periodicFn func() error

func (u *runner) periodic(ctx context.Context, every time.Duration, name string, fn periodicFn) {
	// first tick follows the interval; the join sequence already fired the initial requests
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := fn(); err != nil {
				u.p.Rep.AddError("send." + name)
			}
		}
	}
}

func (u *runner) sendUsersList() error {
	// arm the request pairing for this reconciliation round
	u.mu.Lock()
	u.ulSent = time.Now()
	u.ulNext = 0
	u.mu.Unlock()
	if err := u.send(u.jsWorkerSubject(), plugnmeet.NatsMsgClientToServerEvents_REQ_JOINED_USERS_LIST, "", nil); err != nil {
		return err
	}
	return nil
}

func (u *runner) sendOnlineList() error {
	u.mu.Lock()
	u.onlineSent = time.Now()
	u.mu.Unlock()
	serr := u.send(u.coreWorkerSubject(), plugnmeet.NatsMsgClientToServerEvents_REQ_ONLINE_USERS_LIST, "", nil)
	if serr == nil {
		u.p.Rep.Sent(plugnmeet.NatsMsgClientToServerEvents_REQ_ONLINE_USERS_LIST.String())
	}
	return serr
}

func (u *runner) sendRenew() error {
	u.mu.Lock()
	u.renewSent = time.Now()
	tok := u.token
	u.mu.Unlock()
	serr := u.send(u.jsWorkerSubject(), plugnmeet.NatsMsgClientToServerEvents_REQ_RENEW_PNM_TOKEN, tok, nil)
	if serr == nil {
		u.p.Rep.Sent(plugnmeet.NatsMsgClientToServerEvents_REQ_RENEW_PNM_TOKEN.String())
	}
	return serr
}

func (u *runner) sendPing() error {
	u.mu.Lock()
	u.lastPingSent = time.Now()
	u.mu.Unlock()
	serr := u.send(u.coreWorkerSubject(), plugnmeet.NatsMsgClientToServerEvents_PING, "", nil)
	if serr == nil {
		u.p.Rep.Sent(plugnmeet.NatsMsgClientToServerEvents_PING.String())
	}
	return serr
}

// pingLoop: PING immediately then every 10s; ≥12 consecutive ticks without
// PONG end the session (counted as an error).
func (u *runner) pingLoop(ctx context.Context, cancel context.CancelFunc) {
	if err := u.sendPing(); err != nil {
		cancel()
		return
	}
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			u.mu.Lock()
			missed := u.lastPongAt.Before(u.lastPingSent)
			if missed {
				u.consecMissed++
			}
			u.p.Rep.PingMissed(btoi(missed))
			tooMany := u.consecMissed >= maxConsecutiveMiss
			u.mu.Unlock()
			if tooMany {
				u.p.Rep.AddError("user.ended.pingsMissed")
				u.runErr = fmt.Errorf("12+ consecutive missed pings")
				cancel()
				return
			}
			if err := u.sendPing(); err != nil {
				cancel()
				return
			}
		}
	}
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// onMsg: decode the server envelope and dispatch by event.
func (u *runner) onMsg(m jetstream.Msg) {
	r := u.p.Rep
	// Envelope reuse: proto.Unmarshal merges, so Reset first. Safe because
	// every dispatcher decodes its own fresh sub-message, responder goroutines
	// capture only extracted scalars (never proto pointers), and onMsg is
	// serialized by nats.go's Consume loop.
	proto.Reset(&u.env)
	if err := proto.Unmarshal(m.Data(), &u.env); err != nil {
		_ = m.Nak()
		r.AddError("badEnvelope")
		return
	}
	_ = m.Ack()
	ev := u.env.GetEvent()
	r.Received(ev.String())
	r.MsgRxAdd(1)

	// FirstSeen once per event kind per runner (the collector is set-once;
	// caching avoids rebuilding the key string per message).
	if _, seen := u.seenKinds[ev]; !seen {
		u.seenKinds[ev] = struct{}{}
		u.p.Rep.FirstSeen("event."+ev.String(), m.Subject())
	}

	switch ev {
	case plugnmeet.NatsMsgServerToClientEvents_USER_JOINED:
		// MSG = protojson NatsKvUserInfo — regular latency series: server
		// broadcast arrival time vs the user's JoinedAt timestamp (joinedAt).
		ui := new(plugnmeet.NatsKvUserInfo)
		if err := protojson.Unmarshal([]byte(u.env.GetMsg()), ui); err != nil {
			u.errNote("userJoined.decode", err)
			return
		}
		// Latency sample recorded only for the first receipt of each
		// (userId, joinedAt) pair (dedup key above).
		jkey := fmt.Sprintf("%s|%d", ui.GetUserId(), ui.GetJoinedAt())
		u.mu.Lock()
		_, seenJoin := u.joinedSeen[jkey]
		u.joinedSeen[jkey] = struct{}{}
		u.mu.Unlock()
		if !seenJoin {
			u.recordServerStampLatency("delivery.userJoined", int64(ui.GetJoinedAt()))
		}
		// presenter truth: a joiner may arrive with the auto-claim (or, in
		// pre-active rooms, already holding presenter).
		if ui.GetUserId() == u.cfg.UserID {
			u.selfPresenter.Store(ui.GetIsPresenter())
		}

	// census for this event still counted by the top-level Received above.
	case plugnmeet.NatsMsgServerToClientEvents_USER_METADATA_UPDATE:
		// The server's presenter-switch signal: targeted promote+demote pair
		// (MSG = protojson NatsKvUserInfo, same shape as USER_JOINED).
		md := new(plugnmeet.NatsKvUserInfo)
		if uerr := protojson.Unmarshal([]byte(u.env.GetMsg()), md); uerr != nil {
			u.errNote("userMetadata.decode", uerr)
			return
		}
		uid := md.GetUserId()
		if md.GetIsPresenter() {
			if uid == u.cfg.UserID {
				u.selfPresenter.Store(true)
			}
		} else {
			if uid == u.cfg.UserID {
				u.selfPresenter.Store(false)
			}
		}

	case plugnmeet.NatsMsgServerToClientEvents_USER_DISCONNECTED,
		plugnmeet.NatsMsgServerToClientEvents_USER_OFFLINE:
		// Leave-signal receipts for the churn kinds (USER_DISCONNECTED →
		// leaveDetect, USER_OFFLINE → leaveOffline, ~5s server consumer-grace).
		ul := new(plugnmeet.NatsKvUserInfo)
		if err := protojson.Unmarshal([]byte(u.env.GetMsg()), ul); err != nil {
			u.errNote("userLeave.decode", err)
			return
		}
		switch ev {
		case plugnmeet.NatsMsgServerToClientEvents_USER_DISCONNECTED:
			r.RecordRecv("leaveDetect", ul.GetUserId(), time.Now())
		case plugnmeet.NatsMsgServerToClientEvents_USER_OFFLINE:
			r.RecordRecv("leaveOffline", ul.GetUserId(), time.Now())
		}

	case plugnmeet.NatsMsgServerToClientEvents_DELIVERY_PRIVATE_DATA:
		u.onPrivateDelivery(&u.env)

	case plugnmeet.NatsMsgServerToClientEvents_RES_INITIAL_DATA:
		init := new(plugnmeet.NatsInitialData)
		if err := protojson.Unmarshal([]byte(u.env.GetMsg()), init); err != nil {
			u.errNote("initialData.decode", err)
			return
		}
		// presenter truth: self-claim from the initial-data snapshot
		if lu := init.GetLocalUser(); lu != nil && lu.GetIsPresenter() {
			u.selfPresenter.Store(true)
		}
		select {
		case u.chInit <- init:
		default:
		}

	case plugnmeet.NatsMsgServerToClientEvents_RES_MEDIA_SERVER_DATA:
		// REQ_MEDIA_SERVER_DATA reply (protojson MediaServerConnInfo) — not
		// decoded at all in core mode.
		if u.p.MediaRole == media.RoleCore {
			break
		}
		md := new(plugnmeet.MediaServerConnInfo)
		if err := protojson.Unmarshal([]byte(u.env.GetMsg()), md); err != nil {
			u.errNote("mediaServerData.decode", err)
			return
		}
		u.mu.Lock()
		u.mediaURL = md.GetUrl()
		u.mediaToken = md.GetToken()
		u.mu.Unlock()

	case plugnmeet.NatsMsgServerToClientEvents_RES_JOINED_USERS_LIST:
		u.onUsersList(u.env.GetMsg())

	case plugnmeet.NatsMsgServerToClientEvents_PONG:
		// msg = server's lastPing (UnixMilli string) — evidence + RTT.
		lastPing := u.env.GetMsg()
		u.mu.Lock()
		sent := u.lastPingSent
		u.lastPongAt = time.Now()
		u.consecMissed = 0
		u.mu.Unlock()
		if lastPing == "" {
			r.AddError("pong.badLastPing")
			return
		}
		// evidence captured by report (latency bucket "ping")
		if !sent.IsZero() {
			r.Record("ping", time.Since(sent))
		}
		r.PingOK()

	case plugnmeet.NatsMsgServerToClientEvents_RESP_RENEW_PNM_TOKEN:
		// msg = renewed JWT
		if u.env.GetMsg() == "" {
			r.AddError("renew.empty")
			return
		}
		// store token under u.mu: sendRenew reads it from the renewal-timer goroutine
		u.mu.Lock()
		sent := u.renewSent
		u.token = u.env.GetMsg()
		u.mu.Unlock()
		if !sent.IsZero() {
			r.Record("renew", time.Since(sent))
		}

	case plugnmeet.NatsMsgServerToClientEvents_RESP_ONLINE_USERS_LIST:
		kv := new(onlineIDs)
		if err := json.Unmarshal([]byte(u.env.GetMsg()), kv); err != nil {
			u.errNote("onlineList.decode", err)
			return
		}
		u.mu.Lock()
		sent := u.onlineSent
		u.mu.Unlock()
		if !sent.IsZero() {
			r.Record("onlineList", time.Since(sent))
		}
		_ = len(kv.Ids) // online user count

	default:
		// census covers everything else (USER_JOINED, ROOM_METADATA_UPDATE, ...)
	}
}

// onlineIDs matches the RESP_ONLINE_USERS_LIST msg JSON ({ids, hiddenIds});
// no protocol type exists.
type onlineIDs struct {
	Ids       []string `json:"ids"`
	HiddenIds []string `json:"hiddenIds"`
}

func (u *runner) onUsersList(msg string) {
	env := new(usersChunkEnvelope)
	if err := json.Unmarshal([]byte(msg), env); err != nil {
		u.errNote("usersList.decode", err)
		return
	}
	now := time.Now()
	u.mu.Lock()
	// Chunk pairing with the armed request: only chunks continuing the
	// expected order count; the request's own final chunk records the sample
	// and disarms. Broadcast responses aggregate peers below without a sample.
	var ulSample time.Duration
	ulPaired := false
	if !u.ulSent.IsZero() && env.Chunk == u.ulNext {
		u.ulNext++
		if env.Chunk == env.Total-1 {
			ulSample = now.Sub(u.ulSent)
			ulPaired = true
			u.ulSent = time.Time{}
			u.ulNext = 0
		}
	}
	for _, raw := range env.Users {
		ui := new(plugnmeet.NatsKvUserInfo)
		if err := protojson.Unmarshal(raw, ui); err != nil {
			u.mu.Unlock()
			u.errNote("usersList.user.decode", err)
			return
		}
		// presenter truth: the 60s refresh is the full census.
		if ui.GetUserId() == u.cfg.UserID {
			u.selfPresenter.Store(ui.GetIsPresenter())
		}
		if ui.GetUserId() == u.cfg.UserID || ui.GetUserId() == "" {
			continue
		}
		// users-list re-syncs re-deliver the same ids; the membership set
		// keeps duplicates out (pickPeer keeps working off the slice).
		if _, dup := u.peerSet[ui.GetUserId()]; !dup {
			u.peerSet[ui.GetUserId()] = struct{}{}
			u.peers = append(u.peers, ui.GetUserId())
		}
		if u.donorID == "" || ui.GetJoinedAt() < u.donorAt {
			u.donorID = ui.GetUserId()
			u.donorAt = ui.GetJoinedAt()
		}
	}
	done := env.Chunk == env.Total-1
	u.mu.Unlock()
	if ulPaired {
		u.p.Rep.Record("usersList", ulSample)
	}
	if done {
		select {
		case u.chUL <- struct{}{}:
		default:
		}
	}
}

// usersChunkEnvelope mirrors pnm-server GetOnlineUsersListAsJsonChunks:
// {"chunk":0,"total":1,"users":[protojson NatsKvUserInfo, ...]}; chunk size
// 50, so at ≤50 users total==1. No protocol type exists.
type usersChunkEnvelope struct {
	Chunk int               `json:"chunk"`
	Total int               `json:"total"`
	Users []json.RawMessage `json:"users"`
}
