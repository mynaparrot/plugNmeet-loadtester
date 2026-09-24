package user

import (
	"context"
	"encoding/json"
	"math/rand"
	"time"

	"github.com/mynaparrot/plugnmeet-protocol/plugnmeet"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

// Whiteboard actions mirroring the pnm-client: SCENE_UPDATE on
// whiteboard.{roomId} with scope {fileId,page[,gzip]} JSON in .message and a
// Yjs update in .bin (WhiteboardController.ts handleDocUpdate 477-495 /
// full-state 820-843 / scope 163-165; PAGE_CHANGE m=page, FILE_CHANGE
// m={"fileId",page} handleRequests.ts:14-33; POINTER_UPDATE awareness binary
// handleAwarenessChange 1318-1341).

// wbScope mirrors the pnm-client buildScopeMessage (WhiteboardController.ts
// 163-165): the JSON in every whiteboard DataChannelMessage .message.
type wbScope struct {
	FileId string `json:"fileId"`
	Page   int    `json:"page"`
	Gzip   bool   `json:"gzip,omitempty"`
}

func (u *runner) wbSubject() string {
	return u.cfg.Subjects.Whiteboard + "." + u.cfg.RoomID
}

// whiteboardDC builds the raw whiteboard.{roomId} binary (ConnectNats.ts 724-767).
func (u *runner) whiteboardDC(id string, ty plugnmeet.DataMsgBodyType, message string, bin []byte) ([]byte, error) {
	return proto.Marshal(&plugnmeet.DataChannelMessage{
		Id:         id,
		FromUserId: u.cfg.UserID,
		Type:       ty,
		Message:    message,
		BinMessage: bin,
	})
}

// sceneUpdate: SCENE_UPDATE on whiteboard.{roomId} (handleDocUpdate 477-495).
func (u *runner) sceneUpdate(size int) error {
	id := uuidV4()
	scope, err := json.Marshal(wbScope{FileId: "default", Page: 1})
	if err != nil {
		return err
	}
	// HYBRID-SAFETY: minimal VALID yjs update whenever a real user is present.
	bin := synthPayload(size)
	if u.anyRealUser() {
		bin = wbSyncMinUpdate
	}
	b, err := u.whiteboardDC(id, plugnmeet.DataMsgBodyType_SCENE_UPDATE, string(scope), bin)
	if err != nil {
		return err
	}
	if perr := u.rawPublish(u.wbSubject(), b); perr != nil {
		return perr
	}
	u.p.Rep.RecordSend("scene", id, time.Now(), u.fanExpected())
	return nil
}

// fullStateBroadcast: gzip-flagged SCENE_UPDATE full state (broadcastFullState
// 960-997); synthesized update = 200_000-500_000 bytes.
func (u *runner) fullStateBroadcast() error {
	id := uuidV4()
	size := 200_000 + rand.Intn(300_001)
	scope, err := json.Marshal(wbScope{FileId: "default", Page: 1, Gzip: true})
	if err != nil {
		return err
	}
	// HYBRID-SAFETY: gzip of the minimal VALID update whenever a real user is present.
	var raw []byte
	if u.anyRealUser() {
		raw = wbSyncMinUpdate
	} else {
		// gzip of random data barely compresses; tile one 1KB block to the target size.
		block := synthPayload(1_024)
		raw = make([]byte, size)
		for i := 0; i < size; i += len(block) {
			copy(raw[i:], block)
		}
	}
	comp, gerr := gzipBytes(raw)
	if gerr != nil {
		return gerr
	}
	b, err := u.whiteboardDC(id, plugnmeet.DataMsgBodyType_SCENE_UPDATE, string(scope), comp)
	if err != nil {
		return err
	}
	if perr := u.rawPublish(u.wbSubject(), b); perr != nil {
		return perr
	}
	u.p.Rep.RecordSend("fullstate", id, time.Now(), u.fanExpected())
	return nil
}

// pageChange: PAGE_CHANGE, message = page number string (pnm-client
// handleRequests.ts broadcastCurrentPageNumber 10-22).
func (u *runner) pageChange() error {
	id := uuidV4()
	b, err := u.whiteboardDC(id, plugnmeet.DataMsgBodyType_PAGE_CHANGE, "1", nil)
	if err != nil {
		return err
	}
	if perr := u.rawPublish(u.wbSubject(), b); perr != nil {
		return perr
	}
	u.p.Rep.RecordSend("pageChange", id, time.Now(), u.fanExpected())
	return nil
}

// wbFileChange is the JSON .message of FILE_CHANGE (handleRequests.ts
// broadcastCurrentFileId 27-39).
type wbFileChange struct {
	FileId string `json:"fileId"`
	Page   int    `json:"page"`
}

// fileChange: FILE_CHANGE, message = {"fileId","page"} JSON
// (handleRequests.ts broadcastCurrentFileId 27-39).
func (u *runner) fileChange() error {
	id := uuidV4()
	fc, err := json.Marshal(wbFileChange{FileId: "default", Page: 1})
	if err != nil {
		return err
	}
	b, err := u.whiteboardDC(id, plugnmeet.DataMsgBodyType_FILE_CHANGE, string(fc), nil)
	if err != nil {
		return err
	}
	if perr := u.rawPublish(u.wbSubject(), b); perr != nil {
		return perr
	}
	u.p.Rep.RecordSend("fileChange", id, time.Now(), u.fanExpected())
	return nil
}

// pointerUpdate: POINTER_UPDATE on whiteboard.{roomId}, message = scope JSON,
// bin = small (≤128B) synthesized awareness blob (pnm-client
// WhiteboardController.ts handleAwarenessChange 1318-1341).
func (u *runner) pointerUpdate() error {
	id := uuidV4()
	scope, err := json.Marshal(wbScope{FileId: "default", Page: 1})
	if err != nil {
		return err
	}
	// HYBRID-SAFETY: the client decodes awareness bytes, so random corpus
	// corrupts state; send the minimal VALID awareness update for real users.
	bin := synthPayload(64)
	if u.anyRealUser() {
		bin = wbAwarenessMinUpdate
	}
	b, err := u.whiteboardDC(id, plugnmeet.DataMsgBodyType_POINTER_UPDATE, string(scope), bin)
	if err != nil {
		return err
	}
	if perr := u.rawPublish(u.wbSubject(), b); perr != nil {
		return perr
	}
	u.p.Rep.RecordSend("pointer", id, time.Now(), u.fanExpected())
	return nil
}

// wbSyncMinUpdate is the minimal VALID yjs empty-doc update (2 bytes, per the
// pnm-client's bundled yjs); safe for real browsers that apply it.
var wbSyncMinUpdate = []byte{0x00, 0x00}

// wbAwarenessMinUpdate is the minimal VALID y-protocols awareness update
// (pointer/POINTER_UPDATE payloads): one 0x00 byte, i.e. zero states —
// decodes cleanly as a no-op in applyAwarenessUpdate.
var wbAwarenessMinUpdate = []byte{0x00}

// wbDonorAppState mirrors the pnm-client WhiteboardController.ts
// buildInitialData / WhiteboardDataAsDonorData.appState fields
// (WhiteboardController.ts 1030-1040).
type wbDonorAppState struct {
	Height              int     `json:"height"`
	Width               int     `json:"width"`
	ScrollX             int     `json:"scrollX"`
	ScrollY             int     `json:"scrollY"`
	ZoomValue           float64 `json:"zoomValue"`
	Theme               string  `json:"theme"`
	ViewBackgroundColor string  `json:"viewBackgroundColor"`
	ZenModeEnabled      bool    `json:"zenModeEnabled"`
	GridSize            *int    `json:"gridSize"`
}

// wbDonorInitialData mirrors WhiteboardDataAsDonorData (whiteboard.ts 76-81);
// CurrentOfficeFilePages is a STRING on the wire.
type wbDonorInitialData struct {
	CurrentPageNumber             int             `json:"currentPageNumber"`
	CurrentWhiteboardOfficeFileId string          `json:"currentWhiteboardOfficeFileId"`
	CurrentOfficeFilePages        string          `json:"currentOfficeFilePages"`
	AppState                      wbDonorAppState `json:"appState"`
}

// wbBootstrapMsg is the WHITEBOARD_SYNC_RESPONSE .message in the bootstrap
// donor form: buildScopeMessage({initial_data: JSON.stringify(initialData),
// gzip: true}) — WhiteboardController.ts 1013-1048 (sendFullInitialData
// branch), backfilled with the corpus fileId/page.
type wbBootstrapMsg struct {
	FileId      string `json:"fileId"`
	Page        int    `json:"page"`
	Gzip        bool   `json:"gzip"`
	InitialData string `json:"initial_data"`
}

// wbStateVectorMsg is the WHITEBOARD_SYNC_RESPONSE .message in the scope-
// response form: {fileId, page, stateVector: uint8ToBase64(...)} →
// WhiteboardController.ts 1077 (the requester echoes them back as-is).
type wbStateVectorMsg struct {
	FileId      string `json:"fileId"`
	Page        int    `json:"page"`
	StateVector string `json:"stateVector"`
}

// whiteboardSyncRequest: join-time WHITEBOARD_SYNC_REQUEST via the
// private-delivery path; .message = scope JSON, .bin = RAW empty state vector.
// Not gated by --disable (join-time protocol).
func (u *runner) whiteboardSyncRequest(to string) error {
	if to == "" {
		return nil
	}
	id := uuidV4()
	scope, err := json.Marshal(wbScope{FileId: "default_1", Page: 1})
	if err != nil {
		return err
	}
	b, err := proto.Marshal(&plugnmeet.DataChannelMessage{
		Id:         id,
		FromUserId: u.cfg.UserID,
		Type:       plugnmeet.DataMsgBodyType_WHITEBOARD_SYNC_REQUEST,
		Message:    string(scope),
		BinMessage: []byte{},
	})
	if err != nil {
		return err
	}
	if serr := u.sendPrivate(to, "WHITEBOARD_MSG", b); serr != nil {
		return serr
	}
	// Honest accounting: a foreign target never receipts into this process's
	// collector, so it is outside the fan/RTT registry; direct RTT is kept
	// via the real-user-sync pending map.
	if u.isSyntheticID(to) {
		u.p.Rep.RecordSend("wbSyncReq", id, time.Now(), 1)
	} else {
		u.p.Rep.RecordForeignSend("wbSyncReq", to, id, time.Now(), "wbSyncReq")
	}
	return nil
}

// wbSyncResponse sends the WHITEBOARD_SYNC_RESPONSE (WhiteboardController.ts
// 1003-1077): bootstrap donor form when the request scope is empty/unparseable,
// scope-response echo form otherwise (every parseable scope is treated as the
// active page — synthetic peers never switch pages).
func (u *runner) wbSyncResponse(to, echoId string, scope wbScope) error {
	if to == "" {
		return nil
	}
	var message string
	var bin []byte
	var err error
	if scope.FileId == "" || scope.Page == 0 {
		donor, derr := json.Marshal(wbDonorInitialData{
			CurrentPageNumber:             1,
			CurrentWhiteboardOfficeFileId: "default_1",
			CurrentOfficeFilePages:        "",
			AppState: wbDonorAppState{
				Height: 1080, Width: 1920, ScrollX: 0, ScrollY: 0,
				ZoomValue: 1, Theme: "light", ViewBackgroundColor: "#ffffff",
				ZenModeEnabled: false, GridSize: nil,
			},
		})
		if derr != nil {
			return derr
		}
		m, merr := json.Marshal(wbBootstrapMsg{
			FileId:      "default_1",
			Page:        1,
			Gzip:        true,
			InitialData: string(donor),
		})
		if merr != nil {
			return merr
		}
		message = string(m)
		if bin, err = gzipBytes(wbSyncMinUpdate); err != nil {
			return err
		}
	} else {
		m, merr := json.Marshal(wbStateVectorMsg{
			FileId:      scope.FileId,
			Page:        scope.Page,
			StateVector: "", // base64 of the empty state vector
		})
		if merr != nil {
			return merr
		}
		message = string(m)
		bin = wbSyncMinUpdate // RAW
	}
	b, err := proto.Marshal(&plugnmeet.DataChannelMessage{
		Id:         echoId,
		FromUserId: u.cfg.UserID,
		Type:       plugnmeet.DataMsgBodyType_WHITEBOARD_SYNC_RESPONSE,
		Message:    message,
		BinMessage: bin,
	})
	if err != nil {
		return err
	}
	if serr := u.sendPrivate(to, "WHITEBOARD_MSG", b); serr != nil {
		return serr
	}
	// Foreign requester: this response can never be receipted by us.
	if u.isSyntheticID(to) {
		u.p.Rep.RecordSend("wbSyncRes", echoId, time.Now(), 1)
	} else {
		u.p.Rep.RecordForeignSend("wbSyncRes", to, echoId, time.Now(), "")
	}
	return nil
}

// onWhiteboardSyncRequest: RTT receipt + jittered responder goroutine
// (immediate at Idx 0, else 250-800ms warmup — mirrors the pnm-client responder).
func (u *runner) onWhiteboardSyncRequest(inner *plugnmeet.DataChannelMessage) {
	r := u.p.Rep
	r.RecordRecv("wbSyncReq", inner.GetId(), time.Now())
	requester := inner.GetFromUserId()
	if requester == "" {
		return
	}
	// parse the request scope (empty/unparseable → bootstrap donor form)
	var scope wbScope
	_ = json.Unmarshal([]byte(inner.GetMessage()), &scope)
	go func(req, echoId string, aScope wbScope) {
		if u.p.Idx != 0 {
			time.Sleep(warmup(250*time.Millisecond, 800*time.Millisecond))
		}
		if err := u.wbSyncResponse(req, echoId, aScope); err != nil {
			u.p.Rep.AddError("send.wbSyncResponse")
		}
	}(requester, inner.GetId(), scope)
}

// onWhiteboardSyncResponse: RTT receipt only; ids echo our request ids, real
// client responses are accepted without payload parsing.
func (u *runner) onWhiteboardSyncResponse(inner *plugnmeet.DataChannelMessage) {
	u.p.Rep.RecordRecv("wbSyncRes", inner.GetId(), time.Now())
	// foreign direct-RTT: the responder id links a pending wbSyncReq entry.
	if from := inner.GetFromUserId(); from != "" && !u.isSyntheticID(from) {
		u.p.Rep.RecordForeignRecv("wbSyncRes", "wbSyncReq", from, time.Now())
	}
}

// onWhiteboardSub: whiteboard.{roomId} — SCENE_UPDATE split by scope gzip
// (plain scene updates vs full-state gzip broadcasts; both carry an Id used
// as the delivery-RTT correlation key).
func (u *runner) onWhiteboardSub(m *nats.Msg) {
	r := u.p.Rep
	r.CoreRxAdd(1)
	dc := new(plugnmeet.DataChannelMessage)
	if err := proto.Unmarshal(m.Data, dc); err != nil {
		u.errNote("whiteboard.decode", err)
		return
	}
	r.Received(dc.GetType().String())
	r.FirstSeen("type."+dc.GetType().String(), m.Subject)
	var kind string
	switch dc.GetType() {
	case plugnmeet.DataMsgBodyType_PAGE_CHANGE:
		// message = page number string, not scope JSON
		kind = "pageChange"
	case plugnmeet.DataMsgBodyType_FILE_CHANGE:
		kind = "fileChange"
	case plugnmeet.DataMsgBodyType_UPDATE_CURRENT_OFFICE_FILE_PAGES:
		// message is not scope JSON (pnm-client HandleWhiteboard.ts
		// dispatches updateCurrentOfficeFilePages(payload.message) raw;
		// real clients send it with an empty Message)
		kind = "officePages"
	case plugnmeet.DataMsgBodyType_WHITEBOARD_APP_STATE_CHANGE:
		// real-user app-state changes carry the same scope JSON; classify them
		// instead of falling through to the "scene" label
		kind = "appState"
	case plugnmeet.DataMsgBodyType_POINTER_UPDATE:
		kind = "pointer"
	default:
		// message = scope JSON {fileId,page,gzip}
		var scope wbScope
		if err := json.Unmarshal([]byte(dc.GetMessage()), &scope); err != nil {
			u.errNote("whiteboard.scope.decode", err)
			return
		}
		kind = "scene"
		if scope.Gzip {
			kind = "fullstate"
		}
	}
	r.RecordRecv(kind, dc.GetId(), time.Now())
}

// whiteboardLoop: presenter-only burst loop — 30-60s bursts with 20-45s idle
// gaps, sceneUpdate every 2s, one pageChange/fileChange pair per burst, and a
// cross-burst fullStateBroadcast deadline of jitter(2m).
func (u *runner) whiteboardLoop(ctx context.Context) {
	nextFull := time.Now().Add(jitter(2 * time.Minute))
	for {
		idle := u.ctxTimer(ctx, warmup(20*time.Second, 45*time.Second))
		select {
		case <-ctx.Done():
			return
		case <-idle:
		}
		// PRESENTER TRUTH: idx==0 merely designates this loop as the
		// presenter-actor; sends only fire while the server actually says so
		// (idle keep-running so a re-promote resumes actions).
		if !u.isPresenter() {
			continue
		}
		burstEnd := time.Now().Add(warmup(30*time.Second, 60*time.Second))
		sceneTick := time.NewTicker(2 * time.Second)
		start := time.Now()
		pageDone, fileDone := false, false
		pageAt := warmup(5*time.Second, 50*time.Second)
		fileAt := warmup(5*time.Second, 50*time.Second)
		for time.Now().Before(burstEnd) {
			select {
			case <-ctx.Done():
				sceneTick.Stop()
				return
			case <-sceneTick.C:
				// re-check at the moment the fullState deadline fires (and
				// skip the whole tick when demoted mid-burst).
				if !u.isPresenter() {
					continue
				}
				if now := time.Now(); !now.Before(nextFull) {
					if err := u.fullStateBroadcast(); err != nil {
						u.p.Rep.AddError("send.fullStateBroadcast")
					}
					nextFull = now.Add(jitter(2 * time.Minute))
				}
				size := 200 + rand.Intn(1801)
				if rand.Intn(10) == 0 {
					size = 10_000 + rand.Intn(40_001)
				}
				if err := u.sceneUpdate(size); err != nil {
					u.p.Rep.AddError("send.sceneUpdate")
				}
			default:
				elapsed := time.Since(start)
				if !pageDone && elapsed >= pageAt && rand.Intn(2) == 0 {
					pageDone = true
					_ = u.pageChange()
				}
				if !fileDone && elapsed >= fileAt && rand.Intn(2) == 0 {
					fileDone = true
					_ = u.fileChange()
				}
				time.Sleep(200 * time.Millisecond)
			}
		}
		sceneTick.Stop()
	}
}

// pointerLoop: viewer loop — watching toggles on for 20-40s then off 20-40s;
// while watching pointerUpdate fires every ~1.5s.
func (u *runner) pointerLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-u.ctxTimer(ctx, warmup(20*time.Second, 40*time.Second)):
		}
		// watching phase
		end := time.Now().Add(warmup(20*time.Second, 40*time.Second))
		t := time.NewTicker(1500 * time.Millisecond)
		defer t.Stop()
		for time.Now().Before(end) {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := u.pointerUpdate(); err != nil {
					u.p.Rep.AddError("send.pointerUpdate")
				}
			}
		}
	}
}

// ctxTimer returns a channel that fires after d (select-able sleep helper).
func (u *runner) ctxTimer(ctx context.Context, d time.Duration) chan time.Time {
	_ = ctx
	ch := make(chan time.Time, 1)
	time.AfterFunc(d, func() { ch <- time.Now() })
	return ch
}
