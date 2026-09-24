package user

import (
	"encoding/base64"
	"encoding/json"
	"math/rand"
	"time"

	"github.com/mynaparrot/plugnmeet-protocol/plugnmeet"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Notepad actions mirroring the pnm-client NotepadController.ts (203-243,
// 560-655): join-time session-data fetch ("snapshot"+"snapshot~d"),
// NOTEPAD_SYNC_REQUEST state vector, responses ALWAYS gzip (non-presenters
// with 250-800ms jitter), presenter NOTEPAD_UPDATE; session-data fetches are
// SESSION_DATA_FETCH_REQUEST with a protojson SessionDataHeader.

// notepadYjsUpdate returns the yjs body for NOTEPAD_UPDATE: the minimal
// VALID empty-doc update when hybrid-safety demands it (see anyRealUser),
// otherwise the random corpus.
func (u *runner) notepadYjsUpdate() []byte {
	if u.anyRealUser() {
		return wbSyncMinUpdate
	}
	return synthPayload(1_024)
}

// notepadUpdate: NOTEPAD_UPDATE raw bytes on dataChannel.{roomId}
// (NotepadController.ts 200-205). Idle-skip when not the true presenter.
func (u *runner) notepadUpdate() error {
	if !u.isPresenter() {
		return nil
	}
	id := uuidV4()
	b, err := proto.Marshal(&plugnmeet.DataChannelMessage{
		Id:         id,
		FromUserId: u.cfg.UserID,
		Type:       plugnmeet.DataMsgBodyType_NOTEPAD_UPDATE,
		// HYBRID-SAFETY: real browsers apply this via Y.applyUpdate; the random
		// corpus is only valid load for a pure-synthetic room, so minimal VALID
		// yjs update ({0x00,0x00}) whenever a real user is present.
		BinMessage: u.notepadYjsUpdate(),
	})
	if err != nil {
		return err
	}
	if perr := u.rawPublish(u.dataChannelSubject(), b); perr != nil {
		return perr
	}
	u.p.Rep.RecordSend("notepadUpdate", id, time.Now(), u.fanExpected())
	return nil
}

// notepadSyncRequest: NOTEPAD_SYNC_REQUEST to one peer; the state vector goes
// in BinMessage RAW, Message unset (NotepadController.ts sendSyncRequest 505-545).
func (u *runner) notepadSyncRequest(to string) error {
	if to == "" {
		return nil
	}
	id := uuidV4()
	b, err := proto.Marshal(&plugnmeet.DataChannelMessage{
		Id:         id,
		FromUserId: u.cfg.UserID,
		Type:       plugnmeet.DataMsgBodyType_NOTEPAD_SYNC_REQUEST,
		BinMessage: synthPayload(64),
	})
	if err != nil {
		return err
	}
	if serr := u.sendPrivate(to, plugnmeet.DataMsgBodyType_NOTEPAD_SYNC_REQUEST.String(), b); serr != nil {
		return serr
	}
	// Honest accounting: a foreign target never receipts into this process's
	// collector, so a correlated foreign→synthetic response instead resolves
	// a pending direct-RTT entry.
	if u.isSyntheticID(to) {
		u.p.Rep.RecordSend("notepadSyncReq", id, time.Now(), 1)
	} else {
		u.p.Rep.RecordForeignSend("notepadSyncReq", to, id, time.Now(), "notepadSyncReq")
	}
	return nil
}

// notepadSyncResponse: NOTEPAD_SYNC_RESPONSE to the requestor
// (NotepadController.ts handleSyncRequest 575-635): BinMessage ALWAYS
// gzip-compressed, Message = {"stateVector": base64}. echoId is the inner
// DataChannel id of the request being answered (same echo convention as
// whiteboard wbSyncResponse — leaves a same-fit correlation key for foreign
// requesters and this process's notepadSyncRes registry accounting).
func (u *runner) notepadSyncResponse(to, echoId string) error {
	if to == "" || echoId == "" {
		return nil
	}
	id := echoId
	// HYBRID-SAFETY: gzip the minimal VALID yjs update for real requesters.
	missing := synthPayload(2_000 + rand.Intn(18_001))
	if !u.isSyntheticID(to) {
		missing = wbSyncMinUpdate
	}
	comp, gerr := gzipBytes(missing)
	if gerr != nil {
		return gerr
	}
	sv := base64.StdEncoding.EncodeToString(synthPayload(64))
	scope, err := json.Marshal(struct {
		StateVector string `json:"stateVector"`
	}{StateVector: sv})
	if err != nil {
		return err
	}
	b, err := proto.Marshal(&plugnmeet.DataChannelMessage{
		Id:         id,
		FromUserId: u.cfg.UserID,
		Type:       plugnmeet.DataMsgBodyType_NOTEPAD_SYNC_RESPONSE,
		Message:    string(scope),
		BinMessage: comp,
	})
	if err != nil {
		return err
	}
	if serr := u.sendPrivate(to, plugnmeet.DataMsgBodyType_NOTEPAD_SYNC_RESPONSE.String(), b); serr != nil {
		return serr
	}
	// Foreign requester: this response can never be receipted by us.
	if u.isSyntheticID(to) {
		u.p.Rep.RecordSend("notepadSyncRes", id, time.Now(), 1)
	} else {
		u.p.Rep.RecordForeignSend("notepadSyncRes", to, id, time.Now(), "")
	}
	return nil
}

// onNotepadSyncRequest: RTT receipt + jittered responder goroutine (never
// blocks onMsg).
func (u *runner) onNotepadSyncRequest(inner *plugnmeet.DataChannelMessage) {
	r := u.p.Rep
	r.RecordRecv("notepadSyncReq", inner.GetId(), time.Now())
	requester := inner.GetFromUserId()
	if requester == "" {
		return
	}
	go func(req, echoId string) {
		if u.p.Idx != 0 {
			time.Sleep(warmup(250*time.Millisecond, 800*time.Millisecond))
		}
		if err := u.notepadSyncResponse(req, echoId); err != nil {
			u.p.Rep.AddError("send.notepadSyncResponse")
		}
	}(requester, inner.GetId())
}

// onNotepadSyncResponse is the DELIVERY_PRIVATE_DATA NOTEPAD_SYNC_RESPONSE
// step: census + delivery-RTT receipt.
func (u *runner) onNotepadSyncResponse(inner *plugnmeet.DataChannelMessage) {
	r := u.p.Rep
	r.Received(plugnmeet.DataMsgBodyType_NOTEPAD_SYNC_RESPONSE.String())
	r.RecordRecv("notepadSyncRes", inner.GetId(), time.Now())
	// foreign direct-RTT: the responder id links a pending notepadSyncReq entry.
	if from := inner.GetFromUserId(); from != "" && !u.isSyntheticID(from) {
		r.RecordForeignRecv("notepadSyncRes", "notepadSyncReq", from, time.Now())
	}
}

// onDataChannelSub: dataChannel.{roomId} — per-type census + RTT receipts for
// reaction / notepadUpdate; everything else is census-counted only.
func (u *runner) onDataChannelSub(m *nats.Msg) {
	r := u.p.Rep
	r.CoreRxAdd(1)
	dc := new(plugnmeet.DataChannelMessage)
	if err := proto.Unmarshal(m.Data, dc); err != nil {
		u.errNote("dataChannel.decode", err)
		return
	}
	r.Received(dc.GetType().String())
	r.FirstSeen("type."+dc.GetType().String(), m.Subject)
	now := time.Now()
	switch dc.GetType() {
	case plugnmeet.DataMsgBodyType_REACTION:
		r.RecordRecv("reaction", dc.GetId(), now)
	case plugnmeet.DataMsgBodyType_NOTEPAD_UPDATE:
		r.RecordRecv("notepadUpdate", dc.GetId(), now)
	}
}

// sessionDataFetchRequest: SESSION_DATA_FETCH_REQUEST with a protojson
// SessionDataHeader (NotepadController.ts 406-429 / WhiteboardController.ts
// 535-560 share the shape).
func (u *runner) sessionDataFetchRequest(dataType plugnmeet.SessionDataType, key string) error {
	msg, err := protojson.Marshal(&plugnmeet.SessionDataHeader{
		DataType: dataType,
		Key:      new(key),
	})
	if err != nil {
		return err
	}
	serr := u.send(u.jsWorkerSubject(), plugnmeet.NatsMsgClientToServerEvents_SESSION_DATA_FETCH_REQUEST, string(msg), nil)
	if serr == nil {
		u.p.Rep.Sent(plugnmeet.NatsMsgClientToServerEvents_SESSION_DATA_FETCH_REQUEST.String())
	}
	return serr
}
