package user

import (
	"encoding/json"
	"math/rand"
	"time"

	"github.com/mynaparrot/plugnmeet-protocol/plugnmeet"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Chat actions mirroring the pnm-client: public chat = RAW ChatMessage binary
// on chat.{roomId} (ConnectNats.ts deliverChatMessage 560-606); private chat =
// REQ_PRIVATE_DATA_DELIVERY with PrivateDataDelivery JSON header
// (sendPrivateData 504-517); donor reply = chunks of ≤50 ChatMessages as a
// JSON array privately delivered (HandleDataMessage.ts 215-235).

// maxChatHistory bounds the donor reply history (200 entries).
const maxChatHistory = 200

// publicChat: RAW ChatMessage binary on chat.{roomId} (ConnectNats.ts 560-606).
func (u *runner) publicChat() error {
	id := uuidV4()
	b, err := proto.Marshal(&plugnmeet.ChatMessage{
		Id:         id,
		FromName:   u.p.Name,
		FromUserId: u.cfg.UserID,
		SentAt:     time.Now().UnixMilli(),
		Message:    publicChatCorpus[rand.Intn(len(publicChatCorpus))],
		FromAdmin:  u.p.IsAdmin,
		IsPrivate:  false,
	})
	if err != nil {
		return err
	}
	if perr := u.rawPublish(u.chatSubject(), b); perr != nil {
		return perr
	}
	u.p.Rep.RecordSend("chat", id, time.Now(), u.fanExpected())
	return nil
}

// privateChat: REQ_PRIVATE_DATA_DELIVERY "CHAT" with the inner ChatMessage binary.
func (u *runner) privateChat() error {
	peer := u.pickPeer()
	if peer == "" {
		return nil // solo room: no private chat possible
	}
	id := uuidV4()
	b, err := proto.Marshal(&plugnmeet.ChatMessage{
		Id:         id,
		FromName:   u.p.Name,
		FromUserId: u.cfg.UserID,
		SentAt:     time.Now().UnixMilli(),
		Message:    publicChatCorpus[rand.Intn(len(publicChatCorpus))],
		ToUserId:   new(peer),
		IsPrivate:  true,
		FromAdmin:  u.p.IsAdmin,
	})
	if err != nil {
		return err
	}
	if serr := u.sendPrivate(peer, "CHAT", b); serr != nil {
		return serr
	}
	// A foreign peer can never receipt into this process's collector.
	if u.isSyntheticID(peer) {
		u.p.Rep.RecordSend("pchat", id, time.Now(), 1)
	} else {
		u.p.Rep.RecordForeignSend("pchat", peer, id, time.Now(), "")
	}
	return nil
}

// chatSyncChunkSize matches the pnm-client CHAT_SYNC_CHUNK_SIZE.
const chatSyncChunkSize = 50

// donorReply: RES_PUBLIC_CHAT_DATA delivered privately in ≤50-message chunks
// (HandleDataMessage.ts 215-235).
func (u *runner) donorReply(to string, history []*plugnmeet.ChatMessage) error {
	if to == "" {
		return nil
	}
	for len(history) > 0 {
		n := chatSyncChunkSize
		if len(history) < n {
			n = len(history)
		}
		chunk := history[:n]
		history = history[n:]
		items := make([]json.RawMessage, 0, n)
		for _, m := range chunk {
			j, err := protojson.Marshal(m)
			if err != nil {
				return err
			}
			items = append(items, j)
		}
		arr, jerr := json.Marshal(items)
		if jerr != nil {
			return jerr
		}
		b, err := proto.Marshal(&plugnmeet.DataChannelMessage{
			Id:         uuidV4(),
			FromUserId: u.cfg.UserID,
			Type:       plugnmeet.DataMsgBodyType_RES_PUBLIC_CHAT_DATA,
			Message:    string(arr),
		})
		if err != nil {
			return err
		}
		if serr := u.sendPrivate(to, plugnmeet.DataMsgBodyType_RES_PUBLIC_CHAT_DATA.String(), b); serr != nil {
			return serr
		}
	}
	return nil
}

// appendChat appends to the donor history bounded to maxChatHistory entries.
func (u *runner) appendChat(cm *plugnmeet.ChatMessage) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.chatHist = append(u.chatHist, cm)
	if len(u.chatHist) > maxChatHistory {
		u.chatHist = u.chatHist[len(u.chatHist)-maxChatHistory:]
	}
}

// chatHistory returns a snapshot copy of the donor reply history.
func (u *runner) chatHistory() []*plugnmeet.ChatMessage {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make([]*plugnmeet.ChatMessage, len(u.chatHist))
	copy(out, u.chatHist)
	return out
}

// onChatSub is the chat.{roomId} core-subscription handler: census +
// delivery-RTT receipt + bounded history for donor replies.
func (u *runner) onChatSub(m *nats.Msg) {
	r := u.p.Rep
	r.CoreRxAdd(1)
	if !u.chatSeen {
		u.chatSeen = true
		r.FirstSeen("chat", m.Subject)
	}
	cm := new(plugnmeet.ChatMessage)
	if err := proto.Unmarshal(m.Data, cm); err != nil {
		u.errNote("chat.decode", err)
		return
	}
	r.Received("ChatMessage")
	r.RecordRecv("chat", cm.GetId(), time.Now())
	u.appendChat(cm)
}

// onPrivateDelivery handles DELIVERY_PRIVATE_DATA (inner payloads arrive as a
// DataChannelMessage inside binMsg; msg = PrivateDataDelivery protojson).
// Notepad sync steps live in notepad.go; whiteboard sync steps in
// whiteboard.go.
func (u *runner) onPrivateDelivery(res *plugnmeet.NatsMsgServerToClient) {
	r := u.p.Rep
	hdr := new(plugnmeet.PrivateDataDelivery)
	if err := protojson.Unmarshal([]byte(res.GetMsg()), hdr); err != nil {
		u.errNote("delivery.decode", err)
		return
	}
	// Private-chat receive: header kind "CHAT" (the same string privateChat
	// sends) carries a ChatMessage proto as the inner bin, not a
	// DataChannelMessage.
	if hdr.GetType() == "CHAT" {
		cm := new(plugnmeet.ChatMessage)
		if len(res.GetBinMsg()) > 0 {
			if err := proto.Unmarshal(res.GetBinMsg(), cm); err != nil {
				u.errNote("delivery.inner.decode", err)
				return
			}
		}
		r.Received("ChatMessage")
		r.RecordRecv("pchat", cm.GetId(), time.Now())
		return
	}
	inner := new(plugnmeet.DataChannelMessage)
	if len(res.GetBinMsg()) > 0 {
		if err := proto.Unmarshal(res.GetBinMsg(), inner); err != nil {
			u.errNote("delivery.inner.decode", err)
			return
		}
	}
	// Census by inner DataMsgBodyType name.
	r.Received("DELIVERY." + inner.GetType().String())

	switch inner.GetType() {
	case plugnmeet.DataMsgBodyType_REQ_PUBLIC_CHAT_DATA:
		// requester id falls back to the private header sender name.
		requester := inner.GetFromUserId()
		if requester == "" {
			requester = hdr.GetType() // never a real id; donorReply guards ""
		}
		r.Sent(plugnmeet.DataMsgBodyType_RES_PUBLIC_CHAT_DATA.String())
		if requester != "" {
			_ = u.donorReply(requester, u.chatHistory())
		}

	case plugnmeet.DataMsgBodyType_NOTEPAD_SYNC_REQUEST:
		u.onNotepadSyncRequest(inner)

	case plugnmeet.DataMsgBodyType_NOTEPAD_SYNC_RESPONSE:
		u.onNotepadSyncResponse(inner)

	case plugnmeet.DataMsgBodyType_WHITEBOARD_SYNC_REQUEST:
		u.onWhiteboardSyncRequest(inner)

	case plugnmeet.DataMsgBodyType_WHITEBOARD_SYNC_RESPONSE:
		u.onWhiteboardSyncResponse(inner)

	case plugnmeet.DataMsgBodyType_RES_PUBLIC_CHAT_DATA:
		// Message is a JSON array of ChatMessage objects — count cheaply.
		var arr []json.RawMessage
		if perr := json.Unmarshal([]byte(inner.GetMessage()), &arr); perr != nil {
			u.errNote("resChatData.decode", perr)
		}
		r.Received(plugnmeet.DataMsgBodyType_RES_PUBLIC_CHAT_DATA.String())
	}
}
