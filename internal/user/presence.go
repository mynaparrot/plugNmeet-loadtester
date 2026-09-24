package user

import (
	"encoding/json"
	"math/rand"
	"time"

	"github.com/mynaparrot/plugnmeet-protocol/plugnmeet"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Reactions, hand raise/lower, visibility, quality and analytics, mirroring
// the pnm-client (reactions.tsx 58-101, useWatchVisibilityChange.tsx 70-92,
// ConnectNats.ts sendAnalyticsData 855-878).

// reactionPayload matches the pnm-client reaction JSON (reactions.tsx 58-65);
// no protocol type exists.
type reactionPayload struct {
	Id         string `json:"id"`
	Emoji      string `json:"emoji"`
	FromUserId string `json:"fromUserId"`
	FromName   string `json:"fromName"`
	CreatedAt  int64  `json:"createdAt"`
}

// reaction: DataChannelMessage{Type: REACTION, Message: JSON} RAW on
// dataChannel.{roomId} + USER_REACTION analytics.
func (u *runner) reaction() error {
	id := uuidV4()
	emojis := []rune(reactionEmojiSet)
	rp := reactionPayload{
		Id:         id,
		Emoji:      string(emojis[rand.Intn(len(emojis))]),
		FromUserId: u.cfg.UserID,
		FromName:   u.p.Name,
		CreatedAt:  time.Now().UnixMilli(),
	}
	msg, err := json.Marshal(rp)
	if err != nil {
		return err
	}
	b, err := proto.Marshal(&plugnmeet.DataChannelMessage{
		Id:      id,
		Type:    plugnmeet.DataMsgBodyType_REACTION,
		Message: string(msg),
	})
	if err != nil {
		return err
	}
	if perr := u.rawPublish(u.dataChannelSubject(), b); perr != nil {
		return perr
	}
	u.p.Rep.RecordSend("reaction", id, time.Now(), u.fanExpected())
	_ = u.sendAnalytics(plugnmeet.AnalyticsEvents_ANALYTICS_EVENT_USER_REACTION)
	return nil
}

// raiseHand fires the REQ_RAISE_HAND envelope once, then lowerHand part of the
// pair after 30-60s (once per user).
func (u *runner) raiseHand() error {
	serr := u.send(u.coreWorkerSubject(), plugnmeet.NatsMsgClientToServerEvents_REQ_RAISE_HAND, u.p.Name+" raised their hand", nil)
	if serr == nil {
		u.p.Rep.Sent(plugnmeet.NatsMsgClientToServerEvents_REQ_RAISE_HAND.String())
	}
	return serr
}

func (u *runner) lowerHand() error {
	serr := u.send(u.coreWorkerSubject(), plugnmeet.NatsMsgClientToServerEvents_REQ_LOWER_HAND, u.p.Name+" lowered their hand", nil)
	if serr == nil {
		u.p.Rep.Sent(plugnmeet.NatsMsgClientToServerEvents_REQ_LOWER_HAND.String())
	}
	return serr
}

// visibilityDataMsg produces the RAW dataChannel.{roomId} payload for the
// given message string.
func (u *runner) visibilityDataMsg(id, val string) ([]byte, error) {
	return proto.Marshal(&plugnmeet.DataChannelMessage{
		Id:      id,
		Type:    plugnmeet.DataMsgBodyType_USER_VISIBILITY_CHANGE,
		Message: val,
	})
}

// visibility toggles hidden/visible (a pair across consecutive invocations)
// + USER_INTERFACE_VISIBILITY analytics (useWatchVisibilityChange.tsx 70-92).
func (u *runner) visibility() error {
	u.mu.Lock()
	u.visHidden = !u.visHidden
	hidden := u.visHidden
	u.mu.Unlock()
	val := "visible"
	if hidden {
		val = "hidden"
	}
	b, err := u.visibilityDataMsg(uuidV4(), val)
	if err != nil {
		return err
	}
	if perr := u.rawPublish(u.dataChannelSubject(), b); perr != nil {
		return perr
	}
	return u.sendAnalytics(plugnmeet.AnalyticsEvents_ANALYTICS_EVENT_USER_INTERFACE_VISIBILITY)
}

// connQuality: USER_CONNECTION_QUALITY_CHANGE datachannel msg "excellent".
func (u *runner) connQuality() error {
	b, err := proto.Marshal(&plugnmeet.DataChannelMessage{
		Id:      uuidV4(),
		Type:    plugnmeet.DataMsgBodyType_USER_CONNECTION_QUALITY_CHANGE,
		Message: "excellent",
	})
	if err != nil {
		return err
	}
	return u.rawPublish(u.dataChannelSubject(), b)
}

// sendAnalytics pushes the fire-and-forget analytics event: envelope
// PUSH_ANALYTICS_DATA on systemCoreWorker.{roomId}.{userId}, msg = protojson
// AnalyticsDataMsg (pnm-client ConnectNats.ts sendAnalyticsData 855-878).
func (u *runner) sendAnalytics(ev plugnmeet.AnalyticsEvents) error {
	msg, err := protojson.Marshal(&plugnmeet.AnalyticsDataMsg{
		EventType: plugnmeet.AnalyticsEventType_ANALYTICS_EVENT_TYPE_USER,
		EventName: ev,
		RoomId:    u.cfg.RoomID,
		Time:      time.Now().UnixMilli(),
		UserId:    new(u.cfg.UserID),
	})
	if err != nil {
		return err
	}
	serr := u.send(u.coreWorkerSubject(), plugnmeet.NatsMsgClientToServerEvents_PUSH_ANALYTICS_DATA, string(msg), nil)
	if serr == nil {
		u.p.Rep.Sent(plugnmeet.NatsMsgClientToServerEvents_PUSH_ANALYTICS_DATA.String())
	}
	return serr
}
