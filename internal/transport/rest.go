package transport

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"

	"github.com/mynaparrot/plugnmeet-protocol/plugnmeet"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Client struct {
	ServerURL string
	ApiKey    string
	ApiSecret string
	HTTP      *http.Client
}

func NewClient(serverURL, apiKey, apiSecret string) *Client {
	return &Client{
		ServerURL: serverURL,
		ApiKey:    apiKey,
		ApiSecret: apiSecret,
		HTTP:      &http.Client{},
	}
}

func (c *Client) post(path string, body []byte, ct string, hdr map[string]string) (*http.Response, error) {
	req, err := http.NewRequest("POST", c.ServerURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", ct)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	return c.HTTP.Do(req)
}

// signedPost sends an /auth/* request with HMAC-SHA256 signature over the raw body.
func (c *Client) signedPost(path string, body []byte) ([]byte, error) {
	mac := hmac.New(sha256.New, []byte(c.ApiSecret))
	mac.Write(body)
	signature := hex.EncodeToString(mac.Sum(nil))

	resp, err := c.post(path, body, "application/json", map[string]string{
		"API-KEY":        c.ApiKey,
		"HASH-SIGNATURE": signature,
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: HTTP %d: %s", path, resp.StatusCode, data)
	}
	return data, nil
}

// IsRoomActive checks whether room_id is active.
func (c *Client) IsRoomActive(roomID string) (bool, error) {
	body, err := protojson.Marshal(&plugnmeet.IsRoomActiveReq{RoomId: roomID})
	if err != nil {
		return false, err
	}
	data, err := c.signedPost("/auth/room/isRoomActive", body)
	if err != nil {
		return false, err
	}
	res := new(plugnmeet.IsRoomActiveRes)
	if err := protojson.Unmarshal(data, res); err != nil {
		return false, err
	}
	return res.GetIsActive(), nil
}

// DefaultCreateRoomReq builds a minimal typed room-create request.
func DefaultCreateRoomReq(roomID string) *plugnmeet.CreateRoomReq {
	return &plugnmeet.CreateRoomReq{
		RoomId: roomID,
		Metadata: &plugnmeet.RoomMetadata{
			RoomTitle: "spike room",
			RoomFeatures: &plugnmeet.RoomCreateFeatures{
				WaitingRoomFeatures: &plugnmeet.WaitingRoomFeatures{},
				ChatFeatures: &plugnmeet.ChatFeatures{
					IsAllow: true,
				},
				SharedNotePadFeatures: &plugnmeet.SharedNotePadFeatures{
					IsAllow: true,
				},
				WhiteboardFeatures: &plugnmeet.WhiteboardFeatures{
					IsAllow: true,
				},
				AllowViewOtherUsersList: true,
				AllowViewOtherWebcams:   true,
				AllowRaiseHand:          new(true),
				AllowReactions:          new(true),
			},
		},
	}
}

// CreateRoom creates the room (/auth/room/create)
func (c *Client) CreateRoom(roomID string) error {
	body, err := protojson.Marshal(DefaultCreateRoomReq(roomID))
	if err != nil {
		return err
	}
	data, err := c.signedPost("/auth/room/create", body)
	if err != nil {
		return err
	}
	res := &plugnmeet.CreateRoomRes{}
	if err := protojson.Unmarshal(data, res); err != nil {
		return err
	}
	if !res.GetStatus() {
		return fmt.Errorf("room/create failed: %s", res.GetMsg())
	}
	return nil
}

// GetJoinToken returns the plugNmeet access token (/auth/room/getJoinToken).
// Request body is GenerateTokenReq; the server replies with GenerateTokenRes.
func (c *Client) GetJoinToken(roomID, name, userID string, isAdmin bool) (string, error) {
	body, err := protojson.Marshal(&plugnmeet.GenerateTokenReq{
		RoomId: roomID,
		UserInfo: &plugnmeet.UserInfo{
			Name:    name,
			UserId:  userID,
			IsAdmin: isAdmin,
		},
	})
	if err != nil {
		return "", err
	}
	data, err := c.signedPost("/auth/room/getJoinToken", body)
	if err != nil {
		return "", err
	}
	res := &plugnmeet.GenerateTokenRes{}
	if err := protojson.Unmarshal(data, res); err != nil {
		return "", err
	}
	if !res.GetStatus() || res.GetToken() == "" {
		return "", fmt.Errorf("getJoinToken failed: %s", res.GetMsg())
	}
	return res.GetToken(), nil
}

// VerifyToken POSTs the access token to /api/verifyToken (protobuf) and the
// parsed VerifyTokenRes reply.
func (c *Client) VerifyToken(token string) (*plugnmeet.VerifyTokenRes, error) {
	body, err := proto.Marshal(&plugnmeet.VerifyTokenReq{IsProduction: proto.Bool(false)})
	if err != nil {
		return nil, err
	}
	resp, err := c.post("/api/verifyToken", body, "application/protobuf", map[string]string{
		"Authorization": token,
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("verifyToken: HTTP %d", resp.StatusCode)
	}
	res := &plugnmeet.VerifyTokenRes{}
	if err := proto.Unmarshal(data, res); err != nil {
		return nil, err
	}
	return res, nil
}
