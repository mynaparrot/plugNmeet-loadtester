// Package media drives the synthetic LiveKit media connections: silent audio
// plus a black video track at full packet rate, so the SFU's forwarding path
// is exercised exactly like real clients (silent/black carries full RTP rate
// while staying polite in hybrid runs).
package media

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/mynaparrot/plugnmeet-loadtester/internal/report"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

// Mode selects what a media connection publishes/subscribes.
type Mode string

const (
	ModeNone   Mode = "none"   // no media connection at all (default)
	ModeListen Mode = "listen" // subscribe only (no publishing)
	ModeAudio  Mode = "audio"  // publish silent audio + subscribe
	ModeVideo  Mode = "video"  // publish silent audio + black video + subscribe
)

// Role is a user's per-cohort media role; core = no LiveKit connection at
// all, every other role opens one connection.
type Role string

const (
	RoleCore   Role = "core"
	RoleListen Role = "listen"
	RoleAudio  Role = "audio"
	RoleVideo  Role = "video"
)

// Mode maps a user role to the LiveKit connection mode it runs; RoleCore
// maps to ModeNone (zero lksdk activity).
func (r Role) Mode() Mode {
	switch r {
	case RoleListen:
		return ModeListen
	case RoleAudio:
		return ModeAudio
	case RoleVideo:
		return ModeVideo
	default:
		return ModeNone
	}
}

// Validate reports whether m is a supported mode string.
func Validate(m Mode) bool {
	switch m {
	case ModeNone, ModeListen, ModeAudio, ModeVideo:
		return true
	}
	return false
}

const (
	audioInterval = 20 * time.Millisecond // one 20ms opus frame per tick
	statsInterval = 10 * time.Second      // getStats RTT sample period
	videoFPS      = 15                    // 15fps black video

	settleMin     = 500 * time.Millisecond // join → mic publish settle window
	settleMax     = 1500 * time.Millisecond
	camGapMin     = 2 * time.Second // mic → camera publish gap window
	camGapMax     = 4 * time.Second
	shutdownGrace = 10 * time.Second // graceful-shutdown window before hard cancel
)

// silentOpus: 3-byte opus packet (TOC 0xFC, zero-length frame) — silence at
// the full 20ms nominal packet rate.
var silentOpus = []byte{0xFC, 0xFF, 0xFE}

// Connection is one user's connection to the LiveKit media server: mode,
// parameters, the shared report collector, and the room handle once connected.
type Connection struct {
	Mode Mode
	URL  string
	// Token authorises this connection's join as one synthetic participant
	// (captured from RES_MEDIA_SERVER_DATA).
	Token string
	// RoomName is the plugNmeet room id — used for the collector's
	// media-live counter and metric scoping.
	RoomName string
	// Label is the connection's user identity (user id), carried on pubTrack
	// summary records.
	Label string

	Report *report.Collector
	Room   *lksdk.Room

	mu     sync.Mutex
	cancel context.CancelFunc // set on Start; used by Stop
	closed bool
}

// Start connects to its LiveKit room (blocking, context-aware — mediaJoin
// measures exactly that blocked join call); publishing is gated on lctx.
func (c *Connection) Start(ctx context.Context) error {
	lctx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.cancel = cancel
	c.mu.Unlock()

	cb := lksdk.NewRoomCallback()
	cb.OnTrackSubscribed = func(track *webrtc.TrackRemote, publication *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
		c.onTrackSubscribed(publication)
	}

	room := lksdk.NewRoom(cb)
	start := time.Now()
	err := room.JoinWithContextAndToken(lctx, c.URL, c.Token)
	elapsed := time.Since(start)
	if err != nil {
		cancel()
		return fmt.Errorf("livekit join: %w", err)
	}
	c.mu.Lock()
	c.Room = room
	c.mu.Unlock()
	if c.Report != nil {
		c.Report.Record("mediaJoin", elapsed)
		c.Report.MediaLiveInc(c.RoomName)
	}
	if perr := c.publish(lctx); perr != nil {
		fmt.Println("media publish:", perr)
	}
	go c.statsLoop(lctx)
	return nil
}

// statsLoop samples mediaRtt every statsInterval from the nominated+succeeded
// getStats candidate pairs of both peer connections (listen-only included).
func (c *Connection) statsLoop(lctx context.Context) {
	if c.Report == nil {
		return
	}
	tk := time.NewTicker(statsInterval)
	defer tk.Stop()
	for {
		select {
		case <-lctx.Done():
			return
		case <-tk.C:
			if c.isStopped() {
				return
			}
			room := c.room()
			if room == nil {
				return
			}
			if pc := room.LocalParticipant.GetPublisherPeerConnection(); pc != nil {
				c.recordPCRTT(pc.GetStats())
			}
			if pc := room.LocalParticipant.GetSubscriberPeerConnection(); pc != nil {
				c.recordPCRTT(pc.GetStats())
			}
		}
	}
}

// recordPCRTT extracts one mediaRtt sample per tick per PC from the nominated
// succeeded candidate pair.
func (c *Connection) recordPCRTT(report webrtc.StatsReport) {
	for _, s := range report {
		pair, ok := s.(webrtc.ICECandidatePairStats)
		if !ok || pair.Type != webrtc.StatsTypeCandidatePair ||
			!pair.Nominated || pair.State != webrtc.StatsICECandidatePairStateSucceeded {
			continue
		}
		if rtt := pair.CurrentRoundTripTime; rtt > 0 {
			c.Report.Record("mediaRtt", time.Duration(rtt*float64(time.Second)))
		}
		break // at most one nominated pair per PC: one sample per tick
	}
}

// Stop shuts the connection down gracefully (idempotent): Disconnect first,
// then MediaLiveDec and a shutdownGrace sleep (deadlines the departure
// broadcasts before cc.Stop()), then cancel as the hard stop. Never-connected
// or mid-join connections cancel immediately.
func (c *Connection) Stop() {
	c.mu.Lock()
	cancel := c.cancel
	c.cancel = nil
	room := c.Room
	c.Room = nil
	c.closed = true
	c.mu.Unlock()

	if room != nil {
		room.Disconnect()
		if c.Report != nil {
			c.Report.MediaLiveDec(c.RoomName)
		}
		time.Sleep(shutdownGrace)
	}
	if cancel != nil {
		cancel()
	}
}

func (c *Connection) isStopped() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *Connection) room() *lksdk.Room {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Room
}

// track publishes one synthetic track, launches its frame-loop goroutine and
// records the pubTrack send; returns the publication SID.
func (c *Connection) track(lctx context.Context, t *lksdk.LocalTrack, name string, frame tgFrame) (string, error) {
	room := c.room()
	if room == nil {
		return "", fmt.Errorf("room already disconnected")
	}
	pub, err := room.LocalParticipant.PublishTrack(t, &lksdk.TrackPublicationOptions{Name: name})
	if err != nil {
		c.Report.AddError("media.publish." + name)
		return "", fmt.Errorf("publish %s: %w", name, err)
	}
	sid := pub.SID()

	go func() {
		tk := time.NewTicker(frame.interval)
		defer tk.Stop()
		for {
			select {
			case <-lctx.Done():
				return
			case <-tk.C:
				if c.isStopped() {
					return
				}
				if err := t.WriteSample(media.Sample{Data: frame.data, Duration: frame.interval}, nil); err != nil {
					c.Report.AddError("media.write." + name)
					return
				}
			}
		}
	}()
	return sid, nil
}

// tgFrame is one synthetic frame description: payload + interval.
type tgFrame struct {
	data     []byte
	interval time.Duration
}

// publish performs the per-mode publishing; silent audio runs in both audio
// and video modes. Listen mode publishes nothing.
func (c *Connection) publish(lctx context.Context) error {
	if c.Mode == ModeListen || c.Mode == ModeNone || c.Report == nil {
		return nil
	}

	// Settle before the mic publish (real clients don't publish instantly).
	if !waitCtx(lctx, jitterDur(settleMin, settleMax)) || c.isStopped() {
		return nil // shutdown won the race; publish nothing further
	}

	// Silent audio: opus, Channels 2 to match server SDP negotiation, one 20ms frame per tick (~20-30 kbps).
	audio, err := lksdk.NewLocalTrack(webrtc.RTPCodecCapability{
		MimeType:  webrtc.MimeTypeOpus,
		ClockRate: 48000,
		Channels:  2,
	})
	if err != nil {
		c.Report.AddError("media.track.audio")
		return fmt.Errorf("new audio track: %w", err)
	}
	sid, err := c.track(lctx, audio, "ltbot-audio", tgFrame{data: silentOpus, interval: audioInterval})
	if err != nil {
		return err
	}
	// Honest-hybrid expected = other live media-connected users in the room,
	// computed fresh per track (fan-out never includes real browsers).
	expected := c.Report.MediaLive(c.RoomName) - 1
	if expected < 0 {
		expected = 0
	}
	c.Report.RecordPubTrackSend(sid, "audio", c.Label, time.Now(), expected)

	if c.Mode != ModeVideo {
		return nil
	}

	// Mic → camera gap keeps renegotiation off a synchronized beat.
	if !waitCtx(lctx, jitterDur(camGapMin, camGapMax)) || c.isStopped() {
		return nil // shutdown won the race; publish nothing further
	}
	// Black video: 640×480 VP8 15fps with a real encoded black keyframe
	// (see blackframe.go; pion webrtc v4 carries no Width/Height in
	// RTPCodecCapability — the codec is fixed by the payload).
	video, err := lksdk.NewLocalTrack(webrtc.RTPCodecCapability{
		MimeType:  webrtc.MimeTypeVP8,
		ClockRate: 90000,
	})
	if err != nil {
		c.Report.AddError("media.track.video")
		return fmt.Errorf("new video track: %w", err)
	}
	vid, err := c.track(lctx, video, "ltbot-video", tgFrame{data: blackVP8Keyframe, interval: time.Second / videoFPS})
	if err != nil {
		return err
	}
	expected = c.Report.MediaLive(c.RoomName) - 1
	if expected < 0 {
		expected = 0
	}
	c.Report.RecordPubTrackSend(vid, "video", c.Label, time.Now(), expected)
	return nil
}

// jitterDur returns a uniform random duration in [min, max).
func jitterDur(min, max time.Duration) time.Duration {
	return min + time.Duration(rand.Int63n(int64(max-min)))
}

// waitCtx blocks for d, returning false if lctx finished first.
func waitCtx(lctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-lctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// onTrackSubscribed records the pubTrack subscriber receipt; the
// publish→first-subscription latency comes from the delivery registry.
func (c *Connection) onTrackSubscribed(publication *lksdk.RemoteTrackPublication) {
	if c.Report == nil {
		return
	}
	c.Report.RecordRecv("pubTrack", publication.SID(), time.Now())
}
