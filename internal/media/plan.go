// Per-room cohort plan implied by --media plus the three count flags.
package media

import "fmt"

// Plan is the resolved per-room cohort layout. Counts are PER ROOM: every
// room gets the same layout, publishers join first (deterministic prefix by
// per-room user index).
type Plan struct {
	Mode            Mode
	VideoPublishers int // mic+cam publishers per room
	AudioPublishers int // mic-only publishers per room
	Subscribers     int // users per room connected to LiveKit
	Users           int // per-room user count
}

// Validate checks the flag rules before any network contact; ("","") means valid.
func (p *Plan) Validate() (msg, fix string) {
	if p.Users <= 0 {
		return "--users must be >= 1", "set --users to a positive number"
	}
	if p.VideoPublishers < 0 || p.AudioPublishers < 0 || p.Subscribers < 0 {
		return "--video-publishers/--audio-publishers/--subscribers must be >= 0", "use 0 (or omit the flag) to disable a count"
	}
	anySet := p.VideoPublishers > 0 || p.AudioPublishers > 0 || p.Subscribers > 0
	switch p.Mode {
	case ModeNone:
		if anySet {
			return "--media none means no LiveKit connection at all; count flags are invalid here",
				"remove --video-publishers/--audio-publishers/--subscribers (or raise the counts and set --media video, audio or listen)"
		}
		return "", ""
	case ModeListen:
		if p.VideoPublishers > 0 || p.AudioPublishers > 0 {
			return "--media listen means nobody publishes", "remove --video-publishers/--audio-publishers, or set --media audio/video to publish"
		}
	case ModeAudio:
		if p.VideoPublishers > 0 {
			return "video publishers require --media video", "set --media video (mixed video+audio cohorts are allowed), or drop --video-publishers"
		}
		if p.AudioPublishers == 0 {
			return fmt.Sprintf("--media audio requires --audio-publishers >= 1 (how many per-room users publish a mic)"),
				fmt.Sprintf("add --audio-publishers N (1 ≤ N ≤ %d for %d users)", p.Users-p.VideoPublishers, p.Users)
		}
	case ModeVideo:
		if p.VideoPublishers == 0 {
			return "--media video requires --video-publishers >= 1 (how many per-room users publish mic+cam)",
				fmt.Sprintf("add --video-publishers N (1 ≤ N ≤ %d for %d users)", p.Users-p.AudioPublishers, p.Users)
		}
	}
	pubs := p.VideoPublishers + p.AudioPublishers
	if pubs > p.Users {
		return fmt.Sprintf("publishers (%d) exceed total users (%d)", pubs, p.Users),
			fmt.Sprintf("raise --users to at least %d, or lower --video-publishers/--audio-publishers", pubs)
	}
	if p.Subscribers > 0 {
		if p.Subscribers > p.Users {
			return fmt.Sprintf("--subscribers (%d) exceed total users (%d)", p.Subscribers, p.Users),
				fmt.Sprintf("raise --users to at least %d, or lower --subscribers", p.Subscribers)
		}
		if p.Subscribers < pubs {
			return fmt.Sprintf("--subscribers (%d) cannot be less than the publishers (%d) — every publisher also connects to LiveKit", p.Subscribers, pubs),
				fmt.Sprintf("set --subscribers to at least %d (or omit it to let all %d users subscribe)", pubs, p.Users)
		}
	} else {
		// Unset → every user subscribes.
		p.Subscribers = p.Users
	}
	return "", ""
}

// HeaderLine renders the resolved cohort shape for the startup header.
func (p *Plan) HeaderLine() string {
	if p.Mode == ModeNone {
		return " media=none"
	}
	return fmt.Sprintf(" media=%s publishers=%dv+%da subscribers=%d/%d core=%d",
		p.Mode, p.VideoPublishers, p.AudioPublishers, p.Subscribers, p.Users, p.Users-p.Subscribers)
}
