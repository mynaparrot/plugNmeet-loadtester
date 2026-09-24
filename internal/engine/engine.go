// Package engine drives rooms and virtual users for a load-test run:
// per room IsRoomActive → CreateRoom if needed → getJoinToken per user →
// spawn user goroutines at the configured global join rate.
package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	mrand "math/rand"
	"sync"
	"time"

	"github.com/mynaparrot/plugnmeet-loadtester/internal/media"
	"github.com/mynaparrot/plugnmeet-loadtester/internal/report"
	"github.com/mynaparrot/plugnmeet-loadtester/internal/resmon"
	"github.com/mynaparrot/plugnmeet-loadtester/internal/transport"
	"github.com/mynaparrot/plugnmeet-loadtester/internal/user"
)

// Config is the (already CLI-parsed) run configuration.
type Config struct {
	ServerURL string
	APIKey    string
	APISecret string
	Room      string
	Rooms     int
	Users     int // per room
	Duration  time.Duration
	JoinRate  int           // users per second, globally
	Features  user.Features // subtractive: disabled action groups are false
	ChurnRate float64       // churn leave/replace pairs per second, globally; 0 = off

	// Per-cohort media counts, all PER ROOM. Publishers join first
	// (mic+cam, then mic-only), then listen-only, the rest stay core.
	// All three 0 ≡ --media none: every user is core.
	VideoPublishers int
	AudioPublishers int
	Subscribers     int // users with ANY LiveKit connection (publishers + listen-only)
}

// mediaRole maps a per-room user index to its cohort role by prefix:
// video publishers, audio publishers, listen-only, then core.
func (c Config) mediaRole(uIdx int) media.Role {
	switch {
	case uIdx < c.VideoPublishers:
		return media.RoleVideo
	case uIdx < c.VideoPublishers+c.AudioPublishers:
		return media.RoleAudio
	case uIdx < c.Subscribers:
		return media.RoleListen
	default:
		return media.RoleCore
	}
}

var names = []string{"Akashi", "Bella", "Bob", "Carlos", "Dana", "Emil", "Fabian",
	"Gita", "Iris", "Jibon", "John", "Jonas", "Kaya", "Liam", "Maya", "Nico",
	"Priya", "Robert", "Rosa", "Sami", "Shovon", "Simon", "Sneha", "Tara"}

func NewRunID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Run executes the whole run; runID is generated once and reused everywhere
// (banner, user IDs, results filenames).
func Run(ctx context.Context, runID string, cfg Config, rep *report.Collector) (string, error) {
	rep.SetTotal(cfg.Rooms * cfg.Users)

	// Memory sampling: fixed 5s cadence from run start until the run window
	// ends; the engine hands the result to the collector before the summary.
	memS := resmon.StartMem(rep.LiveUsers)
	defer func() { rep.SetMemRes(memS.Stop()) }()

	rooms := make([]string, cfg.Rooms)
	for i := range rooms {
		if cfg.Rooms == 1 {
			rooms[i] = cfg.Room
		} else {
			rooms[i] = fmt.Sprintf("%s-%d", cfg.Room, i)
		}
	}

	httpClient := transport.NewClient(cfg.ServerURL, cfg.APIKey, cfg.APISecret)

	// Rooms up first (sequential; the HTTP side is not the load target).
	for _, room := range rooms {
		active, err := httpClient.IsRoomActive(room)
		if err != nil {
			return runID, fmt.Errorf("isRoomActive(%s): %w", room, err)
		}
		if active {
			fmt.Println("room already active:", room)
		} else {
			if err := httpClient.CreateRoom(room); err != nil {
				return runID, fmt.Errorf("createRoom(%s): %w", room, err)
			}
			// pnm-server applies its own default (unlimited); the create
			// response carries no maxParticipants value to echo.
			fmt.Println("room created:", room)
		}
	}

	// Run window: the whole run — joins AND session — lasts exactly
	// cfg.Duration; at window expiry every remaining user closes immediately.
	runCtx, cancelCtx := context.WithTimeout(ctx, cfg.Duration)
	defer cancelCtx()

	var wg sync.WaitGroup

	// churnRunner tracks one active runner so the churn loop can cancel and
	// replace its exact role slot.
	type churnRunner struct {
		cancel context.CancelFunc
		room   string
		rIdx   int
		uIdx   int
		gen    int
		userID string
	}
	active := make([]churnRunner, 0)

	spawn := func(room string, rIdx, uIdx, gen int) {
		p, werr := buildParams(httpClient, rep, runID, room, rIdx, uIdx, gen, cfg)
		if werr != nil {
			fmt.Println(werr)
			return
		}
		// Per-user derived cancel: the churn loop ends a runner early.
		uctx, ucancel := context.WithCancel(runCtx)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := user.Run(uctx, p); err != nil {
				fmt.Printf("user %s failed: %v\n", p.UserID, err)
			}
		}()
		active = append(active, churnRunner{
			cancel: ucancel, room: room, rIdx: rIdx, uIdx: uIdx,
			gen: gen, userID: p.UserID,
		})
	}

	ticker := time.NewTicker(time.Second / time.Duration(maximum(cfg.JoinRate, 1)))
	defer ticker.Stop()

	for rIdx, room := range rooms {
		for uIdx := 0; uIdx < cfg.Users; uIdx++ {
			select {
			case <-runCtx.Done():
				wg.Wait()
				return runID, fmt.Errorf("spawn aborted (run window expired)")
			case <-ticker.C:
			}
			spawn(room, rIdx, uIdx, 1)
		}
	}

	// The spawn loop ends when all users are launched: no wg.Wait() here,
	// churn must fire during the run window.

	// Population gauge (the report section only shows it under churn).
	rep.SetChurnPop(cfg.Rooms * cfg.Users)

	// Churn: steady-state slot refill — cancel a random active runner and
	// spawn a replacement into the same room/uIdx with a fresh id (the
	// departed user's JetStream durable is deleted only ~5s after disconnect).
	if cfg.ChurnRate > 0 && len(active) > 0 {
		tickerChurn := time.NewTicker(time.Duration(float64(time.Second) / cfg.ChurnRate))
		defer tickerChurn.Stop()
	churnLoop:
		for {
			select {
			case <-runCtx.Done():
				break churnLoop
			case <-tickerChurn.C:
				if runCtx.Err() != nil {
					break churnLoop
				}
				i := mrand.Intn(len(active))
				tr := active[i]
				active = append(active[:i], active[i+1:]...)
				// (a) leave-signal sends BEFORE cancel: expected is computed
				// while the user is still counted in its room.
				exp := rep.RoomLive(tr.room) - 1
				if exp < 0 {
					exp = 0
				}
				rep.RecordSend("leaveDetect", tr.userID, time.Now(), exp)
				rep.RecordSend("leaveOffline", tr.userID, time.Now(), exp)
				// (b) immediate close.
				tr.cancel()
				rep.ChurnLeave(tr.room)
				// (c) refill the same role slot with a fresh id.
				before := len(active)
				spawn(tr.room, tr.rIdx, tr.uIdx, tr.gen+1)
				if len(active) > before {
					rep.ChurnJoin()
				}
			}
		}
	}
	wg.Wait()
	return runID, nil
}

func maximum(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// buildParams builds one user's Params; gen ≥ 2 churn replacements get a
// "-g<n>" id suffix so ids are never reused.
func buildParams(c *transport.Client, rep *report.Collector, runID, room string, rIdx, uIdx, gen int, cfg Config) (user.Params, error) {
	userID := fmt.Sprintf("lt-%s-%d-%d", runID, rIdx, uIdx)
	if gen > 1 {
		userID = fmt.Sprintf("%s-g%d", userID, gen)
	}
	name := names[(rIdx*cfg.Users+uIdx)%len(names)]
	t0 := time.Now()
	token, err := c.GetJoinToken(room, name, userID, uIdx == 0)
	if err != nil {
		return user.Params{}, fmt.Errorf("getJoinToken(%s): %w", userID, err)
	}
	rep.Record("getJoinToken", time.Since(t0))
	return user.Params{
		Http:      c,
		RoomID:    room,
		UserID:    userID,
		Name:      name,
		IsAdmin:   uIdx == 0,
		Idx:       uIdx,
		Token:     token,
		Rep:       rep,
		Features:  cfg.Features,
		MediaRole: cfg.mediaRole(uIdx),
		RunID:     runID,
	}, nil
}
