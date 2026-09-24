// Command loadtest is the plugNmeet load-tester entry point. No --nats flag:
// the NATS URL is derived from verifyToken's nats_ws_urls, like the web client.
//
// Flags: --server --api-key --api-secret --room --rooms --users --duration
// --join-rate --churn-rate --media --video-publishers --audio-publishers
// --subscribers --disable --version.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	lkLogger "github.com/livekit/protocol/logger"
	"github.com/mynaparrot/plugnmeet-loadtester/internal/engine"
	"github.com/mynaparrot/plugnmeet-loadtester/internal/media"
	"github.com/mynaparrot/plugnmeet-loadtester/internal/report"
	"github.com/mynaparrot/plugnmeet-loadtester/internal/resmon"
	"github.com/mynaparrot/plugnmeet-loadtester/internal/transport"
	"github.com/mynaparrot/plugnmeet-loadtester/internal/user"
	"github.com/mynaparrot/plugnmeet-loadtester/version"
)

const defaultServer = "http://localhost:8080"

func main() {
	serverURL := flag.String("server", defaultServer, "plugNmeet server URL (local proxy entry point)")
	apiKey := flag.String("api-key", "plugnmeet", "API key")
	apiSecret := flag.String("api-secret", "zumyyYWqv7KR2kUqvYdq4z4sXg7XTBD2ljT6", "API secret")
	room := flag.String("room", "load-test-room", "base room id")
	users := flag.Int("users", 20, "virtual users per room")
	duration := flag.Duration("duration", 10*time.Minute, "run duration")
	rooms := flag.Int("rooms", 1, ">1 appends -<i> to the room id")
	joinRate := flag.Int("join-rate", 1, "users spawned per second (global)")
	churnRate := flag.Float64("churn-rate", 0, "churn leaves/joins pairs per second (global across rooms); 0 = off")
	mediaMode := flag.String("media", "none", "master media mode: none, listen, audio, video; count flags below are PER ROOM")
	videoPubs := flag.Int("video-publishers", 0, "per-room users publishing mic+cam; required >=1 when --media video")
	audioPubs := flag.Int("audio-publishers", 0, "per-room users publishing mic only; required >=1 when --media audio")
	subscribers := flag.Int("subscribers", 0, "per-room cap on users connected to LiveKit (publishers + listen-only); 0/unset = all users subscribe")
	showVersion := flag.Bool("version", false, "print version and exit")
	feats := user.DefaultFeatures()
	var disable user.DisableSet
	flag.Var(&disable, "disable", "comma-separated feature names to turn OFF: chat,whiteboard,notepad,reactions,hands")
	flag.Parse()

	if *showVersion {
		fmt.Println(version.Version)
		os.Exit(0)
	}

	if !media.Validate(media.Mode(*mediaMode)) {
		fmt.Println("error: unknown --media:", *mediaMode)
		fmt.Println("valid(media): none, listen, audio, video")
		os.Exit(1)
	}

	plan := media.Plan{Mode: media.Mode(*mediaMode), VideoPublishers: *videoPubs, AudioPublishers: *audioPubs, Subscribers: *subscribers, Users: *users}
	if msg, fix := plan.Validate(); msg != "" {
		fmt.Println("error:", msg)
		fmt.Println("fix:", fix)
		os.Exit(1)
	}

	if err := disable.Validate(); err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
	disabled := disable.Apply(&feats)

	if err := transport.HealthCheck(*serverURL); err != nil {
		fmt.Println("server health check failed:", err)
		fmt.Printf("is the plugNmeet server reachable at %s (healthCheck endpoint)?\n", *serverURL)
		os.Exit(1)
	}

	rep := report.New()

	offMs, offOK, offErr := transport.MeasureClockOffset(*serverURL)
	if offOK {
		rep.SetClockOffsetMs(offMs)
		fmt.Printf("clock offset (local − pnm-server, HTTP Date): %+dms\n", offMs)
		if offMs >= 2000 || offMs <= -2000 {
			fmt.Printf("WARNING: local clock differs from pnm-server by %+ds — server-timestamp metrics (delivery.userJoined) are skewed by this offset; sync NTP\n", offMs/1000)
		}
	} else {
		fmt.Printf("clock offset (local − pnm-server, HTTP Date): unknown (%v); clock_offset_ms is omitted from the summary\n", offErr)
	}

	// to avoid pion logs
	logConf := &lkLogger.Config{
		Level: "warn",
	}
	lkLogger.InitFromConfig(logConf, "pnm-load-tester")

	runID := engine.NewRunID()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sampler := resmon.Start(2 * time.Second)
	liveStop := make(chan struct{})
	go rep.LiveLine(liveStop)

	featsStr := "-"
	if len(disabled) > 0 {
		sorted := make([]string, len(disabled))
		copy(sorted, disabled)
		sort.Strings(sorted)
		featsStr = strings.Join(sorted, ",")
	}
	churnStr := ""
	if *churnRate > 0 {
		churnStr = fmt.Sprintf(" churnRate=%.2f/s", *churnRate)
	}
	mediaStr := plan.HeaderLine()
	fmt.Printf("== load test %s: rooms=%d users/room=%d duration=%s joinRate=%d/s%s%s server=%s disable=%s\n",
		runID, *rooms, *users, *duration, *joinRate, churnStr, mediaStr, *serverURL, featsStr)

	rep.SetMedia(report.MediaInfo{
		Mode:            string(plan.Mode),
		VideoPublishers: plan.VideoPublishers,
		AudioPublishers: plan.AudioPublishers,
		Subscribers:     plan.Subscribers,
		UsersPerRoom:    *users,
	})

	cnf := engine.Config{
		ServerURL:       *serverURL,
		APIKey:          *apiKey,
		APISecret:       *apiSecret,
		Room:            *room,
		Rooms:           *rooms,
		Users:           *users,
		Duration:        *duration,
		JoinRate:        *joinRate,
		ChurnRate:       *churnRate,
		VideoPublishers: plan.VideoPublishers,
		AudioPublishers: plan.AudioPublishers,
		Subscribers:     plan.Subscribers,
		Features:        feats,
	}

	runIDFinal, rerr := engine.Run(ctx, runID, cnf, rep)
	close(liveStop)
	rep.SetResources(sampler.Stop())

	rep.WriteSummary(runIDFinal)
	if rerr != nil {
		fmt.Println("RUN ERROR:", rerr)
		os.Exit(1)
	}
}
