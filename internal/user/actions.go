package user

import (
	"bytes"
	"compress/gzip"
	"context"
	"math/rand"
	"time"
)

// Default action rates (no flags): public/private chat idx%3==0 (45s/3m
// jittered means), reactions idx%2==0 (90s), hand raise/lower idx%3==1 (10m,
// one pair), visibility+quality all users (5m/60s), whiteboard presenter
// bursts + viewers pointer (~1.5s while "watching"), full-state gzip every
// ~2m during bursts, notepad sync at join + presenter update ~90s.
const (
	chatCorpusSize  = 256 // approx bytes per realistic message + meta padding
	privateChatMu   = 3 * time.Minute
	reactionMu      = 90 * time.Second
	handMu          = 10 * time.Minute
	visibilityMu    = 5 * time.Minute
	cqMu            = 60 * time.Second
	notepadUpdateMu = 90 * time.Second
)

// publicChatCorpus: small realistic corpus (finite so NS pollution is low).
var publicChatCorpus = []string{
	"Can everyone see my screen?",
	"Great question — let me answer that after this slide.",
	"Audio is a bit choppy for me, refreshing now.",
	"I'll share the notes in the notepad afterwards.",
	"Straight to the point, thanks!",
	"Can you zoom in on the diagram?",
	"Perfect, that matches what we discussed yesterday.",
	"Recording started a bit late, I think.",
	"We should validate the delivery RTT numbers again.",
	"Any objections to merging this today?",
	"Looks good to me 👍",
	"Give me a minute, joining from mobile.",
}

const reactionEmojiSet = "👍❤️😂🎉👏🙌😯"

type ctxT = context.Context

func randFloat64() float64 { return rand.Float64() }

// fanExpected: expected recipients = per-room live population − 1 at send time.
func (u *runner) fanExpected() int {
	n := u.p.Rep.RoomLive(u.p.RoomID) - 1
	if n < 0 {
		n = 0
	}
	return n
}

// jitter returns mean * U(0.5,1.5).
func jitter(mean time.Duration) time.Duration {
	f := 0.5 + randFloat64()
	return time.Duration(float64(mean) * f)
}

// warmup returns a first-fire delay below the given ceiling (so every action
// fires once even in short validation runs) — deviation documented in README.
func warmup(min, maxD time.Duration) time.Duration {
	return min + time.Duration(randFloat64()*float64(maxD-min))
}

// actionLoop fires fn once after firstDelay, then at jittered intervals of mean.
type actionLoopFn func() error

// actionLoop fires fn once after firstDelay, then at jittered intervals of mean.
func (u *runner) actionLoop(ctx ctxT, firstDelay time.Duration, mean time.Duration, once bool, name string, fn actionLoopFn) {
	fire := func() error {
		if err := fn(); err != nil {
			u.p.Rep.AddError("send." + name)
		}
		return nil
	}
	_ = fire // placeholder to keep shape; real loop below
	t := time.NewTimer(firstDelay)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fire()
			if once {
				return
			}
		}
		t.Reset(jitter(mean))
	}
}

// synthPayload returns n random bytes standing in for a real Yjs update /
// awareness blob.
func synthPayload(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// gzipBytes compresses src with stdlib gzip — matches the pnm-client compress()
// helper used for full-state broadcasts and NOTEPAD_SYNC_RESPONSE.
func gzipBytes(src []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(src); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// startActions launches the role-aware action loops; they all end with ctx.
func (u *runner) startActions(ctx context.Context) {
	idx := u.p.Idx
	f := u.p.Features
	if idx%3 == 0 && f.Chat {
		go u.actionLoop(ctx, warmup(10*time.Second, 45*time.Second), 45*time.Second, false, "publicChat", u.publicChat)
		go u.actionLoop(ctx, warmup(30*time.Second, 100*time.Second), privateChatMu, false, "privateChat", u.privateChat)
	}
	if idx%2 == 0 && f.Reactions {
		go u.actionLoop(ctx, warmup(20*time.Second, 60*time.Second), reactionMu, false, "reaction", u.reaction)
	}
	if idx%3 == 1 && f.Hands {
		go u.actionLoop(ctx, warmup(60*time.Second, 120*time.Second), handMu, true, "raiseHand", func() error {
			if err := u.raiseHand(); err != nil {
				return err
			}
			time.AfterFunc(warmup(30*time.Second, 60*time.Second), func() {
				_ = u.lowerHand()
			})
			return nil
		})
	}
	// all users
	go u.actionLoop(ctx, warmup(60*time.Second, 180*time.Second), visibilityMu, false, "visibility", u.visibility)
	go u.actionLoop(ctx, warmup(30*time.Second, 60*time.Second), cqMu, false, "connQuality", u.connQuality)
	if idx == 0 {
		// presenter extras
		if f.Notepad {
			go u.actionLoop(ctx, warmup(45*time.Second, 120*time.Second), notepadUpdateMu, false, "notepadUpdate", u.notepadUpdate)
		}
		if f.Whiteboard {
			go u.whiteboardLoop(ctx)
		}
	}
	if idx%2 == 0 && f.Whiteboard {
		go u.pointerLoop(ctx) // pointer watching is part of the whiteboard feature
	}
}
