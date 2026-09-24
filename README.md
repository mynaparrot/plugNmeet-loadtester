# plugNmeet load tester

Simulate realistic meeting participants to answer one question: **how many concurrent users can a room serve?**

Each virtual user behaves like a browser: it joins, chats, draws on the whiteboard, edits shared notes, raises hands, reacts, and sends heartbeats — over the same HTTP + NATS-WebSocket paths as the real web client, so the server cannot tell them apart. An optional `--media` flag also drives LiveKit with real video/audio.

> For published numbers, see [plugNmeet Benchmarks](https://www.plugnmeet.org/docs/benchmark). This README only explains how to run the tool.

## Download or build

**Option A — download a prebuilt binary (easiest):**

Grab one from [GitHub Releases](https://github.com/mynaparrot/plugnmeet-loadtester/releases):

- Stable releases are named like `loadtest-v0.1.0-linux-amd64` (also `linux-arm64`, `windows-amd64.exe`, `windows-arm64.exe`).
- A rolling `beta` build from `main` is also available for the latest fixes.

```
./loadtest-v*-linux-amd64 --version
./loadtest-v*-linux-amd64 --server http://localhost:8080 --room stress-01 --users 50 --duration 10m
```

**Option B — build from source (needs Go):**

```
go build -o bin/loadtest main.go
./bin/loadtest --server http://localhost:8080 --room stress-01 --users 50 --duration 10m
```

Webinar shape — 500 users, 2 cameras + 5 mics, 100 on video, rest meeting-activity only:

```
./bin/loadtest --server http://localhost:8080 --room mix-01 --users 500 --media video \
  --video-publishers 2 --audio-publishers 5 --subscribers 100 --duration 10m
```

Many rooms — 5 rooms x 100 users (users are per room):

```
./bin/loadtest --server http://localhost:8080 --room multi-01 --rooms 5 --users 100 --duration 10m
```

## Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `--server` | `http://localhost:8080` | plugNmeet server URL |
| `--api-key` / `--api-secret` | matches a local dev install | API credentials for token requests |
| `--room` | `load-test-room` | base room id |
| `--rooms` | `1` | number of rooms; >1 appends `-<i>` to the room id |
| `--users` | `20` | virtual users per room (no cap — size to the test machine) |
| `--duration` | `10m` | run duration |
| `--join-rate` | `1/s` | users spawned per second (global) |
| `--disable` | none | ongoing features to turn OFF: `chat`, `whiteboard`, `notepad`, `reactions`, `hands` |
| `--media` | `none` | `listen` (subscribe only), `audio` (publish mic), `video` (publish mic+cam), or `none` |
| `--video-publishers` | `0` | per-room users publishing mic+cam; required ≥1 with `--media video` |
| `--audio-publishers` | `0` | per-room users publishing mic only; required ≥1 with `--media audio`, optional with `--media video` |
| `--subscribers` | `0` (all users) | per-room cap on users connected to LiveKit at all (publishers + listen-only) |
| `--churn-rate` | `0` (off) | leave/replace pairs per second, globally across rooms |

Counts (`--video-publishers`, `--audio-publishers`, `--subscribers`) are **per room**. Publishers join first (presenters-first). Invalid combinations exit before any network contact with a fix message.

## Run notes

- Use a **fresh `--room` per run** — a stale room blocks new joins.
- Spawning must fit the window: 300 users at 1/s need `--duration ≥ 5m`.
- Size the generator at roughly **10 MB RAM per simulated user**.
- Run the built binary (`./bin/loadtest`), not `go run .`.
- Only load-test **servers you own**.
- Real humans can join the same room mid-run and interact with synthetic users.

## Reading the output

Summary prints to stdout and is saved to `results/<runid>-summary.txt` (+ `.json`).

- **Headline:** users X/Y joined, messages received, `pings ok/missed | reconnects=N`.
- **Latency percentiles:** p50/p95/max per operation (`connect`, `verifyToken`/`getJoinToken`, `initialData`, `onlineList`, `usersList`, `ping`, plus `delivery.*`; `--media` runs add `mediaJoin`/`mediaRtt` and `pubTrack`).
- **Delivery RTT + fan-out:** publish→receive per peer; `fan=N%` is delivered/expected — 100% = everyone got everything.

A run counts as clean when:

- joined == requested (`X/Y` with X == Y)
- error block shows `none`
- missed pings = 0
- reconnects = 0
- fan-out ≈ 100%
