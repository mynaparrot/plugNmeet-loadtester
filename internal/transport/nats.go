package transport

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/mynaparrot/plugnmeet-protocol/plugnmeet"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// SessionConfig carries everything verifyToken returned plus the access token.
type SessionConfig struct {
	ServerURL      string
	ApiKey         string
	ApiSecret      string
	AccessToken    string
	NatsWsUrls     []string
	RoomID         string
	UserID         string
	RoomStreamName string
	Subjects       *plugnmeet.NatsSubjects
	// OnReconnect is invoked on every NATS reconnect attempt (optional).
	OnReconnect func()
}

// ConnectNats connects to NATS over WebSocket with client-parity options
// mirroring pnm-client ConnectNats.ts wsconnect (name plugnmeet-client-{roomId}_{userId},
// token auth, no_echo, 10s ping).
func ConnectNats(cfg SessionConfig) (*nats.Conn, error) {
	if len(cfg.NatsWsUrls) == 0 {
		return nil, fmt.Errorf("no nats ws urls")
	}

	// Add the scheme-default port while preserving path/query untouched.
	natsURL := cfg.NatsWsUrls[0]
	if u, err := url.Parse(natsURL); err == nil && u.Port() == "" {
		var port string
		switch u.Scheme {
		case "wss", "tls":
			port = "443"
		case "ws", "nats":
			port = "80"
		}
		if port != "" {
			u.Host = net.JoinHostPort(u.Hostname(), port)
			natsURL = u.String()
		}
	}

	name := fmt.Sprintf("plugnmeet-client-%s_%s", cfg.RoomID, cfg.UserID)
	opts := []nats.Option{
		nats.Token(cfg.AccessToken),
		nats.Name(name),
		nats.NoEcho(),
		nats.PingInterval(10 * time.Second),
		nats.MaxPingsOutstanding(3),
		nats.ReconnectWait(2 * time.Second),
		nats.Timeout(10 * time.Second),
	}
	if cfg.OnReconnect != nil {
		opts = append(opts, nats.ReconnectErrHandler(func(_ *nats.Conn, _ error) {
			cfg.OnReconnect()
		}))
	}
	return nats.Connect(natsURL, opts...)
}

// AttachUserConsumer grabs the server-pre-created per-user JetStream durable
// consumer (name "{roomId}_{userId}"), mirroring SubscriptionHandler.ts.
func AttachUserConsumer(ctx context.Context, js jetstream.JetStream, cfg SessionConfig) (jetstream.Consumer, error) {
	// Client token only carries $JS.API.CONSUMER.INFO/MSG.NEXT/ACK — js.Stream()
	// is not permitted.
	durable := fmt.Sprintf("%s_%s", cfg.RoomID, cfg.UserID)
	return js.Consumer(ctx, cfg.RoomStreamName, durable)
}
