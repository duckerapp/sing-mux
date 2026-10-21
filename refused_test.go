package mux

// ducker: a stream the server refuses reaches Options.OnStreamRefused with the server's message,
// and the reader still gets the error (D-336).

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/metacubex/sing/common/logger"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

// refuseHandler answers every stream the way a node does when it cannot reach the site.
type refuseHandler struct{}

func (refuseHandler) NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	return N.ReportHandshakeFailure(conn, errors.New("lookup "+metadata.Destination.Fqdn+": no such host"))
}

func (refuseHandler) NewPacketConnection(ctx context.Context, conn N.PacketConn, metadata M.Metadata) error {
	return N.ReportHandshakeFailure(conn, errors.New("lookup "+metadata.Destination.Fqdn+": no such host"))
}

func TestRefusedStreamReachesHook(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	service, err := NewService(ServiceOptions{
		NewStreamContext: func(ctx context.Context, conn net.Conn) context.Context { return ctx },
		Logger:           logger.NOP(),
		Handler:          refuseHandler{},
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go service.NewConnection(context.Background(), conn, M.Metadata{})
		}
	}()

	refused := make(chan string, 4)
	client, err := NewClient(Options{
		Dialer:          &testNet{listener: listener},
		Logger:          logger.NOP(),
		Protocol:        "h2mux",
		OnStreamRefused: func(message string) { refused <- message },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })

	for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
		conn, err := client.DialContext(context.Background(), network, M.ParseSocksaddr("gone.example:443"))
		if err != nil {
			t.Fatalf("%s: dial: %v", network, err)
		}
		if _, err = conn.Write([]byte("hello")); err != nil {
			t.Fatalf("%s: write: %v", network, err)
		}
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, err = conn.Read(make([]byte, 16))
		conn.Close()
		if err == nil || !strings.Contains(err.Error(), "remote error: lookup gone.example") {
			t.Fatalf("%s: the reader got %v, want the refusal", network, err)
		}
		select {
		case message := <-refused:
			if message != "lookup gone.example: no such host" {
				t.Fatalf("%s: the hook got %q", network, message)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s: the hook was not called", network)
		}
	}
}
