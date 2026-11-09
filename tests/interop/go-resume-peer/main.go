// Interop peer for tests/e2e/resume.e2e.test.ts: libxumux-go with
// session-resume/1 over TCP. The test builds it against the libxumux-go
// checkout in LIBXUMUX_GO_DIR. Progress goes to stderr.
//
//	go-resume-peer server        listen on 127.0.0.1, print "port N" on stdout
//	go-resume-peer client PORT   connect to PORT and run the client exchange
//
// Both run the bulk exchange on every channel named bulk:<bytes>, whoever
// opened it: each side sends <bytes> random bytes in messages of random sizes
// (type 7, some empty, many fragmented), then an empty type 8. On the peer's
// type 8 it answers with the hex SHA-256 of what it received (type 9); on the
// peer's type 9 it compares that with the hash of what it sent and answers
// "ok" or "mismatch" (type 10). The server prints "bulk ok NAME" or "bulk
// error NAME: ..." on stdout for each.
//
// The server rotates its token every 20 ms, so tokens are replaced around
// every resume. The client opens bulk:12000000, bulk:8000000 (requested
// unreliable and unordered: on TCP it is reliable and ordered, so replayed)
// and has one HELLO channel, bulk:6000000. It resumes whenever the transport
// is lost and exits 0 once every exchange succeeded and the session closed
// normally.
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	mrand "math/rand/v2"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	xumux "github.com/deftai/libxumux-go"
)

func main() {
	log.SetPrefix("[go] ")
	if len(os.Args) > 1 && os.Args[1] == "server" {
		server()
		return
	}
	if len(os.Args) != 3 || os.Args[1] != "client" {
		log.Fatal("usage: go-resume-peer server | client PORT")
	}
	client(os.Args[2])
}

var stdout sync.Mutex

func say(format string, a ...any) {
	stdout.Lock()
	defer stdout.Unlock()
	fmt.Printf(format+"\n", a...)
}

func bulkOptions(report func(name string, err error)) xumux.Options {
	return xumux.Options{
		PingInterval: -1,
		Acceptor: func(context.Context, xumux.ChannelRequest) (xumux.Handler, error) {
			return func(ch *xumux.Channel) {
				if strings.HasPrefix(ch.Name(), "bulk:") {
					report(ch.Name(), bulk(ch))
				}
			}, nil
		},
	}
}

func server() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	say("port %d", ln.Addr().(*net.TCPAddr).Port)
	rs := xumux.NewResumeServer(xumux.ResumeServerOptions{Timeout: 30 * time.Second, RotateEvery: 20 * time.Millisecond})
	opts := bulkOptions(func(name string, err error) {
		if err != nil {
			say("bulk error %s: %v", name, err)
		} else {
			say("bulk ok %s", name)
		}
	})
	for {
		nc, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			_, _, err := xumux.Accept(context.Background(), xumux.NewStreamConn(nc), xumux.ServerConfig{Resume: rs}, opts)
			if err != nil && !errors.Is(err, xumux.ErrFirstFrameHandled) {
				log.Print("accept: ", err)
			}
		}()
	}
}

func client(port string) {
	addr := net.JoinHostPort("127.0.0.1", port)
	dial := func(ctx context.Context) (xumux.FrameConn, error) {
		var d net.Dialer
		nc, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		return xumux.NewStreamConn(nc), nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := dial(ctx)
	if err != nil {
		log.Fatal(err)
	}
	errs := make(chan error, 4)
	resumes := 0
	var mu sync.Mutex
	s, res, err := xumux.Connect(ctx, conn, xumux.ClientConfig{
		Channels: []xumux.ChannelSpec{{Name: "bulk:6000000", Reliable: true, Ordered: true}},
		Resume: &xumux.ResumeClient{
			Redial:     dial,
			RetryDelay: 20 * time.Millisecond,
			OnDetach:   func(err error) { log.Print("detached: ", err) },
			OnResume:   func() { mu.Lock(); resumes++; mu.Unlock(); log.Print("resumed") },
		},
	}, bulkOptions(func(string, error) {}))
	if err != nil {
		log.Fatal(err)
	}
	if len(res.Channels) != 1 {
		log.Fatal("HELLO channel not accepted")
	}
	go func() { errs <- bulk(res.Channels[0]) }()
	for _, spec := range []xumux.ChannelSpec{
		{Name: "bulk:12000000", Reliable: true, Ordered: true},
		{Name: "bulk:8000000", Reliable: false, Ordered: false},
	} {
		ch, err := s.OpenChannel(ctx, spec)
		if err != nil {
			log.Fatal(err)
		}
		if p := ch.Properties(); !p.Reliable || !p.Ordered {
			log.Fatalf("%s: effective properties %+v on TCP", spec.Name, p)
		}
		go func() { errs <- bulk(ch) }()
	}
	for range 3 {
		if err := <-errs; err != nil {
			log.Fatal(err)
		}
	}
	mu.Lock()
	log.Printf("all exchanges ok after %d resumes", resumes)
	mu.Unlock()
	select {
	case <-s.Done():
	case <-ctx.Done():
		log.Fatal("the TS side did not close")
	}
	if info := s.CloseInfo(); info.Code != xumux.CodeNormal {
		log.Fatalf("closed with %+v", info)
	}
}

// bulk runs the bulk exchange on ch.
func bulk(ch *xumux.Channel) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	total, err := strconv.Atoi(strings.TrimPrefix(ch.Name(), "bulk:"))
	if err != nil {
		return err
	}
	sent := make(chan string, 1)
	sendErr := make(chan error, 1)
	go func() {
		h := sha256.New()
		for n := 0; n < total; {
			k := min(1+mrand.IntN(70000), total-n)
			if mrand.IntN(10) == 0 {
				k = min(mrand.IntN(64), total-n)
			}
			b := make([]byte, k)
			_, _ = rand.Read(b)
			h.Write(b)
			if err := ch.Send(ctx, 7, b); err != nil {
				sendErr <- err
				return
			}
			n += k
		}
		if err := ch.Send(ctx, 8, nil); err != nil {
			sendErr <- err
			return
		}
		sent <- hex.EncodeToString(h.Sum(nil))
	}()
	rh := sha256.New()
	received := 0
	var mine, theirs string
	for mine == "" || theirs == "" {
		m, err := ch.Receive(ctx)
		if err != nil {
			return fmt.Errorf("%s: %w", ch.Name(), err)
		}
		switch m.Type {
		case 7:
			rh.Write(m.Payload)
			received += len(m.Payload)
		case 8:
			if err := ch.Send(ctx, 9, []byte(hex.EncodeToString(rh.Sum(nil)))); err != nil {
				return err
			}
		case 9:
			select {
			case want := <-sent:
				mine = "mismatch"
				if string(m.Payload) == want {
					mine = "ok"
				}
			case err := <-sendErr:
				return err
			}
			if err := ch.Send(ctx, 10, []byte(mine)); err != nil {
				return err
			}
		case 10:
			theirs = string(m.Payload)
		default:
			return fmt.Errorf("%s: unexpected type %d", ch.Name(), m.Type)
		}
	}
	if mine != "ok" || theirs != "ok" || received != total {
		return fmt.Errorf("%s: mine=%s theirs=%s received=%d/%d", ch.Name(), mine, theirs, received, total)
	}
	return nil
}
