// Interop peer for tests/e2e/pipe-interop.e2e.test.ts: libxumux-go's pipe/1
// on this process's stdio, as the xumux server. The test builds it against
// the libxumux-go checkout in LIBXUMUX_GO_DIR. Progress goes to stderr.
//
// Pipes it accepts, by target:
//   - "echo": copies the pipe back to itself, then half-closes.
//   - "reset": reads 5 bytes, then sends RESET 4501 "boom".
//   - "dial-back": opens a pipe to the TS side, writes 300 KB and half-closes,
//     reads the echo, and answers "ok" or "mismatch" on the dial-back pipe.
//
// It exits 0 once the TS side closes the session normally.
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"

	xumux "github.com/deftai/libxumux-go"
	"github.com/deftai/libxumux-go/pipe"
)

type stdio struct{}

func (stdio) Read(b []byte) (int, error)  { return os.Stdin.Read(b) }
func (stdio) Write(b []byte) (int, error) { return os.Stdout.Write(b) }
func (stdio) Close() error                { return os.Stdout.Close() }

func main() {
	log.SetPrefix("[go] ")
	ctx := context.Background()
	l := pipe.NewListener()
	s, _, err := xumux.Accept(ctx, xumux.NewStreamConn(stdio{}), xumux.ServerConfig{},
		xumux.Options{Acceptor: l.Acceptor(), PingInterval: -1})
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		for {
			c, err := l.AcceptPipe(ctx)
			if err != nil {
				return
			}
			go serve(ctx, s, c)
		}
	}()
	<-s.Done()
	if info := s.CloseInfo(); info.Code != xumux.CodeNormal {
		log.Fatalf("session closed with %d %s", info.Code, info.Reason)
	}
	log.Print("session closed normally")
}

func serve(ctx context.Context, s *xumux.Session, c *pipe.Conn) {
	switch c.Target() {
	case "echo":
		n, err := io.Copy(c, c)
		log.Printf("echoed %d bytes (%v)", n, err)
		_ = c.CloseWrite()
	case "reset":
		buf := make([]byte, 5)
		_, _ = io.ReadFull(c, buf)
		_ = c.Reset(4501, "boom")
	case "dial-back":
		out, err := pipe.Dial(ctx, s, "from-go", "")
		if err != nil {
			fmt.Fprintf(c, "dial: %v", err)
			_ = c.CloseWrite()
			return
		}
		want := make([]byte, 300_000)
		for i := range want {
			want[i] = byte(i * 7)
		}
		go func() { _, _ = out.Write(want); _ = out.CloseWrite() }()
		got, err := io.ReadAll(out)
		if err == nil && bytes.Equal(got, want) {
			_, _ = io.WriteString(c, "ok")
		} else {
			fmt.Fprintf(c, "mismatch: %d bytes, %v", len(got), err)
		}
		_ = c.CloseWrite()
		_, _ = io.Copy(io.Discard, c)
	default:
		_ = c.Reset(4010, "unknown target")
	}
}
