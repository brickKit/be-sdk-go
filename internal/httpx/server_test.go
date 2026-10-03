package httpx

import (
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"
)

func TestListenIsDualStack(t *testing.T) {
	ln, err := Listen(0)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	for _, host := range []string{"127.0.0.1", "::1"} {
		c, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(port)), time.Second)
		if err != nil {
			if host == "::1" && !hasIPv6Loopback() {
				t.Logf("no IPv6 loopback on this host: %v", err)
				continue
			}
			t.Fatalf("dial %s: %v", host, err)
		}
		c.Close()
	}
}

func hasIPv6Loopback() bool {
	l, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		return false
	}
	l.Close()
	return true
}

func TestServerCutsSlowHeaders(t *testing.T) {
	ln, err := Listen(0)
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}), 10*time.Second)
	srv.ReadHeaderTimeout = 300 * time.Millisecond
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, _ = c.Write([]byte("GET /healthz HTTP/1.1\r\nHost: x\r\n")) // never finishes the headers
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	start := time.Now()
	_, err = io.ReadAll(c)
	if el := time.Since(start); err != nil || el > 2*time.Second {
		t.Fatalf("slow-header client not disconnected in time: %v after %v", err, el)
	}
}

func TestNewServerTimeouts(t *testing.T) {
	s := NewServer(http.NotFoundHandler(), 10*time.Second)
	if s.ReadHeaderTimeout != 5*time.Second || s.ReadTimeout != 30*time.Second ||
		s.WriteTimeout != 15*time.Second || s.IdleTimeout != 120*time.Second {
		t.Fatalf("timeouts %+v", s)
	}
}
