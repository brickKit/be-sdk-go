package httpx

import (
	"fmt"
	"net"
	"net/http"
	"time"
)

// Server timeouts of P3.5.
const (
	ReadHeaderTimeout = 5 * time.Second
	ReadTimeout       = 30 * time.Second
	IdleTimeout       = 120 * time.Second
	WriteMargin       = 5 * time.Second // write = route deadline + 5 s
)

// Listen opens a TCP port on every interface, IPv4 and IPv6 (P1.13): ":port" is one dual-stack
// socket on Linux (IPV6_V6ONLY off), and plain IPv4 where the host has no IPv6.
func Listen(port int) (net.Listener, error) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("listen on port %d: %w", port, err)
	}
	return ln, nil
}

// NewServer returns the HTTP server of a member's main port with the timeouts of P3.5. The write
// timeout is the default route deadline + 5 s; the outer Handler moves it per request once the route
// (and so its own deadline) is known.
func NewServer(h http.Handler, defaultDeadline time.Duration) *http.Server {
	return &http.Server{
		Handler:           h,
		ReadHeaderTimeout: ReadHeaderTimeout,
		ReadTimeout:       ReadTimeout,
		WriteTimeout:      defaultDeadline + WriteMargin,
		IdleTimeout:       IdleTimeout,
		MaxHeaderBytes:    64 << 10,
	}
}
