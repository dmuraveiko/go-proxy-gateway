package httpx

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/textproto"
	"sort"
	"strconv"
	"strings"
	"time"

	"proxy-server/internal/contracts"
)

type Executor struct {
	client      *http.Client
	maxResponse int64
}

func New(maxResponse int64, maxIdle, maxIdlePerHost int, idleTimeout time.Duration) *Executor {
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          maxIdle,
		MaxIdleConnsPerHost:   maxIdlePerHost,
		IdleConnTimeout:       idleTimeout,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		DisableCompression:    true,
	}
	return &Executor{client: &http.Client{Transport: transport, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}, maxResponse: maxResponse}
}

func (e *Executor) Do(ctx context.Context, in contracts.HTTPRequest) (contracts.HTTPResult, error) {
	req, err := http.NewRequestWithContext(ctx, in.Method, in.URL, bytes.NewReader(in.Body))
	if err != nil {
		return contracts.HTTPResult{}, fmt.Errorf("build request: %w", err)
	}
	for _, h := range in.Headers {
		if !validHeaderName(h.Name) || strings.ContainsAny(h.Value, "\r\n") {
			return contracts.HTTPResult{}, fmt.Errorf("invalid header %q", h.Name)
		}
		switch strings.ToLower(h.Name) {
		case "host":
			req.Host = h.Value
		case "content-length":
			n, e := strconv.ParseInt(h.Value, 10, 64)
			if e != nil || n < 0 {
				return contracts.HTTPResult{}, errors.New("invalid Content-Length")
			}
			req.ContentLength = n
		default:
			req.Header.Add(h.Name, h.Value)
		}
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return contracts.HTTPResult{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, e.maxResponse+1))
	if err != nil {
		return contracts.HTTPResult{}, fmt.Errorf("read response: %w", err)
	}
	if int64(len(body)) > e.maxResponse {
		return contracts.HTTPResult{}, errors.New("response body exceeds configured limit")
	}
	return contracts.HTTPResult{StatusCode: resp.StatusCode, Headers: headerFields(resp.Header), Body: body}, nil
}
func (e *Executor) CloseIdleConnections() { e.client.CloseIdleConnections() }
func headerFields(h http.Header) []contracts.HeaderField {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []contracts.HeaderField
	for _, k := range keys {
		for _, v := range h.Values(k) {
			out = append(out, contracts.HeaderField{Name: k, Value: v})
		}
	}
	return out
}
func validHeaderName(v string) bool { return v != "" && textproto.CanonicalMIMEHeaderKey(v) != "" }
