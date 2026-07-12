package security

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

type Allowlist struct {
	hosts    []string
	resolver *net.Resolver
}

func NewAllowlist(hosts []string) *Allowlist {
	return &Allowlist{hosts: hosts, resolver: net.DefaultResolver}
}
func (a *Allowlist) Validate(ctx context.Context, raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return errors.New("invalid URL")
	}
	if u.Scheme != "https" {
		return errors.New("only https URLs are allowed")
	}
	h := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	allowed := false
	for _, rule := range a.hosts {
		rule = strings.TrimSuffix(strings.ToLower(rule), ".")
		if h == rule || (strings.HasPrefix(rule, "*.") && strings.HasSuffix(h, rule[1:]) && h != rule[2:]) {
			allowed = true
			break
		}
	}
	if !allowed {
		return fmt.Errorf("host %q is not allowlisted", h)
	}
	ips, err := a.resolver.LookupIPAddr(ctx, h)
	if err != nil {
		return fmt.Errorf("resolve host: %w", err)
	}
	if len(ips) == 0 {
		return errors.New("host has no addresses")
	}
	for _, item := range ips {
		if forbidden(item.IP) {
			return fmt.Errorf("host resolves to forbidden address %s", item.IP)
		}
	}
	return nil
}
func (a *Allowlist) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if err = a.Validate(ctx, "https://"+net.JoinHostPort(host, port)); err != nil {
		return nil, err
	}
	ips, err := a.resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	dialer := net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	var last error
	for _, item := range ips {
		if forbidden(item.IP) {
			continue
		}
		conn, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(item.IP.String(), port))
		if dialErr == nil {
			return conn, nil
		}
		last = dialErr
	}
	if last == nil {
		last = errors.New("no safe destination addresses")
	}
	return nil, last
}
func forbidden(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() || ip.IsMulticast() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast()
}
