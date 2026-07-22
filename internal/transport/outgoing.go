package transport

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"time"

	"github.com/dmuraveiko/go-proxy-gateway/internal/contracts"
	"github.com/dmuraveiko/go-proxy-gateway/internal/metrics"
	"github.com/dmuraveiko/go-proxy-gateway/internal/repository"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
)

func (c *Core) requestSubject() string     { return "proxy." + c.proxyID + ".requests" }
func (c *Core) acceptedACKSubject() string { return "proxy." + c.proxyID + ".accepted_acks" }
func (c *Core) resultACKSubject() string   { return "proxy." + c.proxyID + ".result_acks" }

func (c *Core) handleRequest(message *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, payload, err := c.decode(message, contracts.TypeHTTPRequest)
	if err != nil {
		metrics.NetworkRequests.WithLabelValues("in", "error", "regular").Inc()
		c.reject("http request", err)
		return
	}
	metrics.NetworkRequests.WithLabelValues("in", "success", "regular").Inc()
	var request contracts.HTTPRequest
	if err = json.Unmarshal(payload, &request); err != nil {
		c.reject("http request", err)
		return
	}
	if err = c.validateRequest(request, client); err != nil {
		c.rejectRequest(ctx, client, request.RequestID, "invalid_request", err)
		return
	}
	acceptance := contracts.Acceptance{RequestID: request.RequestID, DeliveryID: "accept_" + request.RequestID, ProxyID: c.proxyID, Accepted: true, AcceptedAt: time.Now().UTC()}
	if err = c.repo.AcceptHTTPRequest(ctx, request, acceptance, c.clientSubject(client, "accepted")); err != nil {
		if errors.Is(err, repository.ErrRequestConflict) {
			c.rejectRequest(ctx, client, request.RequestID, "request_id_conflict", err)
			return
		}
		c.reject("accept request", err)
	}
}

func (c *Core) rejectRequest(ctx context.Context, clientID, requestID, code string, cause error) {
	c.reject("http request", cause)
	_ = c.Publish(ctx, c.clientSubject(clientID, "accepted"), contracts.TypeAcceptance, contracts.Acceptance{RequestID: requestID, ProxyID: c.proxyID, Accepted: false, ErrorCode: code, Error: cause.Error(), AcceptedAt: time.Now().UTC()})
}

func (c *Core) validateRequest(request contracts.HTTPRequest, client string) error {
	if request.RequestID == "" || request.ClientID != client || request.ProxyID != c.proxyID {
		return errors.New("request identity mismatch")
	}
	if !c.allowedClient(client) {
		return errors.New("client is not allowed on this proxy")
	}
	if int64(len(request.Body)) > c.maxRequestBytes {
		return errors.New("request body exceeds NATS contract limit")
	}
	if request.Method == "" {
		return errors.New("method is required")
	}
	target, err := url.Parse(request.URL)
	if err != nil || target.Host == "" || (target.Scheme != "http" && target.Scheme != "https") {
		return errors.New("absolute HTTP/HTTPS URL is required")
	}
	if request.Retry.MaxAttempts < 0 || request.Retry.MaxAttempts > 20 {
		return errors.New("invalid retry attempts")
	}
	return nil
}

func (c *Core) handleAcceptanceACK(message *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, payload, err := c.decode(message, contracts.TypeAcceptanceACK)
	if err != nil {
		metrics.NetworkRequests.WithLabelValues("in", "error", "regular").Inc()
		c.reject("acceptance ACK", err)
		return
	}
	metrics.NetworkRequests.WithLabelValues("in", "success", "regular").Inc()
	var ack contracts.DeliveryACK
	if json.Unmarshal(payload, &ack) != nil || ack.ClientID != client {
		return
	}
	if err = c.repo.ConfirmAcceptance(ctx, ack); err != nil {
		c.reject("acceptance ACK", err)
		return
	}
	_ = c.Publish(ctx, c.clientSubject(client, "ack_confirmed"), contracts.TypeACKConfirmed, contracts.ACKConfirmed{DeliveryID: ack.DeliveryID, ConfirmedAt: time.Now().UTC()})
}

func (c *Core) handleResultACK(message *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, payload, err := c.decode(message, contracts.TypeResultACK)
	if err != nil {
		metrics.NetworkRequests.WithLabelValues("in", "error", "regular").Inc()
		c.reject("result ACK", err)
		return
	}
	metrics.NetworkRequests.WithLabelValues("in", "success", "regular").Inc()
	var ack contracts.DeliveryACK
	if json.Unmarshal(payload, &ack) != nil || ack.ClientID != client {
		return
	}
	if err = c.repo.ConfirmResult(ctx, ack); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return // A direct result can arrive before its DB delivery row.
		}
		c.reject("result ACK", err)
		return
	}
	_ = c.Publish(ctx, c.clientSubject(client, "ack_confirmed"), contracts.TypeACKConfirmed, contracts.ACKConfirmed{DeliveryID: ack.DeliveryID, ConfirmedAt: time.Now().UTC()})
}
