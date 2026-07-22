package transport

import (
	"context"
	"errors"
	"time"

	"github.com/dmuraveiko/go-proxy-gateway/internal/metrics"
	"github.com/jackc/pgx/v5"
)

func (c *Core) deliveryLoop(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		delivery, err := c.repo.ClaimDelivery(ctx, 10*time.Second)
		if errors.Is(err, pgx.ErrNoRows) {
			if !wait(ctx, 100*time.Millisecond) {
				return nil
			}
			continue
		}
		if err != nil {
			if !wait(ctx, time.Second) {
				return nil
			}
			continue
		}
		if err = c.PublishRaw(ctx, delivery.Subject, delivery.MessageType, delivery.Payload); err != nil {
			metrics.DeliveryRetries.WithLabelValues(c.natsKind(delivery.Subject)).Inc()
			c.log.Warn("core NATS delivery failed", "delivery_id", delivery.ID, "error", err)
		}
		if err = c.repo.RescheduleDelivery(ctx, delivery.ID, c.deliveryRetry); err != nil && ctx.Err() == nil {
			return err
		}
	}
}
