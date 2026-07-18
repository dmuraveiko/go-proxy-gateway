package repository

import (
	"context"
	"time"
)

func (r *Repository) ClaimDelivery(ctx context.Context, lease time.Duration) (Delivery, error) {
	transaction, err := r.pool.Begin(ctx)
	if err != nil {
		return Delivery{}, err
	}
	defer transaction.Rollback(ctx)
	var delivery Delivery
	err = transaction.QueryRow(ctx, `SELECT delivery_id,kind,client_id,subject,message_type,payload FROM proxy_deliveries WHERE acknowledged_at IS NULL AND next_attempt_at<=now() AND (lease_until IS NULL OR lease_until<now()) ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&delivery.ID, &delivery.Kind, &delivery.ClientID, &delivery.Subject, &delivery.MessageType, &delivery.Payload)
	if err != nil {
		return delivery, err
	}
	if _, err = transaction.Exec(ctx, `UPDATE proxy_deliveries SET lease_until=now()+$2::interval,attempts=attempts+1 WHERE delivery_id=$1`, delivery.ID, pgInterval(lease)); err != nil {
		return delivery, err
	}
	return delivery, transaction.Commit(ctx)
}

func (r *Repository) RescheduleDelivery(ctx context.Context, deliveryID string, after time.Duration) error {
	_, err := r.pool.Exec(ctx, `UPDATE proxy_deliveries SET next_attempt_at=now()+$2::interval,lease_until=NULL WHERE delivery_id=$1 AND acknowledged_at IS NULL`, deliveryID, pgInterval(after))
	return err
}
