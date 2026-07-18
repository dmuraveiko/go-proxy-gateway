package repository

import (
	"context"
	"time"
)

func (r *Repository) AcquireHostPermit(ctx context.Context, host string, rps, concurrency int, minInterval, lease time.Duration) (string, error) {
	transaction, err := r.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer transaction.Rollback(ctx)
	if _, err = transaction.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, host); err != nil {
		return "", err
	}
	if _, err = transaction.Exec(ctx, `DELETE FROM proxy_host_permits WHERE lease_until<now()`); err != nil {
		return "", err
	}
	var active int
	if err = transaction.QueryRow(ctx, `SELECT count(*) FROM proxy_host_permits WHERE host=$1`, host).Scan(&active); err != nil {
		return "", err
	}
	if active >= concurrency {
		return "", ErrNoPermit
	}
	if minInterval > 0 {
		tag, updateErr := transaction.Exec(ctx, `INSERT INTO proxy_host_last_dispatch(host,dispatched_at) VALUES($1,now()) ON CONFLICT(host) DO UPDATE SET dispatched_at=excluded.dispatched_at WHERE proxy_host_last_dispatch.dispatched_at<=now()-$2::interval`, host, pgInterval(minInterval))
		if updateErr != nil {
			return "", updateErr
		}
		if tag.RowsAffected() == 0 {
			return "", ErrNoPermit
		}
	}
	tag, err := transaction.Exec(ctx, `INSERT INTO proxy_host_rate_windows(host,window_start,used) VALUES($1,date_trunc('second',now()),1) ON CONFLICT(host,window_start) DO UPDATE SET used=proxy_host_rate_windows.used+1 WHERE proxy_host_rate_windows.used<$2`, host, rps)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() == 0 {
		return "", ErrNoPermit
	}
	token := randomID()
	if _, err = transaction.Exec(ctx, `INSERT INTO proxy_host_permits(token,host,lease_until) VALUES($1,$2,now()+$3::interval)`, token, host, pgInterval(lease)); err != nil {
		return "", err
	}
	return token, transaction.Commit(ctx)
}

func (r *Repository) ReleaseHostPermit(ctx context.Context, token string) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM proxy_host_permits WHERE token=$1`, token)
	return err
}
