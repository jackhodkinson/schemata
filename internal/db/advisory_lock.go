package db

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const advisoryUnlockTimeout = 5 * time.Second

// AdvisoryLockKey returns the stable PostgreSQL advisory-lock key for name.
// Keeping the hash in the client avoids relying on PostgreSQL's hash functions
// remaining identical across server versions.
func AdvisoryLockKey(name string) int64 {
	sum := sha256.Sum256([]byte(name))
	return int64(binary.BigEndian.Uint64(sum[:8]))
}

// SessionOwnsAdvisoryLock reports whether executor's PostgreSQL backend owns
// the granted bigint advisory lock for name without changing its re-entrant
// lock count.
func SessionOwnsAdvisoryLock(ctx context.Context, executor Executor, name string) (bool, error) {
	key := uint64(AdvisoryLockKey(name))
	classID := int64(uint32(key >> 32))
	objectID := int64(uint32(key))

	var owned bool
	err := executor.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_catalog.pg_locks
			WHERE locktype = 'advisory'
			  AND pid = pg_catalog.pg_backend_pid()
			  AND classid = $1::bigint::oid
			  AND objid = $2::bigint::oid
			  AND objsubid = 1
			  AND granted
		)
	`, classID, objectID).Scan(&owned)
	if err != nil {
		return false, fmt.Errorf("failed to inspect advisory lock %q: %w", name, err)
	}
	return owned, nil
}

// WithSessionAdvisoryLock acquires a dedicated pooled connection, holds a
// session-level advisory lock on that exact connection for fn's entire
// lifetime, and releases both afterward. The dedicated session is always
// destroyed rather than returned to the pool: callers may execute arbitrary
// SQL that changes session state or acquires additional advisory locks.
func WithSessionAdvisoryLock(
	ctx context.Context,
	pool *Pool,
	name string,
	waitTimeout time.Duration,
	fn func(*pgxpool.Conn) error,
) (err error) {
	return WithDedicatedConnection(ctx, pool, func(conn *pgxpool.Conn) (err error) {
		lockCtx := ctx
		cancel := func() {}
		if waitTimeout > 0 {
			lockCtx, cancel = context.WithTimeout(ctx, waitTimeout)
		}
		_, err = conn.Exec(lockCtx, "SELECT pg_advisory_lock($1)", AdvisoryLockKey(name))
		cancel()
		if err != nil {
			return fmt.Errorf("failed to acquire advisory lock %q: %w", name, err)
		}

		defer func() {
			unlockCtx, cancelUnlock := context.WithTimeout(context.WithoutCancel(ctx), advisoryUnlockTimeout)
			defer cancelUnlock()

			var unlocked bool
			unlockErr := conn.QueryRow(
				unlockCtx,
				"SELECT pg_advisory_unlock($1)",
				AdvisoryLockKey(name),
			).Scan(&unlocked)
			if unlockErr == nil && !unlocked {
				unlockErr = fmt.Errorf("lock was not held by its dedicated connection")
			}
			if unlockErr != nil {
				err = errors.Join(
					err,
					fmt.Errorf("failed to release advisory lock %q: %w", name, unlockErr),
				)
			}
		}()

		return fn(conn)
	})
}

// WithDedicatedConnection runs fn on a connection that is destroyed instead
// of returned to the caller's pool. A one-connection child pool forces a new
// physical PostgreSQL session, so arbitrary migration SQL cannot inherit or
// leak settings, prepared statements, temporary objects, roles, or locks.
func WithDedicatedConnection(
	ctx context.Context,
	pool *Pool,
	fn func(*pgxpool.Conn) error,
) (err error) {
	config := pool.Config()
	config.MaxConns = 1
	config.MinConns = 0
	config.MinIdleConns = 0
	dedicatedPool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return fmt.Errorf("failed to create dedicated connection pool: %w", err)
	}
	defer dedicatedPool.Close()

	conn, err := dedicatedPool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire dedicated connection: %w", err)
	}
	defer conn.Release()

	return fn(conn)
}
