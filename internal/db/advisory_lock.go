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
const advisoryConnectionCloseTimeout = 5 * time.Second

// AdvisoryLockKey returns the stable PostgreSQL advisory-lock key for name.
// Keeping the hash in the client avoids relying on PostgreSQL's hash functions
// remaining identical across server versions.
func AdvisoryLockKey(name string) int64 {
	sum := sha256.Sum256([]byte(name))
	return int64(binary.BigEndian.Uint64(sum[:8]))
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
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection for advisory lock %q: %w", name, err)
	}
	defer conn.Release()

	lockCtx := ctx
	cancel := func() {}
	if waitTimeout > 0 {
		lockCtx, cancel = context.WithTimeout(ctx, waitTimeout)
	}
	_, err = conn.Exec(lockCtx, "SELECT pg_advisory_lock($1)", AdvisoryLockKey(name))
	cancel()
	if err != nil {
		// Cancellation can race with lock acquisition. Destroying the session
		// guarantees an ambiguously acquired lock cannot leak into the pool.
		closeErr := closeDedicatedConnection(conn)
		return errors.Join(
			fmt.Errorf("failed to acquire advisory lock %q: %w", name, err),
			closeErr,
		)
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

		if closeErr := closeDedicatedConnection(conn); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to close advisory lock session %q: %w", name, closeErr))
		}
	}()

	return fn(conn)
}

func closeDedicatedConnection(conn *pgxpool.Conn) error {
	closeCtx, cancel := context.WithTimeout(context.Background(), advisoryConnectionCloseTimeout)
	defer cancel()
	return conn.Conn().Close(closeCtx)
}
