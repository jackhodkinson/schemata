package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackhodkinson/schemata/internal/config"
)

// Executor is an interface that both Pool and Tx implement
// This allows functions to work with either a connection pool or a transaction
type Executor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Pool wraps a database connection pool
type Pool struct {
	*pgxpool.Pool
}

// Connect creates a new connection pool from a DBConnection config
func Connect(ctx context.Context, conn *config.DBConnection) (*Pool, error) {
	connStr, err := conn.ToConnectionString()
	if err != nil {
		return nil, fmt.Errorf("failed to build connection string: %w", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test the connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Pool{Pool: pool}, nil
}

// Close closes the connection pool
func (p *Pool) Close() {
	if p.Pool != nil {
		p.Pool.Close()
	}
}

