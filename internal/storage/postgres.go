package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresRepository implements Repository using PostgreSQL 16 with pgxpool.
type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(ctx context.Context, connString string) (*PostgresRepository, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres config: %w", err)
	}
	cfg.MaxConns = 25
	cfg.MinConns = 5
	cfg.MaxConnLifetime = time.Hour

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}
	return &PostgresRepository{pool: pool}, nil
}

func (p *PostgresRepository) Close() {
	p.pool.Close()
}

func (p *PostgresRepository) SaveOrderEventAndProject(ctx context.Context, event OrderEventRecord, order OrderRecord, outbox *OutboxRecord) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// 1. Insert order event (Append-Only)
	eventSQL := `
		INSERT INTO order_events (order_id, seq, event_type, payload, created_at)
		VALUES ($1, $2, $3, $4, $5);
	`
	_, err = tx.Exec(ctx, eventSQL, event.OrderID, event.Seq, event.EventType, event.Payload, event.CreatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
			return ErrDuplicateEventSeq
		}
		return fmt.Errorf("failed to insert order_event: %w", err)
	}

	// 2. Upsert orders projection
	orderSQL := `
		INSERT INTO orders (order_id, cl_ord_id, account, symbol, side, order_qty, price, state, cum_qty, leaves_qty, avg_price, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		ON CONFLICT (order_id) DO UPDATE SET
			state = EXCLUDED.state,
			cum_qty = EXCLUDED.cum_qty,
			leaves_qty = EXCLUDED.leaves_qty,
			avg_price = EXCLUDED.avg_price,
			version = EXCLUDED.version,
			updated_at = EXCLUDED.updated_at;
	`
	_, err = tx.Exec(ctx, orderSQL,
		order.OrderID, order.ClOrdID, order.Account, order.Symbol, order.Side,
		order.OrderQty, order.Price, order.State, order.CumQty, order.LeavesQty,
		order.AvgPrice, order.Version, order.CreatedAt, order.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert order projection: %w", err)
	}

	// 3. Insert Outbox if present
	if outbox != nil {
		outboxSQL := `
			INSERT INTO outbox (outbox_id, topic, partition_key, payload, created_at)
			VALUES ($1, $2, $3, $4, $5);
		`
		_, err = tx.Exec(ctx, outboxSQL, outbox.OutboxID, outbox.Topic, outbox.PartitionKey, outbox.Payload, outbox.CreatedAt)
		if err != nil {
			return fmt.Errorf("failed to insert outbox record: %w", err)
		}
	}

	return tx.Commit(ctx)
}

func (p *PostgresRepository) GetOrder(ctx context.Context, orderID string) (*OrderRecord, error) {
	sql := `
		SELECT order_id, cl_ord_id, account, symbol, side, order_qty, price, state, cum_qty, leaves_qty, avg_price, version, created_at, updated_at
		FROM orders WHERE order_id = $1;
	`
	var o OrderRecord
	err := p.pool.QueryRow(ctx, sql, orderID).Scan(
		&o.OrderID, &o.ClOrdID, &o.Account, &o.Symbol, &o.Side,
		&o.OrderQty, &o.Price, &o.State, &o.CumQty, &o.LeavesQty,
		&o.AvgPrice, &o.Version, &o.CreatedAt, &o.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrOrderNotFound
		}
		return nil, fmt.Errorf("query order error: %w", err)
	}
	return &o, nil
}

func (p *PostgresRepository) GetOrderEvents(ctx context.Context, orderID string) ([]OrderEventRecord, error) {
	sql := `
		SELECT event_id, order_id, seq, event_type, payload, created_at
		FROM order_events WHERE order_id = $1 ORDER BY seq ASC;
	`
	rows, err := p.pool.Query(ctx, sql, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []OrderEventRecord
	for rows.Next() {
		var r OrderEventRecord
		if err := rows.Scan(&r.EventID, &r.OrderID, &r.Seq, &r.EventType, &r.Payload, &r.CreatedAt); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

func (p *PostgresRepository) SaveTrade(ctx context.Context, trade TradeRecord) (bool, error) {
	sql := `
		INSERT INTO trades (venue, exec_id, order_id, account, symbol, side, exec_qty, exec_price, transact_time, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (venue, exec_id) DO NOTHING;
	`
	tag, err := p.pool.Exec(ctx, sql,
		trade.Venue, trade.ExecID, trade.OrderID, trade.Account, trade.Symbol,
		trade.Side, trade.ExecQty, trade.ExecPrice, trade.TransactTime, trade.CreatedAt,
	)
	if err != nil {
		return false, fmt.Errorf("failed to save trade: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

func (p *PostgresRepository) GetTrades(ctx context.Context, account string, symbol string) ([]TradeRecord, error) {
	sql := `
		SELECT trade_id, venue, exec_id, order_id, account, symbol, side, exec_qty, exec_price, transact_time, created_at
		FROM trades WHERE account = $1 AND symbol = $2 ORDER BY transact_time ASC;
	`
	rows, err := p.pool.Query(ctx, sql, account, symbol)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var trades []TradeRecord
	for rows.Next() {
		var t TradeRecord
		if err := rows.Scan(
			&t.TradeID, &t.Venue, &t.ExecID, &t.OrderID, &t.Account, &t.Symbol,
			&t.Side, &t.ExecQty, &t.ExecPrice, &t.TransactTime, &t.CreatedAt,
		); err != nil {
			return nil, err
		}
		trades = append(trades, t)
	}
	return trades, rows.Err()
}

func (p *PostgresRepository) GetAllTrades(ctx context.Context) ([]TradeRecord, error) {
	sql := `
		SELECT trade_id, venue, exec_id, order_id, account, symbol, side, exec_qty, exec_price, transact_time, created_at
		FROM trades ORDER BY transact_time ASC;
	`
	rows, err := p.pool.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var trades []TradeRecord
	for rows.Next() {
		var t TradeRecord
		if err := rows.Scan(
			&t.TradeID, &t.Venue, &t.ExecID, &t.OrderID, &t.Account, &t.Symbol,
			&t.Side, &t.ExecQty, &t.ExecPrice, &t.TransactTime, &t.CreatedAt,
		); err != nil {
			return nil, err
		}
		trades = append(trades, t)
	}
	return trades, rows.Err()
}

func (p *PostgresRepository) GetPosition(ctx context.Context, account string, symbol string) (*PositionRecord, error) {
	sql := `
		SELECT account, symbol, net_qty, avg_cost, realized_pnl, unrealized_pnl, updated_at
		FROM positions WHERE account = $1 AND symbol = $2;
	`
	var pos PositionRecord
	err := p.pool.QueryRow(ctx, sql, account, symbol).Scan(
		&pos.Account, &pos.Symbol, &pos.NetQty, &pos.AvgCost,
		&pos.RealizedPnL, &pos.UnrealizedPnL, &pos.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrPositionNotFound
		}
		return nil, err
	}
	return &pos, nil
}

func (p *PostgresRepository) SavePosition(ctx context.Context, pos PositionRecord) error {
	sql := `
		INSERT INTO positions (account, symbol, net_qty, avg_cost, realized_pnl, unrealized_pnl, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (account, symbol) DO UPDATE SET
			net_qty = EXCLUDED.net_qty,
			avg_cost = EXCLUDED.avg_cost,
			realized_pnl = EXCLUDED.realized_pnl,
			unrealized_pnl = EXCLUDED.unrealized_pnl,
			updated_at = EXCLUDED.updated_at;
	`
	_, err := p.pool.Exec(ctx, sql,
		pos.Account, pos.Symbol, pos.NetQty, pos.AvgCost,
		pos.RealizedPnL, pos.UnrealizedPnL, pos.UpdatedAt,
	)
	return err
}

func (p *PostgresRepository) GetAllPositions(ctx context.Context) ([]PositionRecord, error) {
	sql := `SELECT account, symbol, net_qty, avg_cost, realized_pnl, unrealized_pnl, updated_at FROM positions;`
	rows, err := p.pool.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var positions []PositionRecord
	for rows.Next() {
		var pos PositionRecord
		if err := rows.Scan(
			&pos.Account, &pos.Symbol, &pos.NetQty, &pos.AvgCost,
			&pos.RealizedPnL, &pos.UnrealizedPnL, &pos.UpdatedAt,
		); err != nil {
			return nil, err
		}
		positions = append(positions, pos)
	}
	return positions, rows.Err()
}

func (p *PostgresRepository) GetUnpublishedOutbox(ctx context.Context, limit int) ([]OutboxRecord, error) {
	sql := `
		SELECT outbox_id, topic, partition_key, payload, created_at
		FROM outbox WHERE published_at IS NULL
		ORDER BY created_at ASC LIMIT $1;
	`
	rows, err := p.pool.Query(ctx, sql, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []OutboxRecord
	for rows.Next() {
		var o OutboxRecord
		if err := rows.Scan(&o.OutboxID, &o.Topic, &o.PartitionKey, &o.Payload, &o.CreatedAt); err != nil {
			return nil, err
		}
		records = append(records, o)
	}
	return records, rows.Err()
}

func (p *PostgresRepository) MarkOutboxPublished(ctx context.Context, outboxID string) error {
	sql := `UPDATE outbox SET published_at = clock_timestamp() WHERE outbox_id = $1;`
	_, err := p.pool.Exec(ctx, sql, outboxID)
	return err
}

func (p *PostgresRepository) SavePendingFill(ctx context.Context, fill PendingFillRecord) (bool, error) {
	sql := `
		INSERT INTO pending_fills (venue, exec_id, order_id, payload, retry_count, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (venue, exec_id) DO NOTHING;
	`
	tag, err := p.pool.Exec(ctx, sql,
		fill.Venue, fill.ExecID, fill.OrderID, fill.Payload, fill.RetryCount, fill.Status, fill.CreatedAt,
	)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (p *PostgresRepository) GetPendingFillsByOrder(ctx context.Context, orderID string) ([]PendingFillRecord, error) {
	sql := `
		SELECT pending_id, venue, exec_id, order_id, payload, retry_count, status, created_at, last_retry_at
		FROM pending_fills WHERE order_id = $1 AND status = 'PENDING';
	`
	rows, err := p.pool.Query(ctx, sql, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []PendingFillRecord
	for rows.Next() {
		var pf PendingFillRecord
		if err := rows.Scan(
			&pf.PendingID, &pf.Venue, &pf.ExecID, &pf.OrderID, &pf.Payload,
			&pf.RetryCount, &pf.Status, &pf.CreatedAt, &pf.LastRetryAt,
		); err != nil {
			return nil, err
		}
		records = append(records, pf)
	}
	return records, rows.Err()
}

func (p *PostgresRepository) MarkPendingFillProcessed(ctx context.Context, pendingID string) error {
	sql := `UPDATE pending_fills SET status = 'PROCESSED' WHERE pending_id = $1;`
	_, err := p.pool.Exec(ctx, sql, pendingID)
	return err
}

func (p *PostgresRepository) SaveBreak(ctx context.Context, b BreakRecord) error {
	sql := `
		INSERT INTO breaks (recon_date, account, symbol, internal_qty, clearing_qty, break_type, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
	`
	_, err := p.pool.Exec(ctx, sql,
		b.ReconDate, b.Account, b.Symbol, b.InternalQty, b.ClearingQty, b.BreakType, b.Status, b.CreatedAt,
	)
	return err
}

func (p *PostgresRepository) GetBreaksByDate(ctx context.Context, date string) ([]BreakRecord, error) {
	sql := `SELECT break_id, recon_date, account, symbol, internal_qty, clearing_qty, break_type, status, created_at FROM breaks WHERE recon_date = $1;`
	rows, err := p.pool.Query(ctx, sql, date)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var breaks []BreakRecord
	for rows.Next() {
		var b BreakRecord
		if err := rows.Scan(
			&b.BreakID, &b.ReconDate, &b.Account, &b.Symbol,
			&b.InternalQty, &b.ClearingQty, &b.BreakType, &b.Status, &b.CreatedAt,
		); err != nil {
			return nil, err
		}
		breaks = append(breaks, b)
	}
	return breaks, rows.Err()
}
