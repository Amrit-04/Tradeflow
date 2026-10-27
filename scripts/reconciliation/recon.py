#!/usr/bin/env python3
"""
Tradeflow End-of-Day (EOD) Reconciliation Engine
Connects to PostgreSQL 16 to extract internal positions and trades, compares them against
the simulated clearing-house CSV report, and identifies breaks:
  - MISSING_TRADE: Trade reported by clearing house but missing internally
  - EXTRA_TRADE: Trade logged internally but unconfirmed by clearing house
  - QTY_MISMATCH: Residual position quantity discrepancy not accounted for by trade-level breaks

Institutional Feature:
  Root-cause attribution suppresses derivative QTY_MISMATCH when unmatched MISSING or EXTRA
  trades fully account for the net quantity variance on that account/symbol.
"""

import sys
import os
import csv
import argparse
from collections import defaultdict
from datetime import date, datetime

def read_clearing_csv(csv_path):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Clearing house CSV file not found: {csv_path}")
    
    trades = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row or row.get('venue', '').startswith('#'):
                continue
            trades.append({
                "venue": row["venue"].strip(),
                "exec_id": row["exec_id"].strip(),
                "account": row["account"].strip(),
                "symbol": row["symbol"].strip(),
                "side": row["side"].strip().upper(),
                "qty": int(row["qty"]),
                "price": int(row.get("price", 0)),
                "transact_time": row.get("transact_time", "")
            })
    return trades

def load_from_postgres(pg_url):
    try:
        import psycopg
    except ImportError:
        print("[Recon] Warning: 'psycopg' module not installed. Falling back to synthetic internal dataset.")
        return None, None

    try:
        with psycopg.connect(pg_url) as conn:
            with conn.cursor() as cur:
                # Query confirmed trades
                cur.execute("""
                    SELECT venue, exec_id, account, symbol, side, exec_qty, exec_price, transact_time
                    FROM trades ORDER BY transact_time ASC;
                """)
                trade_rows = cur.fetchall()
                internal_trades = []
                for r in trade_rows:
                    internal_trades.append({
                        "venue": r[0],
                        "exec_id": r[1],
                        "account": r[2],
                        "symbol": r[3],
                        "side": r[4],
                        "qty": int(r[5]),
                        "price": int(r[6]),
                        "transact_time": str(r[7])
                    })

                # Query positions
                cur.execute("""
                    SELECT account, symbol, net_qty, avg_cost, realized_pnl, unrealized_pnl
                    FROM positions;
                """)
                pos_rows = cur.fetchall()
                internal_positions = {}
                for r in pos_rows:
                    internal_positions[(r[0], r[1])] = int(r[2])

                return internal_trades, internal_positions
    except Exception as e:
        print(f"[Recon] Notice: PostgreSQL connection to {pg_url} failed ({e}). Using offline dataset.")
        return None, None

def save_breaks_to_postgres(pg_url, breaks):
    try:
        import psycopg
        with psycopg.connect(pg_url) as conn:
            with conn.cursor() as cur:
                for b in breaks:
                    cur.execute("""
                        INSERT INTO breaks (recon_date, account, symbol, internal_qty, clearing_qty, break_type, status, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, clock_timestamp());
                    """, (
                        b["recon_date"], b["account"], b["symbol"],
                        b["internal_qty"], b["clearing_qty"], b["break_type"], b["status"]
                    ))
            conn.commit()
            print(f"[Recon] Successfully persisted {len(breaks)} reconciliation breaks to PostgreSQL breaks table.")
    except Exception as e:
        print(f"[Recon] Notice: Could not persist breaks to PostgreSQL: {e}")

def reconcile(internal_trades, clearing_trades, internal_positions=None, recon_date=None):
    if recon_date is None:
        recon_date = date.today().isoformat()

    breaks = []

    # 1. Trade-level mapping by (venue, exec_id)
    internal_map = {f"{t['venue']}:{t['exec_id']}": t for t in internal_trades}
    clearing_map = {f"{t['venue']}:{t['exec_id']}": t for t in clearing_trades}

    # Track net quantity effect of missing / extra trades per (account, symbol)
    # Expected variance: clearing_qty - internal_qty
    trade_explained_deltas = defaultdict(int)

    # Missing Trades: present in clearing, absent in internal
    for key, c_trade in clearing_map.items():
        if key not in internal_map:
            qty = int(c_trade["qty"])
            side = c_trade["side"].upper()
            signed_qty = qty if side == "BUY" else -qty
            trade_explained_deltas[(c_trade["account"], c_trade["symbol"])] += signed_qty

            breaks.append({
                "recon_date": recon_date,
                "account": c_trade["account"],
                "symbol": c_trade["symbol"],
                "break_type": "MISSING_TRADE",
                "internal_qty": 0,
                "clearing_qty": qty,
                "details": f"Trade {key} ({side} {qty}) reported by clearing house but missing internally",
                "status": "OPEN",
                "root_cause": True
            })

    # Extra Trades: present in internal, absent in clearing
    for key, i_trade in internal_map.items():
        if key not in clearing_map:
            qty = int(i_trade["qty"])
            side = i_trade["side"].upper()
            signed_qty = qty if side == "BUY" else -qty
            trade_explained_deltas[(i_trade["account"], i_trade["symbol"])] -= signed_qty

            breaks.append({
                "recon_date": recon_date,
                "account": i_trade["account"],
                "symbol": i_trade["symbol"],
                "break_type": "EXTRA_TRADE",
                "internal_qty": qty,
                "clearing_qty": 0,
                "details": f"Trade {key} ({side} {qty}) recorded internally but unconfirmed by clearing house",
                "status": "OPEN",
                "root_cause": True
            })

    # 2. Position-level aggregation
    def aggregate_net_qty(trades):
        net = defaultdict(int)
        for t in trades:
            key = (t["account"], t["symbol"])
            qty = int(t["qty"])
            if t["side"].upper() == "BUY":
                net[key] += qty
            else:
                net[key] -= qty
        return net

    if internal_positions is None:
        internal_positions = aggregate_net_qty(internal_trades)
    clearing_positions = aggregate_net_qty(clearing_trades)

    all_keys = set(internal_positions.keys()).union(set(clearing_positions.keys()))
    for acct, sym in sorted(all_keys):
        iq = internal_positions.get((acct, sym), 0)
        cq = clearing_positions.get((acct, sym), 0)
        actual_variance = cq - iq

        if iq != cq:
            explained = trade_explained_deltas.get((acct, sym), 0)
            if explained == actual_variance:
                # ROOT CAUSE SUPPRESSION:
                # The position mismatch is 100% accounted for by the MISSING/EXTRA trade(s).
                # Suppress the derivative QTY_MISMATCH to avoid dual-reporting on the same issue.
                continue

            # Unexplained residual variance
            residual = actual_variance - explained
            breaks.append({
                "recon_date": recon_date,
                "account": acct,
                "symbol": sym,
                "break_type": "QTY_MISMATCH",
                "internal_qty": iq,
                "clearing_qty": cq,
                "details": f"Net quantity mismatch: internal={iq}, clearing={cq} (unexplained variance={residual})",
                "status": "OPEN",
                "root_cause": False
            })

    return breaks

def main():
    parser = argparse.ArgumentParser(description="Tradeflow EOD Position & Trade Reconciliation")
    parser.add_argument("--pg", default=os.getenv("POSTGRES_URL", "postgres://tradeflow:tradeflow_password@localhost:5432/tradeflow?sslmode=disable"),
                        help="PostgreSQL connection string")
    parser.add_argument("--clearing", default="data/clearing_eod.csv",
                        help="Path to clearing house CSV file")
    args = parser.parse_args()

    print("=====================================================================")
    print("           TRADEFLOW END-OF-DAY RECONCILIATION ENGINE               ")
    print("=====================================================================")

    # 1. Load clearing house report
    clearing_trades = read_clearing_csv(args.clearing)
    print(f"[Recon] Ingested {len(clearing_trades)} trades from clearing house report: {args.clearing}")

    # 2. Load internal trades and positions from PostgreSQL
    internal_trades, internal_positions = load_from_postgres(args.pg)

    if internal_trades is None:
        # Fallback to demo fixture if offline
        print("[Recon] Using fallback demo dataset.")
        internal_trades = [
            {"venue": "SIM_VENUE", "exec_id": "EX-001", "account": "ACCT-ALPHA", "symbol": "AAPL", "side": "BUY", "qty": 500, "price": 15000},
            {"venue": "SIM_VENUE", "exec_id": "EX-002", "account": "ACCT-ALPHA", "symbol": "MSFT", "side": "BUY", "qty": 200, "price": 40000},
            {"venue": "SIM_VENUE", "exec_id": "EX-003", "account": "ACCT-ALPHA", "symbol": "AAPL", "side": "SELL", "qty": 200, "price": 15500},
            {"venue": "SIM_VENUE", "exec_id": "EX-004", "account": "ACCT-ALPHA", "symbol": "NVDA", "side": "BUY", "qty": 100, "price": 90000},
        ]
        internal_positions = {
            ("ACCT-ALPHA", "AAPL"): 300,
            ("ACCT-ALPHA", "MSFT"): 200,
            ("ACCT-ALPHA", "NVDA"): 100,
        }
    else:
        print(f"[Recon] Ingested {len(internal_trades)} confirmed trades and {len(internal_positions)} positions from PostgreSQL.")

    # 3. Execute reconciliation with root-cause break suppression
    breaks = reconcile(internal_trades, clearing_trades, internal_positions)

    print(f"\nReconciliation finished: {len(breaks)} breaks identified.\n")
    for i, b in enumerate(breaks, 1):
        print(f"[{i}] {b['break_type']:<15} | Account: {b['account']:<12} | Symbol: {b['symbol']:<6} | Internal: {b['internal_qty']:<5} | Clearing: {b['clearing_qty']:<5}")
        print(f"    Details: {b['details']}")
    print("=====================================================================")

    # Persist breaks to DB if connected
    if internal_trades is not None:
        save_breaks_to_postgres(args.pg, breaks)

if __name__ == "__main__":
    main()
