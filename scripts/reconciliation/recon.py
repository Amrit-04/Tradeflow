#!/usr/bin/env python3
"""
Tradeflow End-of-Day (EOD) Reconciliation Engine
Compares internal trades and positions against simulated clearing house reports.
Classifies breaks:
  - QTY_MISMATCH: Internal net quantity != clearing house net quantity
  - MISSING_TRADE: Trade reported by clearing house but missing internally
  - EXTRA_TRADE: Trade logged internally but unconfirmed by clearing house
"""

import sys
import json
import csv
from collections import defaultdict
from datetime import date

def reconcile(internal_trades, clearing_trades, recon_date=None):
    if recon_date is None:
        recon_date = date.today().isoformat()

    breaks = []

    # 1. Trade-level mapping by (venue, exec_id)
    internal_map = {f"{t['venue']}:{t['exec_id']}": t for t in internal_trades}
    clearing_map = {f"{t['venue']}:{t['exec_id']}": t for t in clearing_trades}

    # Missing Trades: present in clearing, absent in internal
    for key, c_trade in clearing_map.items():
        if key not in internal_map:
            breaks.append({
                "recon_date": recon_date,
                "account": c_trade["account"],
                "symbol": c_trade["symbol"],
                "break_type": "MISSING_TRADE",
                "internal_qty": 0,
                "clearing_qty": int(c_trade["qty"]),
                "details": f"Trade {key} exists in clearing house but is missing internally",
                "status": "OPEN"
            })

    # Extra Trades: present in internal, absent in clearing
    for key, i_trade in internal_map.items():
        if key not in clearing_map:
            breaks.append({
                "recon_date": recon_date,
                "account": i_trade["account"],
                "symbol": i_trade["symbol"],
                "break_type": "EXTRA_TRADE",
                "internal_qty": int(i_trade["qty"]),
                "clearing_qty": 0,
                "details": f"Trade {key} recorded internally but unconfirmed by clearing house",
                "status": "OPEN"
            })

    # 2. Position-level aggregation: Net Qty = sum(Buy) - sum(Sell)
    def aggregate_net_qty(trades):
        net = defaultdict(int)
        for t in trades:
            key = (t["account"], t["symbol"])
            qty = int(t["qty"])
            if t["side"] == "BUY":
                net[key] += qty
            else:
                net[key] -= qty
        return net

    int_positions = aggregate_net_qty(internal_trades)
    clr_positions = aggregate_net_qty(clearing_trades)

    all_keys = set(int_positions.keys()).union(set(clr_positions.keys()))
    for acct, sym in sorted(all_keys):
        iq = int_positions.get((acct, sym), 0)
        cq = clr_positions.get((acct, sym), 0)
        if iq != cq:
            breaks.append({
                "recon_date": recon_date,
                "account": acct,
                "symbol": sym,
                "break_type": "QTY_MISMATCH",
                "internal_qty": iq,
                "clearing_qty": cq,
                "details": f"Net quantity mismatch: internal={iq}, clearing={cq} (variance={iq - cq})",
                "status": "OPEN"
            })

    return breaks

def demo():
    # Synthetic internal trades
    internal_data = [
        {"venue": "SIM_VENUE", "exec_id": "EX-001", "account": "ACCT-A", "symbol": "AAPL", "side": "BUY", "qty": 500, "price": 15000},
        {"venue": "SIM_VENUE", "exec_id": "EX-002", "account": "ACCT-A", "symbol": "AAPL", "side": "BUY", "qty": 200, "price": 15050},
        {"venue": "SIM_VENUE", "exec_id": "EX-003", "account": "ACCT-A", "symbol": "MSFT", "side": "BUY", "qty": 100, "price": 40000},
        {"venue": "SIM_VENUE", "exec_id": "EX-EXTRA", "account": "ACCT-B", "symbol": "NVDA", "side": "BUY", "qty": 50, "price": 90000},
    ]

    # Synthetic clearing trades (with 1 missing trade and 1 extra trade)
    clearing_data = [
        {"venue": "SIM_VENUE", "exec_id": "EX-001", "account": "ACCT-A", "symbol": "AAPL", "side": "BUY", "qty": 500, "price": 15000},
        {"venue": "SIM_VENUE", "exec_id": "EX-002", "account": "ACCT-A", "symbol": "AAPL", "side": "BUY", "qty": 200, "price": 15050},
        {"venue": "SIM_VENUE", "exec_id": "EX-003", "account": "ACCT-A", "symbol": "MSFT", "side": "BUY", "qty": 100, "price": 40000},
        {"venue": "SIM_VENUE", "exec_id": "EX-MISSING", "account": "ACCT-A", "symbol": "GOOGL", "side": "BUY", "qty": 300, "price": 17500},
    ]

    print("=====================================================================")
    print("           TRADEFLOW END-OF-DAY RECONCILIATION ENGINE               ")
    print("=====================================================================")
    breaks = reconcile(internal_data, clearing_data)

    print(f"\nReconciliation finished: {len(breaks)} breaks identified.\n")
    for i, b in enumerate(breaks, 1):
        print(f"[{i}] {b['break_type']:<15} | Account: {b['account']:<8} | Symbol: {b['symbol']:<6} | Internal: {b['internal_qty']:<5} | Clearing: {b['clearing_qty']:<5}")
        print(f"    Details: {b['details']}")
    print("=====================================================================")

if __name__ == "__main__":
    demo()
