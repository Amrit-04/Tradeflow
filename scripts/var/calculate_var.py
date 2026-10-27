#!/usr/bin/env python3
"""
Tradeflow Historical Simulation Value at Risk (VaR) Engine
Computes 1-Day 99% Value at Risk (VaR) per account using real 250-day historical equity returns.

Methodology:
  Full revaluation historical simulation across 250 consecutive trading sessions.
  For each historical trading day d in [1..250]:
    Scenario PnL_d = sum_i( Position_Notional_i * Daily_Return_{i, d} )
  Scenario PnLs are sorted ascending; the 99% 1-Day VaR cutoff is the 1st percentile of losses:
    cutoff_index = floor(250 * (1 - 0.99)) = 2 (the 3rd worst historical trading day).
"""

import sys
import os
import csv
import math
import argparse
from datetime import date

def load_historical_returns(csv_path="data/returns_250d.csv"):
    """
    Reads 250 consecutive daily historical return scenarios from data/returns_250d.csv.
    Extracts provenance metadata (source, date range) from header comments.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Historical returns dataset not found at {csv_path}")

    metadata = []
    dates = []
    returns_by_symbol = {}

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            if row[0].startswith('#'):
                metadata.append(row[0].lstrip('#').strip())
                continue
            
            # Header row
            if 'date' in row:
                symbols = [s.strip() for s in row[1:]]
                for s in symbols:
                    returns_by_symbol[s] = []
                continue

            # Data row
            d = row[0].strip()
            dates.append(d)
            for idx, s in enumerate(symbols, 1):
                returns_by_symbol[s].append(float(row[idx]))

    return {
        "metadata": metadata,
        "dates": dates,
        "returns": returns_by_symbol,
        "symbols": list(returns_by_symbol.keys()),
        "scenario_count": len(dates)
    }

def calculate_historical_var(positions, returns_dataset, confidence_level=0.99):
    """
    Computes 1-Day VaR via Historical Simulation using real 250-day return scenarios.
    positions: list of dicts: {'symbol': 'AAPL', 'net_qty': 1000, 'mark_price': 15000} (price in cents)
    confidence_level: 0.99 (99% 1-Day VaR)
    """
    if not positions:
        return {"total_notional_dollars": 0, "var_99_1d_dollars": 0.0, "scenarios": 0}

    # Total portfolio notional in cents
    total_notional_cents = sum(p["net_qty"] * p["mark_price"] for p in positions)
    scenarios_count = returns_dataset["scenario_count"]
    dates = returns_dataset["dates"]
    returns_by_symbol = returns_dataset["returns"]

    # Calculate portfolio PnL for each of the 250 historical trading days
    scenario_results = [] # list of (pnl_cents, date_str)

    for i in range(scenarios_count):
        scenario_pnl = 0.0
        scenario_date = dates[i]

        for p in positions:
            sym = p["symbol"]
            pos_notional = float(p["net_qty"] * p["mark_price"])
            
            # Use specific asset return if available, fallback to SPY benchmark
            if sym in returns_by_symbol:
                asset_ret = returns_by_symbol[sym][i]
            elif "SPY" in returns_by_symbol:
                asset_ret = returns_by_symbol["SPY"][i]
            else:
                asset_ret = 0.0

            asset_pnl = pos_notional * asset_ret
            scenario_pnl += asset_pnl

        scenario_results.append((scenario_pnl, scenario_date))

    # Sort scenarios from worst loss (most negative PnL) to highest gain
    scenario_results.sort(key=lambda x: x[0])

    # Quantile cutoff: floor(250 * (1 - 0.99)) = 2 (3rd worst day)
    cutoff_index = int(math.floor(scenarios_count * (1.0 - confidence_level)))
    cutoff_pnl, cutoff_date = scenario_results[cutoff_index]

    # Worst historical day
    worst_pnl, worst_date = scenario_results[0]
    median_pnl, median_date = scenario_results[scenarios_count // 2]

    var_cents = max(0, -int(cutoff_pnl))
    var_dollars = var_cents / 100.0

    return {
        "total_notional_cents": total_notional_cents,
        "total_notional_dollars": total_notional_cents / 100.0,
        "var_99_1d_cents": var_cents,
        "var_99_1d_dollars": var_dollars,
        "var_percent": (var_cents / total_notional_cents * 100.0) if total_notional_cents > 0 else 0.0,
        "var_cutoff_scenario_date": cutoff_date,
        "worst_scenario_loss_dollars": max(0.0, -worst_pnl) / 100.0,
        "worst_scenario_date": worst_date,
        "median_scenario_pnl_dollars": median_pnl / 100.0,
        "scenario_count": scenarios_count,
        "confidence_level": confidence_level,
        "dataset_metadata": returns_dataset["metadata"]
    }

def generate_synthetic_scenarios(positions, scenarios_count=250, confidence_level=0.99, seed=42):
    """
    [LABELED TEST FIXTURE]
    Synthetic random Gaussian return generator used exclusively for standalone unit tests
    when live historical market data files are unavailable.
    """
    import random
    rng = random.Random(seed)
    total_notional = sum(p["net_qty"] * p["mark_price"] for p in positions)
    scenario_pnls = []

    for s in range(scenarios_count):
        scenario_pnl = 0
        market_shock = rng.gauss(0.0003, 0.012)
        for p in positions:
            pos_notional = p["net_qty"] * p["mark_price"]
            asset_pnl = pos_notional * (market_shock + rng.gauss(0.0, 0.015))
            scenario_pnl += asset_pnl
        scenario_pnls.append(scenario_pnl)

    scenario_pnls.sort()
    cutoff_index = int(math.floor(scenarios_count * (1.0 - confidence_level)))
    cutoff_pnl = scenario_pnls[cutoff_index]
    var_cents = max(0, -int(cutoff_pnl))

    return {
        "fixture_label": "SYNTHETIC_TEST_FIXTURE",
        "total_notional_dollars": total_notional / 100.0,
        "var_99_1d_dollars": var_cents / 100.0,
        "var_percent": (var_cents / total_notional * 100.0) if total_notional > 0 else 0.0,
        "scenario_count": scenarios_count
    }

def load_positions_from_postgres(pg_url):
    try:
        import psycopg
        with psycopg.connect(pg_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT account, symbol, net_qty, avg_cost FROM positions WHERE net_qty != 0;")
                rows = cur.fetchall()
                positions = []
                for r in rows:
                    positions.append({
                        "account": r[0],
                        "symbol": r[1],
                        "net_qty": int(r[2]),
                        "mark_price": int(r[3])
                    })
                return positions
    except Exception as e:
        print(f"[VaR] Notice: PostgreSQL connection to {pg_url} failed ({e}). Using institutional benchmark portfolio.")
        return None

def main():
    parser = argparse.ArgumentParser(description="Tradeflow 1-Day 99% Historical Simulation VaR Engine")
    parser.add_argument("--returns", default="data/returns_250d.csv", help="Path to 250-day returns CSV")
    parser.add_argument("--pg", default=os.getenv("POSTGRES_URL", "postgres://tradeflow:tradeflow_password@localhost:5432/tradeflow?sslmode=disable"),
                        help="PostgreSQL connection string")
    parser.add_argument("--synthetic", action="store_true", help="Run with labeled synthetic fixture instead of real data")
    args = parser.parse_args()

    print("=====================================================================")
    print("      TRADEFLOW HISTORICAL SIMULATION 1-DAY 99% VaR ANALYTICS        ")
    print("=====================================================================")

    # 1. Load portfolio positions from PostgreSQL (or use benchmark portfolio)
    positions = load_positions_from_postgres(args.pg)
    if not positions:
        positions = [
            {"account": "ACCT-ALPHA", "symbol": "AAPL", "net_qty": 300, "mark_price": 15000},  # $45,000
            {"account": "ACCT-ALPHA", "symbol": "MSFT", "net_qty": 200, "mark_price": 40000},  # $80,000
            {"account": "ACCT-ALPHA", "symbol": "NVDA", "net_qty": 100, "mark_price": 90000},  # $90,000
        ]
        print("[VaR] Evaluated on Institutional Benchmark Portfolio (AAPL, MSFT, NVDA).")
    else:
        print(f"[VaR] Successfully loaded {len(positions)} live positions from PostgreSQL database.")

    # 2. Compute VaR
    if args.synthetic:
        print("\n[MODE: LABELED SYNTHETIC TEST FIXTURE]")
        res = generate_synthetic_scenarios(positions)
        print(f"Total Notional:     ${res['total_notional_dollars']:,.2f}")
        print(f"1-Day 99% VaR:      ${res['var_99_1d_dollars']:,.2f} ({res['var_percent']:.2f}%)")
        print(f"Scenarios:          {res['scenario_count']} synthetic Gaussian simulations")
    else:
        print("\n[MODE: REAL 250-DAY HISTORICAL MARKET RETURNS]")
        dataset = load_historical_returns(args.returns)
        for meta in dataset["metadata"]:
            print(f"  • {meta}")

        res = calculate_historical_var(positions, dataset)
        print("---------------------------------------------------------------------")
        print(f"Portfolio Total Notional:   ${res['total_notional_dollars']:,.2f}")
        print(f"Historical Sessions:        {res['scenario_count']} trading days")
        print(f"Confidence Level:           {res['confidence_level']*100:.0f}% (1-Day Horizon)")
        print("---------------------------------------------------------------------")
        print(f"1-Day 99% Historical VaR:   ${res['var_99_1d_dollars']:,.2f} ({res['var_percent']:.2f}% of portfolio)")
        print(f"VaR Cutoff Scenario Date:   {res['var_cutoff_scenario_date']} (3rd worst session)")
        print(f"Worst Historical Shock:     ${res['worst_scenario_loss_dollars']:,.2f} (occurred on {res['worst_scenario_date']})")
        print(f"Median Daily Historical PnL:${res['median_scenario_pnl_dollars']:,.2f}")
    print("=====================================================================")

if __name__ == "__main__":
    main()
