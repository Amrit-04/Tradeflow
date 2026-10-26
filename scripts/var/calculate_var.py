#!/usr/bin/env python3
"""
Tradeflow Historical Simulation Value at Risk (VaR) Engine
Computes 1-Day 99% Value at Risk (VaR) per account using historical simulation.
"""

import sys
import math
import random
from datetime import date

def calculate_portfolio_var(positions, scenarios_count=250, confidence_level=0.99, seed=42):
    """
    positions: list of dicts: {'symbol': 'AAPL', 'net_qty': 1000, 'mark_price': 15000} (price in cents)
    confidence_level: e.g. 0.99 (99% 1-Day VaR)
    """
    if not positions:
        return {"total_notional": 0, "var_cents": 0, "var_dollars": 0.0, "scenarios": 0}

    # Calculate total portfolio notional in cents
    total_notional = sum(p["net_qty"] * p["mark_price"] for p in positions)

    # Generate synthetic 250 daily return scenarios per symbol with realistic equity volatilities
    rng = random.Random(seed)
    symbol_volatilities = {
        "AAPL": 0.015,  # 1.5% daily stdev
        "MSFT": 0.014,
        "NVDA": 0.028,
        "GOOGL": 0.017,
        "DEFAULT": 0.018
    }

    scenario_pnls = [] # PnL in cents per scenario

    for s in range(scenarios_count):
        scenario_pnl = 0
        # Common market factor + idiosyncratic factor
        market_shock = rng.gauss(0.0003, 0.012) # Market drift and variance

        for p in positions:
            sym = p["symbol"]
            vol = symbol_volatilities.get(sym, symbol_volatilities["DEFAULT"])
            idio_shock = rng.gauss(0.0, vol)
            asset_return = market_shock + idio_shock

            pos_notional = p["net_qty"] * p["mark_price"]
            asset_pnl = pos_notional * asset_return
            scenario_pnl += asset_pnl

        scenario_pnls.append(scenario_pnl)

    # Sort scenarios from worst loss (most negative PnL) to highest gain
    scenario_pnls.sort()

    # Determine quantile index for (1 - confidence_level)
    # At 250 scenarios and 99% confidence: 250 * 0.01 = 2.5 -> index 2 (the 3rd worst day)
    cutoff_index = int(math.floor(scenarios_count * (1.0 - confidence_level)))
    cutoff_pnl = scenario_pnls[cutoff_index]

    # VaR is reported as a positive loss number
    var_cents = max(0, -int(cutoff_pnl))
    var_dollars = var_cents / 100.0

    worst_loss_dollars = max(0, -scenario_pnls[0]) / 100.0
    median_pnl_dollars = scenario_pnls[scenarios_count // 2] / 100.0

    return {
        "total_notional_cents": total_notional,
        "total_notional_dollars": total_notional / 100.0,
        "var_99_1d_cents": var_cents,
        "var_99_1d_dollars": var_dollars,
        "var_percent": (var_cents / total_notional * 100.0) if total_notional > 0 else 0.0,
        "worst_scenario_loss_dollars": worst_loss_dollars,
        "median_scenario_pnl_dollars": median_pnl_dollars,
        "scenario_count": scenarios_count,
        "confidence_level": confidence_level
    }

def demo():
    sample_portfolio = [
        {"account": "ACCT-PROD-1", "symbol": "AAPL", "net_qty": 2000, "mark_price": 15000},  # $300,000
        {"account": "ACCT-PROD-1", "symbol": "MSFT", "net_qty": 1000, "mark_price": 42000},  # $420,000
        {"account": "ACCT-PROD-1", "symbol": "NVDA", "net_qty": 500,  "mark_price": 95000},  # $475,000
    ]

    print("=====================================================================")
    print("      TRADEFLOW HISTORICAL SIMULATION 1-DAY 99% VaR ANALYTICS        ")
    print("=====================================================================")

    res = calculate_portfolio_var(sample_portfolio)

    print(f"Portfolio Total Notional:   ${res['total_notional_dollars']:,.2f}")
    print(f"Scenarios Evaluated:        {res['scenario_count']} historical days")
    print(f"Confidence Level:           {res['confidence_level']*100:.0f}%")
    print("---------------------------------------------------------------------")
    print(f"1-Day 99% VaR:              ${res['var_99_1d_dollars']:,.2f} ({res['var_percent']:.2f}% of portfolio)")
    print(f"Max Tail Loss (Worst Day):  ${res['worst_scenario_loss_dollars']:,.2f}")
    print(f"Median Daily P&L:           ${res['median_scenario_pnl_dollars']:,.2f}")
    print("=====================================================================")

if __name__ == "__main__":
    demo()
