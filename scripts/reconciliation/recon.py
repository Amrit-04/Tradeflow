"""
Tradeflow End-of-Day (EOD) Reconciliation
Compares internal positions and trades against external clearing house CSV.
Classifies breaks:
  - QTY_MISMATCH: Internal net quantity != clearing house net quantity
  - MISSING_TRADE: Trade reported by clearing house but absent internally
  - EXTRA_TRADE: Trade present in internal database but unconfirmed by clearing
"""

import sys
import argparse
import pandas as pd

def run_reconciliation(internal_trades_path: str, clearing_csv_path: str):
    print(f"[Recon] Loading internal trades: {internal_trades_path}")
    print(f"[Recon] Loading clearing records: {clearing_csv_path}")
    # In Week 6: Full pandas dataframe diffing, break classification, and DB writing
    print("[Recon] Reconciliation scaffold ready.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tradeflow EOD Reconciliation")
    parser.add_argument("--internal", default="internal_trades.csv", help="Internal trades file")
    parser.add_argument("--clearing", default="clearing_eod.csv", help="Clearing house file")
    args = parser.parse_args()
    run_reconciliation(args.internal, args.clearing)
