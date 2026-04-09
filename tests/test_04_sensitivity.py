"""
Test script for Notebook 04 — Sensitivity Analysis
Run after notebook 04: poetry run python tests/test_04_sensitivity.py
"""
import os, sys, json
from pathlib import Path

pass  # cwd set by pipeline

import pandas as pd
import numpy as np

def run_all():
    errors = []
    warnings = []
    print("=" * 55)
    print("TEST SUITE — Notebook 04: Sensitivity Analysis")
    print("=" * 55)

    required = ["results/sensitivity_results.csv","results/sensitivity_summary.json"]
    for f in required:
        if not os.path.exists(f): errors.append(f"T4-01 FAIL: Missing — {f}")
        else: print(f"T4-01 PASS: {f} exists")
    if errors: _summary(errors, warnings); return

    res = pd.read_csv("results/sensitivity_results.csv")
    with open("results/sensitivity_summary.json") as f: summary = json.load(f)
    with open("results/mc_summary.json") as f: base = json.load(f)

    # T4-02: All 8 scenarios present
    expected = ['BASE','MAT','LAB','FUEL','SPEC','ENERGY','REG','ALL']
    missing  = [s for s in expected if s not in res['scenario'].tolist()]
    if missing: errors.append(f"T4-02 FAIL: Missing scenarios: {missing}")
    else: print(f"T4-02 PASS: All {len(expected)} scenarios present")

    # T4-03: All scenario P50s >= base P50
    base_p50 = base['total_p50']
    non_base = res[res['scenario'] != 'BASE']
    below    = non_base[non_base['p50'] < base_p50]
    if len(below): errors.append(f"T4-03 FAIL: {len(below)} scenarios below base P50")
    else: print("T4-03 PASS: All scenario P50s >= base P50")

    # T4-04: P10 < P50 < P90 for all scenarios
    bad = res[(res['p10'] >= res['p50']) | (res['p50'] >= res['p90'])]
    if len(bad): errors.append(f"T4-04 FAIL: Ordering broken for: {bad['scenario'].tolist()}")
    else: print("T4-04 PASS: P10 < P50 < P90 for all scenarios")

    # T4-05: Worst single driver matches summary
    single = res[(res['scenario'] != 'BASE') & (res['scenario'] != 'ALL')]
    if len(single):
        worst = single.nlargest(1,'p50_swing').iloc[0]
        if worst['scenario'] != summary.get('worst_single_driver'):
            warnings.append(f"T4-05 WARN: Worst driver mismatch in summary JSON")
        else: print(f"T4-05 PASS: Worst driver = {worst['scenario']} (+£{worst['p50_swing']:,.0f})")

    # T4-06: Combined > all single drivers
    all_row = res[res['scenario']=='ALL']
    if len(all_row):
        all_p50    = all_row.iloc[0]['p50']
        max_single = single['p50'].max() if len(single) else 0
        if all_p50 <= max_single:
            errors.append(f"T4-06 FAIL: Combined P50 <= highest single driver")
        else: print(f"T4-06 PASS: Combined P50 £{all_p50:,.0f} > all single drivers")

    # T4-07: Swing values consistent
    swing_fails = []
    for _, row in res.iterrows():
        if row['scenario'] == 'BASE': continue
        expected_swing = round(row['p50']) - round(base_p50)
        if abs(row['p50_swing'] - expected_swing) > 1000:
            swing_fails.append(row['scenario'])
    if swing_fails: errors.append(f"T4-07 FAIL: Swing mismatch for {swing_fails}")
    else: print("T4-07 PASS: p50_swing values consistent")

    # T4-08: Summary JSON consistent with base
    if abs(summary['base_p50'] - base_p50) > 1000:
        errors.append(f"T4-08 FAIL: summary base_p50 inconsistent with mc_summary")
    else: print("T4-08 PASS: sensitivity_summary.json consistent")

    # T4-09: Row counts plausible
    fuel_row = res[res['scenario']=='FUEL']
    if len(fuel_row):
        n = fuel_row.iloc[0]['rows_affected']
        if n < 5 or n > 96: warnings.append(f"T4-09 WARN: FUEL affected {n} rows")
        else: print(f"T4-09 PASS: Row counts plausible (FUEL: {n} rows)")

    _summary(errors, warnings)

def _summary(errors, warnings):
    print(f"\n{'='*55}")
    if warnings:
        print(f"WARNINGS ({len(warnings)}):")
        for w in warnings: print(f"  ⚠️  {w}")
    if errors:
        print(f"\nFAILURES ({len(errors)}):")
        for e in errors: print(f"  ❌ {e}")
        sys.exit(1)
    else: print("✅ All tests passed — Sensitivity analysis validated")

if __name__ == "__main__":
    run_all()
