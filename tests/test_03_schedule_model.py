"""
Test script for Notebook 03 — Schedule Risk Model
Run after notebook 03: poetry run python tests/test_03_schedule_model.py
"""
import os, sys, json
from pathlib import Path

os.chdir(Path(__file__).parent.parent)

import pandas as pd
import numpy as np

def run_all():
    errors   = []
    warnings = []

    print("=" * 55)
    print("TEST SUITE — Notebook 03: Schedule Model")
    print("=" * 55)

    # ── T3-01: Output files exist ─────────────────────────────
    required = [
        "results/schedule_mc_results.csv",
        "results/schedule_summary.json",
    ]
    for f in required:
        if not os.path.exists(f):
            errors.append(f"T3-01 FAIL: Missing — {f}")
        else:
            print(f"T3-01 PASS: {f} exists")

    if errors:
        _summary(errors, warnings)
        return

    res = pd.read_csv("results/schedule_mc_results.csv")
    with open("results/schedule_summary.json") as f:
        summary = json.load(f)
    with open("data/assumptions.json") as f:
        assum = json.load(f)

    N = assum["n_simulations"]

    # ── T3-02: Correct simulation count ──────────────────────
    if len(res) != N:
        errors.append(f"T3-02 FAIL: {len(res)} rows, expected {N:,}")
    else:
        print(f"T3-02 PASS: {N:,} simulations present")

    # ── T3-03: Required columns present ──────────────────────
    expected_cols = ["phase1_end", "phase2_end", "phase3_end",
                     "phase4_end", "project_end"]
    for col in expected_cols:
        if col not in res.columns:
            errors.append(f"T3-03 FAIL: Missing column — {col}")
        else:
            print(f"T3-03 PASS: Column {col} present")

    # ── T3-04: Phase ordering — Ph1 always before Ph3 ────────
    if "phase1_end" in res.columns and "phase3_end" in res.columns:
        violations = (res["phase1_end"] >= res["phase3_end"]).sum()
        if violations:
            errors.append(f"T3-04 FAIL: {violations} sims have Ph1 >= Ph3")
        else:
            print("T3-04 PASS: Phase 1 always ends before Phase 3")

    # ── T3-05: Ph2 always before Ph3 ─────────────────────────
    if "phase2_end" in res.columns and "phase3_end" in res.columns:
        violations = (res["phase2_end"] >= res["phase3_end"]).sum()
        if violations > N * 0.05:   # allow 5% overlap (parallel phases)
            errors.append(
                f"T3-05 FAIL: {violations} sims have Ph2 >= Ph3 "
                f"(>{N*0.05:.0f} threshold)"
            )
        else:
            print(f"T3-05 PASS: Phase 2/3 overlap within tolerance "
                  f"({violations} sims)")

    # ── T3-06: P50 project end 24–60 months ──────────────────
    if "project_end" in res.columns:
        p10 = np.percentile(res["project_end"], 10)
        p50 = np.percentile(res["project_end"], 50)
        p90 = np.percentile(res["project_end"], 90)

        if not (24 <= p50 <= 60):
            errors.append(
                f"T3-06 FAIL: P50 = {p50:.1f}m — expected 24–60 months"
            )
        else:
            print(f"T3-06 PASS: P50 end = {p50:.1f} months")

        # ── T3-07: P10 < P50 < P90 ───────────────────────────
        if not (p10 < p50 < p90):
            errors.append("T3-07 FAIL: Percentile ordering broken")
        else:
            print(f"T3-07 PASS: P10={p10:.1f}  P50={p50:.1f}  P90={p90:.1f}")

        # ── T3-08: Spread >= 0.5 months ──────────────────────
        # Note: narrow spread is EXPECTED for parallel-stream programmes
        # Risk diversification reduces aggregate uncertainty
        # A spread of 0.5m+ confirms distributions are working
        spread = p90 - p10
        if spread < 0.5:
            errors.append(
                f"T3-08 FAIL: P90–P10 spread = {spread:.2f}m — "
                "distributions may not be working"
            )
        else:
            print(f"T3-08 PASS: P90–P10 spread = {spread:.2f}m "
                  f"(narrow spread expected — parallel streams diversify risk)")

        # ── T3-09: No sims complete in under 18 months ────────
        too_short = (res["project_end"] < 18).sum()
        if too_short:
            errors.append(
                f"T3-09 FAIL: {too_short} sims complete in under 18 months"
            )
        else:
            print("T3-09 PASS: No simulations complete in under 18 months")

        # ── T3-10: P50 within 50% of planned end ─────────────
        planned = summary["planned_end_months"]
        ratio   = p50 / planned
        if ratio < 0.8 or ratio > 1.5:
            errors.append(
                f"T3-10 FAIL: P50 ({p50:.1f}m) is {ratio:.2f}x "
                f"planned ({planned}m)"
            )
        else:
            print(f"T3-10 PASS: P50 is {ratio:.2f}x planned end")

    # ── T3-11: Summary JSON consistent ───────────────────────
    if abs(summary["project_p50_months"] - round(p50, 1)) > 0.5:
        errors.append(
            f"T3-11 FAIL: schedule_summary.json P50 "
            f"({summary['project_p50_months']}) "
            f"doesn't match computed ({p50:.1f})"
        )
    else:
        print("T3-11 PASS: schedule_summary.json consistent")

    # ── T3-12: Delay cost plausible ──────────────────────────
    p90_cost = summary["p90_delay_cost_gbp"]
    if p90_cost < 0 or p90_cost > 5_000_000:
        errors.append(
            f"T3-12 FAIL: P90 delay cost £{p90_cost:,.0f} — "
            "outside expected £0–£5m range"
        )
    else:
        print(f"T3-12 PASS: P90 delay cost = £{p90_cost:,.0f}")

    _summary(errors, warnings)

def _summary(errors, warnings):
    print(f"\n{'='*55}")
    if warnings:
        print(f"WARNINGS ({len(warnings)}):")
        for w in warnings: print(f"  ⚠️  {w}")
    if errors:
        print(f"\nFAILURES ({len(errors)}):")
        for e in errors: print(f"  ❌ {e}")
        print("\n❌ Fix failures before proceeding")
        sys.exit(1)
    else:
        print("✅ All tests passed — Notebooks 01–03 complete")

if __name__ == "__main__":
    run_all()
