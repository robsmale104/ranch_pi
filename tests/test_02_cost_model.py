"""
Test script for Notebook 02 — Cost Model & Monte Carlo
Run after notebook 02: poetry run python tests/test_02_cost_model.py
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
    print("TEST SUITE — Notebook 02: Cost Model")
    print("=" * 55)

    # ── T2-01: Output files exist ─────────────────────────────
    required = [
        "results/cost_mc_results.csv",
        "results/monthly_profile.npy",
        "results/mc_summary.json",
    ]
    for f in required:
        if not os.path.exists(f):
            errors.append(f"T2-01 FAIL: Missing — {f}")
        else:
            print(f"T2-01 PASS: {f} exists")

    if errors:
        _summary(errors, warnings)
        return

    # ── Load results ──────────────────────────────────────────
    res     = pd.read_csv("results/cost_mc_results.csv")
    matrix  = np.load("results/monthly_profile.npy")
    with open("results/mc_summary.json") as f:
        summary = json.load(f)
    with open("data/assumptions.json") as f:
        assum = json.load(f)
    inputs  = pd.read_csv("data/clean_inputs.csv")
    build   = inputs[inputs["inservice_year"].isna()]

    # ── T2-02: Correct simulation count ──────────────────────
    N = assum["n_simulations"]
    if len(res) != N:
        errors.append(f"T2-02 FAIL: {len(res)} rows, expected {N:,}")
    else:
        print(f"T2-02 PASS: {N:,} simulations present")

    # ── T2-03: Required columns ───────────────────────────────
    for col in ["total_cost", "build_cost", "inservice_cost"]:
        if col not in res.columns:
            errors.append(f"T2-03 FAIL: Missing column — {col}")
        else:
            print(f"T2-03 PASS: Column {col} present")

    # ── T2-04: No negative or zero totals ─────────────────────
    bad = (res["total_cost"] <= 0).sum()
    if bad:
        errors.append(f"T2-04 FAIL: {bad} simulations have zero/negative total")
    else:
        print("T2-04 PASS: All totals positive")

    # ── T2-05: P10 < P50 < P90 ───────────────────────────────
    p10 = np.percentile(res["total_cost"], 10)
    p50 = np.percentile(res["total_cost"], 50)
    p90 = np.percentile(res["total_cost"], 90)
    if not (p10 < p50 < p90):
        errors.append(f"T2-05 FAIL: Percentile ordering broken")
    else:
        print(f"T2-05 PASS: P10=£{p10:,.0f}  P50=£{p50:,.0f}  P90=£{p90:,.0f}")

    # ── T2-06: P50 within 20% of sum of ML inputs ────────────
    sum_ml   = build["total_ml"].sum()
    build_p50 = np.percentile(res["build_cost"], 50)
    diff_pct  = (build_p50 - sum_ml) / sum_ml * 100
    if abs(diff_pct) > 20:
        errors.append(
            f"T2-06 FAIL: Build P50 is {diff_pct:+.1f}% from sum of ML "
            f"(£{sum_ml:,.0f}) — expected within ±20%"
        )
    else:
        print(f"T2-06 PASS: Build P50 {diff_pct:+.1f}% vs sum of ML "
              f"(inflation uplift expected)")

    # ── T2-07: Inflation visible — P50 above sum of ML ───────
    if build_p50 < sum_ml:
        errors.append(
            f"T2-07 FAIL: Build P50 (£{build_p50:,.0f}) below sum of ML "
            f"(£{sum_ml:,.0f}) — check get_cpi_factor()"
        )
    else:
        uplift = (build_p50 - sum_ml) / sum_ml * 100
        print(f"T2-07 PASS: Inflation uplift = {uplift:.1f}%")

    # ── T2-08: P90/P50 ratio plausible ───────────────────────
    ratio = p90 / p50
    if ratio < 1.05 or ratio > 2.0:
        errors.append(
            f"T2-08 FAIL: P90/P50 = {ratio:.2f} — "
            "expected between 1.05 and 2.0"
        )
    else:
        print(f"T2-08 PASS: P90/P50 ratio = {ratio:.2f}")

    # ── T2-09: Matrix shape correct ──────────────────────────
    expected_shape = (N, assum["total_model_months"])
    if matrix.shape != expected_shape:
        errors.append(
            f"T2-09 FAIL: Matrix shape {matrix.shape}, "
            f"expected {expected_shape}"
        )
    else:
        print(f"T2-09 PASS: Matrix shape = {matrix.shape}")

    # ── T2-10: Matrix row sums match total_cost ───────────────
    matrix_totals = matrix.sum(axis=1)
    max_diff      = np.abs(matrix_totals - res["total_cost"].values).max()
    max_diff_pct  = max_diff / res["total_cost"].mean() * 100
    if max_diff_pct > 0.5:
        errors.append(
            f"T2-10 FAIL: Matrix row sums diverge from total_cost "
            f"by {max_diff_pct:.2f}%"
        )
    else:
        print(f"T2-10 PASS: Matrix row sums match total_cost "
              f"(max diff {max_diff_pct:.3f}%)")

    # ── T2-11: No negative values in matrix ──────────────────
    neg = (matrix < 0).sum()
    if neg:
        errors.append(f"T2-11 FAIL: {neg} negative values in matrix")
    else:
        print("T2-11 PASS: No negative values in matrix")

    # ── T2-12: Peak spend in plausible month ──────────────────
    p50_monthly = np.percentile(matrix[:, :36], 50, axis=0)
    peak_month  = int(np.argmax(p50_monthly)) + 1
    if peak_month < 8 or peak_month > 30:
        warnings.append(
            f"T2-12 WARN: Peak monthly spend at M{peak_month} "
            "— expected M8–M30"
        )
    else:
        print(f"T2-12 PASS: Peak monthly spend at M{peak_month}")

    # ── T2-13: Summary JSON matches computed values ───────────
    if abs(summary["total_p50"] - round(p50)) > 1000:
        errors.append(
            f"T2-13 FAIL: mc_summary.json P50 (£{summary['total_p50']:,}) "
            f"doesn't match computed (£{p50:,.0f})"
        )
    else:
        print("T2-13 PASS: mc_summary.json consistent with results")

    _summary(errors, warnings)

def _summary(errors, warnings):
    print(f"\n{'='*55}")
    if warnings:
        print(f"WARNINGS ({len(warnings)}):")
        for w in warnings: print(f"  ⚠️  {w}")
    if errors:
        print(f"\nFAILURES ({len(errors)}):")
        for e in errors: print(f"  ❌ {e}")
        print("\n❌ Fix failures before running Notebook 03")
        sys.exit(1)
    else:
        print("✅ All tests passed — safe to run Notebook 03")

if __name__ == "__main__":
    run_all()
