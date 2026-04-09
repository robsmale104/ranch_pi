"""
Test script for Notebook 01 — Data Preparation
Run after notebook 01: python tests/test_01_data_prep.py
Source file: Self_Build_Costs_Data.xlsx
"""
import pandas as pd
import numpy as np
import json, os, sys

def run_all():
    errors = []
    warnings = []
    print("=" * 55)
    print("TEST SUITE — Notebook 01: Data Preparation")
    print("=" * 55)

    required = ["data/clean_inputs.csv","data/gantt_map.csv",
                "data/risks.csv","data/assumptions.json"]
    for f in required:
        if not os.path.exists(f):
            errors.append(f"T1-01 FAIL: Missing — {f}")
        else:
            print(f"T1-01 PASS: {f} exists")
    if errors:
        _summary(errors, warnings); return

    df    = pd.read_csv("data/clean_inputs.csv")
    gantt = pd.read_csv("data/gantt_map.csv")
    risks = pd.read_csv("data/risks.csv")
    with open("data/assumptions.json") as f:
        assum = json.load(f)

    bad = df[df["row_type"] != "INPUT"]
    if len(bad): errors.append(f"T1-02 FAIL: {len(bad)} non-INPUT rows")
    else: print(f"T1-02 PASS: All {len(df)} rows are INPUT type")

    build = df[df["inservice_year"].isna()].copy()
    cost  = build.dropna(subset=["total_low","total_ml","total_high"])
    bad   = cost[(cost["total_low"] > cost["total_ml"]) | (cost["total_ml"] > cost["total_high"])]
    if len(bad): errors.append(f"T1-03 FAIL: {len(bad)} rows violate Low<=ML<=High")
    else: print("T1-03 PASS: All build rows satisfy Low <= ML <= High")

    dupes = build[build["cbs_code"].duplicated()]
    if len(dupes): errors.append(f"T1-04 FAIL: {len(dupes)} duplicate CBS codes")
    else: print("T1-04 PASS: No duplicate CBS codes")

    no_idx = df[df["gantt_index"].isna()]
    if len(no_idx): errors.append(f"T1-05 FAIL: {len(no_idx)} rows missing gantt_index")
    else: print("T1-05 PASS: All rows have gantt_index")

    no_dur = build[build["dur_ml_days"].isna()]
    if len(no_dur): warnings.append(f"T1-06 WARN: {len(no_dur)} build rows missing dur_ml_days")
    else: print("T1-06 PASS: All build rows have duration ML")

    inservice    = df[df["inservice_year"].notna()]
    exp_start    = assum["inservice_start_yr"]
    exp_end      = assum["inservice_end_yr"]
    expected_yrs = list(range(exp_start, exp_end + 1))
    actual_yrs   = sorted(inservice["inservice_year"].unique())
    if actual_yrs != expected_yrs:
        errors.append(f"T1-07 FAIL: In-service years mismatch")
    else:
        n_lines = len(assum["inservice_cbs"])
        n_years = len(expected_yrs)
        if len(inservice) != n_lines * n_years:
            errors.append(f"T1-07 FAIL: {len(inservice)} rows, expected {n_lines*n_years}")
        else:
            print(f"T1-07 PASS: In-service {n_lines} lines x {n_years} years = {len(inservice)} rows")

    bad_g = gantt[gantt["start_month"] > gantt["end_month"]]
    if len(bad_g): errors.append(f"T1-08 FAIL: {len(bad_g)} Gantt rows start > end")
    else: print(f"T1-08 PASS: Gantt map valid ({len(gantt)} rows)")

    is_gantt = gantt[gantt["gantt_index"] == 0]
    if len(is_gantt) != len(expected_yrs):
        errors.append(f"T1-09 FAIL: {len(is_gantt)} in-service Gantt rows, expected {len(expected_yrs)}")
    else: print(f"T1-09 PASS: {len(is_gantt)} in-service Gantt rows (2029–2040)")

    required_keys = ["n_simulations","base_year","inflation_rate","cpi_factors",
                     "corr_matrix","driver_order","inservice_start_yr",
                     "inservice_end_yr","inservice_cbs"]
    missing_keys = [k for k in required_keys if k not in assum]
    if missing_keys: errors.append(f"T1-10 FAIL: Missing keys: {missing_keys}")
    else: print("T1-10 PASS: All required assumption keys present")

    build_ml = build["total_ml"].sum()
    if build_ml < 1_000_000 or build_ml > 20_000_000:
        warnings.append(f"T1-11 WARN: Build ML £{build_ml:,.0f} outside £1m–£20m range")
    else: print(f"T1-11 PASS: Build ML total = £{build_ml:,.0f}")

    annual_is = df[df["inservice_year"] == exp_start]["total_ml"].sum()
    if annual_is < 50_000 or annual_is > 1_000_000:
        warnings.append(f"T1-12 WARN: Annual in-service ML £{annual_is:,.0f} outside expected range")
    else: print(f"T1-12 PASS: Annual in-service ML = £{annual_is:,.0f}")

    _summary(errors, warnings)

def _summary(errors, warnings):
    print(f"\n{'='*55}")
    if warnings:
        print(f"WARNINGS ({len(warnings)}):")
        for w in warnings: print(f"  ⚠️  {w}")
    if errors:
        print(f"\nFAILURES ({len(errors)}):")
        for e in errors: print(f"  ❌ {e}")
        print("\n❌ Fix failures before running Notebook 02")
        sys.exit(1)
    else:
        print("✅ All tests passed — safe to run Notebook 02")

if __name__ == "__main__":
    run_all()
