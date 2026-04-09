"""
stage_04_sensitivity.py
Pipeline Stage 4 — Sensitivity Analysis
Applies driver uplifts to total_high, re-runs MC per scenario,
produces tornado chart and comparison outputs.
"""
import json
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path
from scipy.stats import norm as scipy_norm
from scipy.linalg import cholesky


SCENARIOS = {
    'MAT':    {'label': 'Materials (+40%)',         'driver': 'MAT',    'uplift': 0.40},
    'LAB':    {'label': 'Labour (+25%)',             'driver': 'LAB',    'uplift': 0.25},
    'FUEL':   {'label': 'Fuel & Logistics (+50%)',   'driver': 'FUEL',   'uplift': 0.50},
    'SPEC':   {'label': 'Specialist/Bespoke (+35%)', 'driver': 'SPEC',   'uplift': 0.35},
    'ENERGY': {'label': 'Energy Prices (+35%)',      'driver': 'ENERGY', 'uplift': 0.35},
    'REG':    {'label': 'Regulatory (+30%)',         'driver': 'REG',    'uplift': 0.30},
    'ALL':    {
        'label': 'Combined Stress (all drivers)',
        'driver': 'ALL',
        'uplifts': {
            'MAT': 0.35, 'LAB': 0.20, 'FUEL': 0.40,
            'SPEC': 0.30, 'ENERGY': 0.30, 'REG': 0.20
        }
    },
}


def beta_weights(n):
    t = np.linspace(0.5/n, 1-0.5/n, n)
    w = t * (1-t)
    return w / w.sum()

def flat_weights(n):
    return np.ones(n) / n

def get_cpi_factor(month_idx, base_year, cpi_factors, inflation_rate):
    spend_year = base_year + month_idx // 12
    if str(spend_year) in cpi_factors:
        return cpi_factors[str(spend_year)]
    last       = max(int(k) for k in cpi_factors)
    last_f     = cpi_factors[str(last)]
    return last_f * (1 + inflation_rate) ** (spend_year - last)

def triangular_from_uniform(u, low, ml, high):
    low, ml, high = float(low), float(ml), float(high)
    if high == low:
        return np.full_like(u, ml)
    fc = (ml - low) / (high - low)
    return np.where(
        u < fc,
        low  + np.sqrt(np.maximum(u * (high-low) * (ml-low), 0)),
        high - np.sqrt(np.maximum((1-u) * (high-low) * (high-ml), 0))
    )

def apply_uplift(df_in, scenario):
    df_s = df_in.copy()
    if scenario['driver'] == 'ALL':
        for driver, uplift in scenario['uplifts'].items():
            mask = df_s['sens_drivers'].str.contains(driver, na=False, regex=False)
            df_s.loc[mask, 'total_high'] *= (1 + uplift)
    else:
        mask = df_s['sens_drivers'].str.contains(
            scenario['driver'], na=False, regex=False
        )
        df_s.loc[mask, 'total_high'] *= (1 + scenario['uplift'])
    return df_s


def run(cfg, logger):
    results_dir = Path(cfg["results_dir"])
    data_dir    = Path(cfg["data_dir"])
    results_dir.mkdir(exist_ok=True)

    logger.info("Stage 04 — Sensitivity Analysis starting")

    with open(data_dir / "assumptions.json") as f:
        A = json.load(f)
    with open(results_dir / "mc_summary.json") as f:
        base_summary = json.load(f)

    df       = pd.read_csv(data_dir / "clean_inputs.csv")
    df_gantt = pd.read_csv(data_dir / "gantt_map.csv")

    BASE_P50 = base_summary["total_p50"]
    BASE_P90 = base_summary["total_p90"]
    BASE_P10 = base_summary["total_p10"]

    N    = A["n_simulations"]
    SEED = A["random_seed"]
    np.random.seed(SEED)

    # ── Correlation ───────────────────────────────────────────
    drivers      = A["driver_order"]
    driver_index = {d: i for i, d in enumerate(drivers)}
    corr_matrix  = np.array(A["corr_matrix"])
    L            = cholesky(corr_matrix, lower=True)
    z            = np.random.standard_normal((len(drivers), N))
    corr_u       = scipy_norm.cdf(L @ z)

    gantt_bld = df_gantt[df_gantt["inservice_year"].isna()].set_index("gantt_index")
    gantt_is  = df_gantt[df_gantt["inservice_year"].notna()]
    N_MONTHS  = A["total_model_months"]

    def run_mc(df_scenario):
        matrix  = np.zeros((N, N_MONTHS))
        for _, row in df_scenario.iterrows():
            low = row["total_low"]; ml = row["total_ml"]; high = row["total_high"]
            if pd.isna(low) or pd.isna(ml) or pd.isna(high): continue
            if high <= low:
                samples = np.full(N, ml)
            else:
                drv_str = str(row.get("sens_drivers","") or "")
                primary = drv_str.split(",")[0].strip() if drv_str else ""
                u = (corr_u[driver_index[primary]]
                     if primary in driver_index
                     else np.random.uniform(0, 1, N))
                samples = triangular_from_uniform(u, low, ml, high)

            g_idx   = int(row["gantt_index"])
            is_year = row.get("inservice_year")
            if pd.notna(is_year):
                match = gantt_is[gantt_is["inservice_year"] == int(is_year)]
                if len(match) == 0: continue
                start_m = int(match.iloc[0]["start_month"])
                end_m   = int(match.iloc[0]["end_month"])
            else:
                if g_idx not in gantt_bld.index: continue
                val     = gantt_bld.loc[g_idx, "start_month"]
                start_m = int(val.iloc[0] if hasattr(val,"iloc") else val)
                val     = gantt_bld.loc[g_idx, "end_month"]
                end_m   = int(val.iloc[0] if hasattr(val,"iloc") else val)

            n_active = end_m - start_m + 1
            if n_active <= 0: continue
            phase   = str(row.get("phase",""))
            weights = (flat_weights(n_active)
                       if any(p in phase for p in ["Phase 1","Phase 2","Phase 5"])
                       else beta_weights(n_active))
            for m_offset, w in enumerate(weights):
                m_idx = (start_m - 1) + m_offset
                if m_idx >= N_MONTHS: break
                cpi = get_cpi_factor(m_idx, A["base_year"],
                                     A["cpi_factors"], A["inflation_rate"])
                matrix[:, m_idx] += samples * w * cpi
        return matrix.sum(axis=1)

    # ── Run all scenarios ─────────────────────────────────────
    results = [{"scenario":"BASE","label":"Base (no uplift)",
                "p10":round(BASE_P10),"p50":round(BASE_P50),
                "p90":round(BASE_P90),"p50_swing":0,"p90_swing":0,
                "rows_affected":0}]

    for key, scenario in SCENARIOS.items():
        df_mod = apply_uplift(df, scenario)
        if scenario["driver"] == "ALL":
            mask = df["sens_drivers"].str.contains(
                "|".join(scenario["uplifts"].keys()), na=False, regex=True)
        else:
            mask = df["sens_drivers"].str.contains(
                scenario["driver"], na=False, regex=False)

        totals = run_mc(df_mod)
        p10, p50, p90 = (np.percentile(totals, p) for p in (10, 50, 90))
        results.append({
            "scenario": key, "label": scenario["label"],
            "p10": round(p10), "p50": round(p50), "p90": round(p90),
            "p50_swing": round(p50 - BASE_P50),
            "p90_swing": round(p90 - BASE_P90),
            "rows_affected": int(mask.sum()),
        })
        logger.info(f"  {key:<8} P50=£{p50:,.0f}  swing=£{p50-BASE_P50:+,.0f}"
                    f"  ({mask.sum()} rows)")

    df_res = pd.DataFrame(results)
    df_res.to_csv(results_dir / "sensitivity_results.csv", index=False)

    # ── Summary ───────────────────────────────────────────────
    single   = df_res[(df_res["scenario"] != "BASE") & (df_res["scenario"] != "ALL")]
    worst    = single.nlargest(1, "p50_swing").iloc[0]
    combined = df_res[df_res["scenario"] == "ALL"].iloc[0]

    sens_summary = {
        "base_p50":               round(BASE_P50),
        "base_p90":               round(BASE_P90),
        "worst_single_driver":    worst["scenario"],
        "worst_single_p50_swing": int(worst["p50_swing"]),
        "combined_p50":           int(combined["p50"]),
        "combined_p90":           int(combined["p90"]),
        "combined_p50_swing":     int(combined["p50_swing"]),
        "combined_p90_swing":     int(combined["p90_swing"]),
        "n_simulations":          N,
        "scenario":               cfg.get("scenario_name", "Base"),
    }
    with open(results_dir / "sensitivity_summary.json", "w") as f:
        json.dump(sens_summary, f, indent=2)

    # ── Charts ────────────────────────────────────────────────
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle("Ranch Build — Sensitivity Analysis",
                 fontsize=13, fontweight="bold")

    ax1   = axes[0]
    sngl  = df_res[(df_res["scenario"] != "BASE") &
                   (df_res["scenario"] != "ALL")].sort_values("p50_swing")
    cols  = ["#C55A11" if v > 0 else "#2E75B6" for v in sngl["p50_swing"]]
    bars  = ax1.barh(sngl["label"], sngl["p50_swing"]/1000,
                     color=cols, alpha=0.85, edgecolor="white")
    ax1.axvline(0, color="black", linewidth=0.8)
    ax1.set_xlabel("P50 Cost Swing vs Base (£000s)")
    ax1.set_title("Tornado Chart — P50 Impact per Driver")
    ax1.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"£{x:,.0f}k"))
    for bar, val in zip(bars, sngl["p50_swing"]):
        ax1.text(bar.get_width() + max(sngl["p50_swing"]) * 0.02,
                 bar.get_y() + bar.get_height()/2,
                 f"£{val/1000:.0f}k", va="center", fontsize=9)

    ax2 = axes[1]
    x   = np.arange(len(df_res))
    w   = 0.25
    ax2.bar(x-w, df_res["p10"]/1e6, w, label="P10",
            color="#70AD47", alpha=0.8)
    ax2.bar(x,   df_res["p50"]/1e6, w, label="P50",
            color="#2E75B6", alpha=0.8)
    ax2.bar(x+w, df_res["p90"]/1e6, w, label="P90",
            color="#C55A11", alpha=0.8)
    ax2.axhline(BASE_P50/1e6, color="#1F3864", linestyle="--",
                linewidth=1.2, label=f"Base P50")
    ax2.set_xticks(x)
    ax2.set_xticklabels(
        ["Base","MAT","LAB","FUEL","SPEC","ENERGY","REG","Combined"],
        fontsize=8, rotation=30, ha="right")
    ax2.set_ylabel("Total Project Cost (£m)")
    ax2.set_title("P10 / P50 / P90 by Scenario")
    ax2.legend(fontsize=9)
    ax2.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"£{x:.1f}m"))

    plt.tight_layout()
    plt.savefig(results_dir / "sensitivity_charts.png",
                dpi=150, bbox_inches="tight")
    plt.close()

    logger.info(f"Worst single driver: {worst['scenario']} "
                f"(+£{worst['p50_swing']:,.0f})")
    logger.info(f"Combined stress P50: £{combined['p50']:,.0f} "
                f"(+£{combined['p50_swing']:,.0f})")
    logger.info("Chart saved: results/sensitivity_charts.png")
    logger.info("Stage 04 complete")

    return sens_summary
