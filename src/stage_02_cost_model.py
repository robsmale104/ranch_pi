"""
stage_02_cost_model.py
Pipeline Stage 2 — Monte Carlo Cost Model
Inputs:  data/clean_inputs.csv, data/gantt_map.csv, data/assumptions.json
Outputs: results/cost_mc_results.csv, results/monthly_profile.npy,
         results/mc_summary.json, results/cost_model_charts.png
"""
import pandas as pd
import numpy as np
import json
import matplotlib
matplotlib.use("Agg")   # non-interactive backend for pipeline
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path
from scipy.stats import norm as scipy_norm
from scipy.linalg import cholesky


def beta_weights(n):
    t = np.linspace(0.5/n, 1-0.5/n, n)
    w = t * (1-t)
    return w / w.sum()

def flat_weights(n):
    return np.ones(n) / n

def get_cpi_factor(month_idx, base_year, cpi_factors, inflation_rate):
    spend_year = base_year + month_idx // 12
    key = str(spend_year)
    if key in cpi_factors:
        return cpi_factors[key]
    last_year   = max(int(k) for k in cpi_factors)
    last_factor = cpi_factors[str(last_year)]
    return last_factor * (1 + inflation_rate) ** (spend_year - last_year)

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


def run(cfg, logger):
    results_dir = Path(cfg["results_dir"])
    results_dir.mkdir(exist_ok=True)
    data_dir    = Path(cfg["data_dir"])

    logger.info("Stage 02 — Cost Model starting")

    with open(data_dir / "assumptions.json") as f:
        A = json.load(f)

    df       = pd.read_csv(data_dir / "clean_inputs.csv")
    df_gantt = pd.read_csv(data_dir / "gantt_map.csv")

    build     = df[df["inservice_year"].isna()].copy()
    gantt_bld = df_gantt[df_gantt["inservice_year"].isna()].set_index("gantt_index")
    gantt_is  = df_gantt[df_gantt["inservice_year"].notna()]

    N    = A["n_simulations"]
    SEED = A["random_seed"]
    np.random.seed(SEED)

    # ── Correlation setup ──────────────────────────────────────────
    drivers      = A["driver_order"]
    driver_index = {d: i for i, d in enumerate(drivers)}
    corr_matrix  = np.array(A["corr_matrix"])
    L            = cholesky(corr_matrix, lower=True)
    z            = np.random.standard_normal((len(drivers), N))
    corr_z       = L @ z
    corr_u       = scipy_norm.cdf(corr_z)
    logger.info(f"Correlation setup: {len(drivers)} drivers, Cholesky OK")

    # ── MC loop ───────────────────────────────────────────────────
    N_MONTHS       = A["total_model_months"]
    monthly_matrix = np.zeros((N, N_MONTHS))
    skipped        = 0

    for _, row in df.iterrows():
        low  = row["total_low"]
        ml   = row["total_ml"]
        high = row["total_high"]
        if pd.isna(low) or pd.isna(ml) or pd.isna(high):
            skipped += 1
            continue

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
            if len(match) == 0:
                skipped += 1; continue
            start_m = int(match.iloc[0]["start_month"])
            end_m   = int(match.iloc[0]["end_month"])
        else:
            if g_idx not in gantt_bld.index:
                skipped += 1; continue
            start_m = int(gantt_bld.loc[g_idx, "start_month"])
            end_m   = int(gantt_bld.loc[g_idx, "end_month"])

        n_active = end_m - start_m + 1
        if n_active <= 0:
            skipped += 1; continue

        phase = str(row.get("phase",""))
        weights = (flat_weights(n_active)
                   if any(p in phase for p in ["Phase 1","Phase 2","Phase 5"])
                   else beta_weights(n_active))

        for m_offset, w in enumerate(weights):
            m_idx = (start_m - 1) + m_offset
            if m_idx >= N_MONTHS:
                break
            cpi = get_cpi_factor(m_idx, A["base_year"],
                                  A["cpi_factors"], A["inflation_rate"])
            monthly_matrix[:, m_idx] += samples * w * cpi

    logger.info(f"MC loop complete — {len(df)-skipped} rows processed, "
                f"{skipped} skipped")

    # ── Results ───────────────────────────────────────────────────
    total_costs     = monthly_matrix.sum(axis=1)
    build_costs     = monthly_matrix[:, :36].sum(axis=1)
    inservice_costs = monthly_matrix[:, 36:].sum(axis=1)
    p10, p50, p90   = (np.percentile(total_costs, p) for p in (10, 50, 90))
    mean            = total_costs.mean()

    p50_monthly = np.percentile(monthly_matrix[:, :36], 50, axis=0)
    peak_month  = int(np.argmax(p50_monthly)) + 1

    logger.info(f"P10=£{p10:,.0f}  P50=£{p50:,.0f}  P90=£{p90:,.0f}")
    logger.info(f"P90/P50={p90/p50:.2f}x  Peak spend: M{peak_month}")

    # ── Save ──────────────────────────────────────────────────────
    pd.DataFrame({
        "simulation": range(N),
        "total_cost": total_costs,
        "build_cost": build_costs,
        "inservice_cost": inservice_costs,
    }).to_csv(results_dir / "cost_mc_results.csv", index=False)

    np.save(results_dir / "monthly_profile.npy", monthly_matrix)

    summary = {
        "n_simulations": N, "n_months": N_MONTHS,
        "total_p10": round(p10), "total_p50": round(p50),
        "total_p90": round(p90), "total_mean": round(mean),
        "build_p50": round(np.percentile(build_costs, 50)),
        "inservice_p50": round(np.percentile(inservice_costs, 50)),
        "p90_p50_ratio": round(p90/p50, 3),
        "inflation_rate": A["inflation_rate"],
        "random_seed": SEED,
        "sum_ml_inputs": round(build["total_ml"].sum()),
        "peak_month": peak_month,
        "scenario_name": A.get("scenario_name","Base"),
    }
    with open(results_dir / "mc_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    # ── Charts ────────────────────────────────────────────────────
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle("Ranch Build — Monte Carlo Cost Model", fontsize=13, fontweight="bold")

    ax1 = axes[0]
    ax1.hist(total_costs/1e6, bins=80, color="#2E75B6", alpha=0.75, edgecolor="white")
    for val, col, ls, lbl in [(p10,"#70AD47","--",f"P10 £{p10/1e6:.1f}m"),
                               (p50,"#1F3864","-", f"P50 £{p50/1e6:.1f}m"),
                               (p90,"#C55A11","--",f"P90 £{p90/1e6:.1f}m")]:
        ax1.axvline(val/1e6, color=col, linestyle=ls, linewidth=1.8, label=lbl)
    ax1.set_xlabel("Total Project Cost (£m)")
    ax1.set_ylabel("Frequency")
    ax1.set_title("Cost Distribution")
    ax1.legend(fontsize=9)
    ax1.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"£{x:.1f}m"))

    ax2 = axes[1]
    months  = np.arange(1, N_MONTHS+1)
    cum_p10 = np.percentile(monthly_matrix, 10, axis=0).cumsum()
    cum_p50 = np.percentile(monthly_matrix, 50, axis=0).cumsum()
    cum_p90 = np.percentile(monthly_matrix, 90, axis=0).cumsum()
    ax2.fill_between(months, cum_p10/1e6, cum_p90/1e6,
                     alpha=0.2, color="#2E75B6", label="P10–P90 band")
    ax2.plot(months, cum_p10/1e6, "--", color="#70AD47", linewidth=1.5)
    ax2.plot(months, cum_p50/1e6, "-",  color="#1F3864", linewidth=2.0, label="P50")
    ax2.plot(months, cum_p90/1e6, "--", color="#C55A11", linewidth=1.5)
    ax2.axvline(36, color="grey", linestyle=":", linewidth=1, label="Handover M36")
    ax2.set_xlabel("Month")
    ax2.set_ylabel("Cumulative Spend (£m)")
    ax2.set_title("S-Curve — Cumulative Spend")
    ax2.legend(fontsize=9)
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"£{x:.1f}m"))

    plt.tight_layout()
    plt.savefig(results_dir / "cost_model_charts.png", dpi=150, bbox_inches="tight")
    plt.close()
    logger.info("Chart saved: results/cost_model_charts.png")
    logger.info("Stage 02 complete")

    return summary
