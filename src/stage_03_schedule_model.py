"""
stage_03_schedule_model.py
Pipeline Stage 3 — Schedule Risk Model
Inputs:  data/clean_inputs.csv, data/gantt_map.csv, data/assumptions.json
Outputs: results/schedule_mc_results.csv, results/schedule_summary.json,
         results/schedule_model_charts.png
"""
import pandas as pd
import numpy as np
import json
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path

PHASE_STREAMS = {
    "phase1_end":  [2, 3, 4, 5],
    "phase2_end":  [7, 8, 9, 10, 11, 12, 13],
    "phase3_end":  list(range(16, 67)),
    "phase4_end":  [64, 65, 66],
    "project_end": list(range(2, 67)),
}


def run(cfg, logger):
    results_dir = Path(cfg["results_dir"])
    results_dir.mkdir(exist_ok=True)
    data_dir    = Path(cfg["data_dir"])

    logger.info("Stage 03 — Schedule Model starting")

    with open(data_dir / "assumptions.json") as f:
        A = json.load(f)

    df       = pd.read_csv(data_dir / "clean_inputs.csv")
    df_gantt = pd.read_csv(data_dir / "gantt_map.csv")

    build       = df[df["inservice_year"].isna()].copy()
    gantt_build = df_gantt[df_gantt["inservice_year"].isna()].copy()

    N              = A["n_simulations"]
    SEED           = A["random_seed"]
    DAILY_COST     = A["daily_delay_cost_gbp"]
    DAYS_PER_MONTH = A["days_per_month"]
    PLANNED_END    = A["programme_months"]

    np.random.seed(SEED)

    # ── Build stream duration lookup ──────────────────────────────
    valid_indices    = set(gantt_build["gantt_index"].astype(int).tolist())
    stream_durations = {}
    GANTT_DURATION_PHASES = {"P1", "P2"}

    for g_idx in valid_indices:
        g_row  = gantt_build[gantt_build["gantt_index"] == g_idx].iloc[0]
        cat    = str(g_row.get("cat", ""))
        months = float(g_row["duration_months"])

        # Phase 1 & 2: use Gantt duration directly.
        # Cost line durations represent fee periods not activity length.
        if cat in GANTT_DURATION_PHASES:
            stream_durations[g_idx] = (
                months * DAYS_PER_MONTH * 0.7,
                months * DAYS_PER_MONTH,
                months * DAYS_PER_MONTH * 1.4,
            )
            continue

        stream_rows = build[build["gantt_index"] == g_idx].dropna(
            subset=["dur_low_days","dur_ml_days","dur_high_days"]
        )
        if len(stream_rows) == 0:
            stream_durations[g_idx] = (
                months * DAYS_PER_MONTH * 0.7,
                months * DAYS_PER_MONTH,
                months * DAYS_PER_MONTH * 1.4,
            )
        else:
            stream_durations[g_idx] = (
                float(stream_rows["dur_low_days"].max()),
                float(stream_rows["dur_ml_days"].max()),
                float(stream_rows["dur_high_days"].max()),
            )

    logger.info(f"Stream durations built: {len(stream_durations)} streams")

    # ── MC loop ───────────────────────────────────────────────────
    phase_names   = list(PHASE_STREAMS.keys())
    phase_results = np.zeros((N, len(phase_names)))

    for sim in range(N):
        stream_end_months = {}
        for g_idx, (d_low, d_ml, d_high) in stream_durations.items():
            g_rows = gantt_build[gantt_build["gantt_index"] == g_idx]
            if len(g_rows) == 0:
                continue
            start_month  = float(g_rows.iloc[0]["start_month"])
            sampled_days = (d_ml if d_high <= d_low
                            else np.random.triangular(d_low, d_ml, d_high))
            stream_end_months[g_idx] = start_month + sampled_days / DAYS_PER_MONTH

        for pi, (phase_name, indices) in enumerate(PHASE_STREAMS.items()):
            ends = [stream_end_months[i] for i in indices if i in stream_end_months]
            phase_results[sim, pi] = max(ends) if ends else 0

    logger.info(f"Schedule MC complete: {N:,} simulations")

    # ── Results ───────────────────────────────────────────────────
    proj_idx  = phase_names.index("project_end")
    proj_ends = phase_results[:, proj_idx]
    p10_end   = float(np.percentile(proj_ends, 10))
    p50_end   = float(np.percentile(proj_ends, 50))
    p90_end   = float(np.percentile(proj_ends, 90))

    p50_delay = max(0.0, p50_end - PLANNED_END)
    p90_delay = max(0.0, p90_end - PLANNED_END)
    p50_cost  = p50_delay * DAYS_PER_MONTH * DAILY_COST
    p90_cost  = p90_delay * DAYS_PER_MONTH * DAILY_COST

    logger.info(f"P10={p10_end:.1f}m  P50={p50_end:.1f}m  P90={p90_end:.1f}m")
    logger.info(f"P50 delay: {p50_delay:.1f}m  cost: £{p50_cost:,.0f}")
    logger.info(f"P90 delay: {p90_delay:.1f}m  cost: £{p90_cost:,.0f}")

    # ── Save ──────────────────────────────────────────────────────
    df_sched = pd.DataFrame(phase_results, columns=phase_names)
    df_sched.insert(0, "simulation", range(N))
    df_sched.to_csv(results_dir / "schedule_mc_results.csv", index=False)

    summary = {
        "planned_end_months":  PLANNED_END,
        "project_p10_months":  round(p10_end, 1),
        "project_p50_months":  round(p50_end, 1),
        "project_p90_months":  round(p90_end, 1),
        "p50_delay_months":    round(p50_delay, 1),
        "p90_delay_months":    round(p90_delay, 1),
        "p50_delay_cost_gbp":  round(p50_cost),
        "p90_delay_cost_gbp":  round(p90_cost),
        "daily_delay_cost_gbp": DAILY_COST,
        "spread_months":        round(p90_end - p10_end, 2),
        "n_simulations":        N,
        "random_seed":          SEED,
    }
    with open(results_dir / "schedule_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    # ── Charts ────────────────────────────────────────────────────
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle("Ranch Build — Schedule Risk Model", fontsize=13, fontweight="bold")

    ax1 = axes[0]
    ax1.hist(proj_ends, bins=60, color="#7030A0", alpha=0.75, edgecolor="white")
    ax1.axvline(PLANNED_END, color="black",   ls=":", lw=1.5, label=f"Planned M{PLANNED_END}")
    ax1.axvline(p10_end,     color="#70AD47", ls="--",lw=1.5, label=f"P10 M{p10_end:.1f}")
    ax1.axvline(p50_end,     color="#1F3864", ls="-", lw=2.0, label=f"P50 M{p50_end:.1f}")
    ax1.axvline(p90_end,     color="#C55A11", ls="--",lw=1.5, label=f"P90 M{p90_end:.1f}")
    ax1.set_xlabel("Project End (months)")
    ax1.set_ylabel("Frequency")
    ax1.set_title("Project End Date Distribution")
    ax1.legend(fontsize=9)

    ax2 = axes[1]
    display  = ["phase1_end","phase2_end","phase3_end","phase4_end"]
    labels   = ["Phase 1\nPlanning","Phase 2\nDesign",
                "Phase 3\nBuild","Phase 4\nHandover"]
    x        = np.arange(len(display))
    p10s = [np.percentile(phase_results[:,phase_names.index(p)],10) for p in display]
    p50s = [np.percentile(phase_results[:,phase_names.index(p)],50) for p in display]
    p90s = [np.percentile(phase_results[:,phase_names.index(p)],90) for p in display]
    ax2.bar(x, p50s, color="#2E75B6", alpha=0.8, label="P50")
    ax2.errorbar(x, p50s,
                 yerr=[np.array(p50s)-np.array(p10s), np.array(p90s)-np.array(p50s)],
                 fmt="none", color="#1F3864", capsize=6, linewidth=2, label="P10–P90")
    ax2.set_xticks(x); ax2.set_xticklabels(labels, fontsize=9)
    ax2.set_ylabel("End Month")
    ax2.set_title("Phase End Dates (P10/P50/P90)")
    ax2.legend(fontsize=9)
    ax2.axhline(36, color="grey", ls=":", lw=1)

    plt.tight_layout()
    plt.savefig(results_dir / "schedule_model_charts.png", dpi=150, bbox_inches="tight")
    plt.close()
    logger.info("Chart saved: results/schedule_model_charts.png")
    logger.info("Stage 03 complete")

    return summary
