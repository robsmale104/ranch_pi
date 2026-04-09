"""
main.py
Entry point for the Ranch Self-Build Cost Engineering Pipeline.

Usage:
    poetry run python main.py
    poetry run python main.py --model config/stress_config.yaml
    poetry run python main.py --stage cost_model

Options:
    --model   Path to alternate model config YAML (default: config/model_config.yaml)
    --pipeline Path to pipeline config YAML (default: config/pipeline_config.yaml)
    --stage   Run a single stage only (data_prep | cost_model | schedule_model)
"""
import sys
import argparse
import time
from pathlib import Path

# Add src/ to path so stage modules can be imported
sys.path.insert(0, str(Path(__file__).parent / "src"))

from config_validator import load_and_validate
from pipeline import setup_logger, run_pipeline


def parse_args():
    parser = argparse.ArgumentParser(
        description="Ranch Self-Build Cost Engineering Pipeline"
    )
    parser.add_argument(
        "--model",
        default="config/model_config.yaml",
        help="Path to model config YAML (default: config/model_config.yaml)"
    )
    parser.add_argument(
        "--pipeline",
        default="config/pipeline_config.yaml",
        help="Path to pipeline config YAML"
    )
    parser.add_argument(
        "--stage",
        default=None,
        choices=["data_prep","cost_model","schedule_model"],
        help="Run a single stage only"
    )
    return parser.parse_args()


def main():
    args         = parse_args()
    project_root = Path(__file__).parent

    # ── Temp logger for config validation ─────────────────────────
    import logging
    pre_logger = logging.getLogger("pre")
    pre_logger.setLevel(logging.INFO)
    if not pre_logger.handlers:
        pre_logger.addHandler(logging.StreamHandler(sys.stdout))

    print("\n" + "=" * 60)
    print("  RANCH SELF-BUILD — COST ENGINEERING PIPELINE")
    print("  ICEAA 2026 Research Project")
    print("=" * 60)
    print(f"\n  Model config:    {args.model}")
    print(f"  Pipeline config: {args.pipeline}")
    if args.stage:
        print(f"  Single stage:    {args.stage}")
    print()

    # ── Load and validate config ───────────────────────────────────
    print("Validating config files...")
    try:
        cfg = load_and_validate(
            pipeline_config_path=str(project_root / args.pipeline),
            model_config_path=str(project_root / args.model),
            logger=pre_logger
        )
    except SystemExit as e:
        print(f"\n{e}")
        sys.exit(1)

    # ── Setup proper logger (writes to file) ──────────────────────
    log_path = project_root / cfg["log_file"]
    logger   = setup_logger(log_path)

    # ── Filter to single stage if requested ───────────────────────
    if args.stage:
        cfg["stages"] = [s for s in cfg["stages"] if s["name"] == args.stage]
        if not cfg["stages"]:
            logger.error(f"Stage '{args.stage}' not found in pipeline config")
            sys.exit(1)
        logger.info(f"Single stage mode: {args.stage}")

    # ── Run pipeline ──────────────────────────────────────────────
    start = time.time()
    try:
        results = run_pipeline(cfg, project_root, logger)
    except SystemExit as e:
        logger.critical(f"Pipeline aborted: {e}")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Unexpected error: {type(e).__name__}: {e}")
        import traceback
        logger.critical(traceback.format_exc())
        sys.exit(1)

    elapsed = time.time() - start
    logger.info(f"\nTotal pipeline time: {elapsed:.1f}s")

    # ── Print final summary to terminal ───────────────────────────
    print("\n" + "=" * 60)
    print("  PIPELINE RESULTS SUMMARY")
    print("=" * 60)
    if results.get("cost_p50"):
        print(f"  Cost  P10: £{results['cost_p10']:>12,.0f}")
        print(f"  Cost  P50: £{results['cost_p50']:>12,.0f}")
        print(f"  Cost  P90: £{results['cost_p90']:>12,.0f}")
    if results.get("schedule_p50_months"):
        print(f"  Sched P50: Month {results['schedule_p50_months']:.1f}")
        print(f"  Sched P90: Month {results['schedule_p90_months']:.1f}")
        print(f"  P90 delay cost: £{results['p90_delay_cost']:>10,.0f}")
    print(f"\n  Log: {log_path}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
