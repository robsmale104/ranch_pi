"""
pipeline.py
Orchestrates all stages: validate config, run stages, run tests, log results.
"""
import importlib.util
import subprocess
import sys
import json
import time
import logging
from pathlib import Path


def setup_logger(log_path):
    """Creates logger that writes to both terminal and log file."""
    log_path = Path(log_path)
    log_path.parent.mkdir(exist_ok=True)

    logger = logging.getLogger("ranch_pipeline")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")

    # Terminal handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File handler
    fh = logging.FileHandler(log_path, mode="w")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


def load_stage_module(script_path):
    """Dynamically loads a stage module from its file path."""
    spec   = importlib.util.spec_from_file_location("stage", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_test(test_path, logger):
    """Runs a test script as a subprocess. Returns (passed, output)."""
    result = subprocess.run(
        [sys.executable, str(test_path)],
        capture_output=True, text=True
    )
    passed = result.returncode == 0
    output = result.stdout + result.stderr
    return passed, output


def run_pipeline(cfg, project_root, logger):
    """
    Runs all pipeline stages defined in cfg['stages'].
    Returns dict of stage results.
    """
    stages       = cfg.get("stages", [])
    all_results  = {}
    pipeline_ok  = True

    logger.info("=" * 60)
    logger.info(f"PIPELINE START")
    logger.info(f"Scenario: {cfg.get('scenario_name','Base')}")
    logger.info(f"Source:   {Path(cfg['source_file']).name}")
    logger.info(f"Sims:     {cfg['n_simulations']:,}")
    logger.info(f"Inflation:{cfg['inflation_rate']*100:.2f}%")
    logger.info("=" * 60)

    for stage_def in stages:
        name        = stage_def["name"]
        script_path = project_root / stage_def["script"]
        test_path   = project_root / stage_def["test"]
        critical    = stage_def.get("critical", True)
        description = stage_def.get("description", name)

        logger.info("")
        logger.info(f"── STAGE: {name.upper()} ─────────────────────────────")
        logger.info(f"   {description}")

        stage_start = time.time()

        # ── Run stage ─────────────────────────────────────────────
        try:
            module  = load_stage_module(script_path)
            results = module.run(cfg, logger)
            elapsed = time.time() - stage_start
            logger.info(f"   Stage completed in {elapsed:.1f}s")
            all_results[name] = results
        except SystemExit as e:
            logger.error(f"   Stage failed: {e}")
            if critical:
                logger.critical("Critical stage failed — pipeline stopping")
                raise
            else:
                logger.warning("Non-critical stage failed — continuing")
                all_results[name] = {"error": str(e)}
                pipeline_ok = False
                continue
        except Exception as e:
            logger.error(f"   Stage error: {type(e).__name__}: {e}")
            if critical:
                logger.critical("Critical stage failed — pipeline stopping")
                raise
            else:
                logger.warning("Non-critical stage failed — continuing")
                all_results[name] = {"error": str(e)}
                pipeline_ok = False
                continue

        # ── Run test ──────────────────────────────────────────────
        logger.info(f"   Running test: {test_path.name}")
        test_start  = time.time()
        passed, output = run_test(test_path, logger)
        test_elapsed   = time.time() - test_start

        for line in output.strip().splitlines():
            if line.strip():
                logger.info(f"   {line}")

        if passed:
            logger.info(f"   Tests PASSED in {test_elapsed:.1f}s")
        else:
            logger.error(f"   Tests FAILED in {test_elapsed:.1f}s")
            if critical:
                logger.critical("Critical test failed — pipeline stopping")
                raise SystemExit(f"Tests failed for stage: {name}")
            else:
                logger.warning("Non-critical test failed — continuing")
                pipeline_ok = False

    # ── Pipeline summary ──────────────────────────────────────────
    logger.info("")
    logger.info("=" * 60)
    if pipeline_ok:
        logger.info("PIPELINE COMPLETE — all stages passed")
    else:
        logger.info("PIPELINE COMPLETE — with warnings (see log)")
    logger.info("=" * 60)

    # Write run summary JSON
    results_dir = Path(cfg["results_dir"])
    results_dir.mkdir(exist_ok=True)

    cost_summary  = {}
    sched_summary = {}
    sens_summary  = {}
    cost_path  = results_dir / "mc_summary.json"
    sched_path = results_dir / "schedule_summary.json"
    sens_path  = results_dir / "sensitivity_summary.json"
    if cost_path.exists():
        with open(cost_path) as f: cost_summary = json.load(f)
    if sched_path.exists():
        with open(sched_path) as f: sched_summary = json.load(f)
    if sens_path.exists():
        with open(sens_path) as f: sens_summary = json.load(f)

    run_summary = {
        "scenario":              cfg.get("scenario_name","Base"),
        "pipeline_ok":           pipeline_ok,
        "n_simulations":         cfg["n_simulations"],
        "inflation_rate":        cfg["inflation_rate"],
        "cost_p10":              cost_summary.get("total_p10"),
        "cost_p50":              cost_summary.get("total_p50"),
        "cost_p90":              cost_summary.get("total_p90"),
        "schedule_p50_months":   sched_summary.get("project_p50_months"),
        "schedule_p90_months":   sched_summary.get("project_p90_months"),
        "p50_delay_cost":        sched_summary.get("p50_delay_cost_gbp"),
        "p90_delay_cost":        sched_summary.get("p90_delay_cost_gbp"),
        "sens_worst_driver":     sens_summary.get("worst_single_driver"),
        "sens_worst_swing":      sens_summary.get("worst_single_p50_swing"),
        "sens_combined_p50":     sens_summary.get("combined_p50"),
        "sens_combined_p90":     sens_summary.get("combined_p90"),
    }
    with open(results_dir / "pipeline_run_summary.json", "w") as f:
        json.dump(run_summary, f, indent=2)

    logger.info("Run summary saved: results/pipeline_run_summary.json")
    logger.info(f"Full log saved:    {cfg['log_file']}")

    return run_summary
