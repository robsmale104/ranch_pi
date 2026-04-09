"""
config_validator.py
Reads and validates both YAML config files before any pipeline stage runs.
Returns merged config dict or raises SystemExit on critical failures.
"""
import yaml
import os
from pathlib import Path


def load_and_validate(pipeline_config_path, model_config_path, logger=None):
    """
    Load both config files, merge them, run validation checks.
    Returns merged config dict.
    Raises SystemExit on any critical failure.
    """
    def log(level, msg):
        if logger:
            getattr(logger, level)(msg)
        else:
            prefix = {"info":"  INFO","warning":"  WARN",
                      "error":"  FAIL","critical":"CRITICAL"}
            print(f"{prefix.get(level,'INFO')} | {msg}")

    errors   = []
    warnings = []

    # ── Load YAML files ────────────────────────────────────────────
    for path, label in [(pipeline_config_path,"pipeline_config.yaml"),
                        (model_config_path,   "model_config.yaml")]:
        if not os.path.exists(path):
            errors.append(f"{label} not found at: {path}")
    if errors:
        for e in errors: log("error", e)
        raise SystemExit("\nPipeline stopped — config files missing.")

    with open(pipeline_config_path) as f:
        pipe_cfg = yaml.safe_load(f)
    with open(model_config_path) as f:
        model_cfg = yaml.safe_load(f)

    log("info", "pipeline_config.yaml loaded")
    log("info", f"model_config.yaml loaded  "
                f"(scenario: {model_cfg.get('scenario_name','unnamed')})")

    # ── Merge — model_config takes precedence ──────────────────────
    cfg = {**pipe_cfg, **model_cfg}

    # ── Validation rules from pipeline_config ─────────────────────
    rules = pipe_cfg.get("validation", {})

    # source_file exists
    src = cfg.get("source_file", "")
    if not os.path.exists(src):
        msg = f"Source file not found: {src}"
        if rules.get("source_file_exists", {}).get("critical", True):
            errors.append(msg)
        else:
            warnings.append(msg)
    else:
        log("info", f"Source file found: {Path(src).name}  OK")

    # inflation_rate bounds
    inf_rate = cfg.get("inflation_rate", 0)
    r = rules.get("inflation_rate", {})
    if not (r.get("min", 0) <= inf_rate <= r.get("max", 1)):
        msg = f"inflation_rate={inf_rate} — {r.get('message','out of range')}"
        (errors if r.get("critical", True) else warnings).append(msg)
    else:
        log("info", f"inflation_rate = {inf_rate*100:.2f}%  OK")

    # n_simulations bounds
    n_sims = cfg.get("n_simulations", 0)
    r = rules.get("n_simulations", {})
    if not (r.get("min", 0) <= n_sims <= r.get("max", 999999)):
        msg = f"n_simulations={n_sims} — {r.get('message','out of range')}"
        (errors if r.get("critical", True) else warnings).append(msg)
    else:
        log("info", f"n_simulations = {n_sims:,}  OK")

    # corr_matrix dimensions
    matrix  = cfg.get("corr_matrix", [])
    drivers = cfg.get("driver_order", [])
    expected = rules.get("corr_matrix_size", {}).get("expected", 6)
    r = rules.get("corr_matrix_size", {})
    if len(matrix) != expected or any(len(row) != expected for row in matrix):
        msg = f"corr_matrix is {len(matrix)}x? — expected {expected}x{expected}"
        (errors if r.get("critical", True) else warnings).append(msg)
    elif len(drivers) != expected:
        errors.append(f"driver_order has {len(drivers)} entries, expected {expected}")
    else:
        log("info", f"corr_matrix {expected}x{expected}  OK")

    # in-service year ordering
    start_yr = cfg.get("inservice_start_yr", 0)
    end_yr   = cfg.get("inservice_end_yr", 0)
    r = rules.get("inservice_yr_order", {})
    if start_yr >= end_yr:
        msg = f"inservice_start_yr ({start_yr}) >= inservice_end_yr ({end_yr})"
        (errors if r.get("critical", True) else warnings).append(msg)
    else:
        log("info", f"In-service years {start_yr}–{end_yr}  OK")

    # ── Convert cpi_factors keys to strings (JSON/YAML safe) ──────
    if "cpi_factors" in cfg:
        cfg["cpi_factors"] = {str(k): v for k, v in cfg["cpi_factors"].items()}

    # ── Report ────────────────────────────────────────────────────
    for w in warnings:
        log("warning", w)

    if errors:
        log("error", f"{len(errors)} critical config error(s):")
        for e in errors:
            log("error", f"  x {e}")
        raise SystemExit("\nPipeline stopped — fix config errors above.")

    log("info", "Config validation passed")
    return cfg
