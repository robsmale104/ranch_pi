# Ranch Pipeline
**ICEAA 2026 — Workflow 3: Pipeline-Based Cost Model**

Single command runs all three stages, validates each, and logs everything.

## Setup
```bash
cd ranch_pipeline
poetry install
```

Update `config/pipeline_config.yaml` — set `source_file` to the full path of `Self_Build_Costs_Data.xlsx`.

## Run
```bash
# Full pipeline
poetry run python main.py

# Single stage
poetry run python main.py --stage cost_model

# Stress scenario (swap inflation to 4.4%)
# Edit model_config.yaml: inflation_rate: 0.044
poetry run python main.py

# Or point to a different model config
poetry run python main.py --model config/stress_config.yaml
```

## Outputs
| File | Description |
|------|-------------|
| `results/pipeline_run.log` | Full timestamped run log |
| `data/clean_inputs.csv` | Validated cost lines |
| `results/cost_mc_results.csv` | 10,000 MC cost simulations |
| `results/monthly_profile.npy` | Monthly spend matrix |
| `results/schedule_mc_results.csv` | 10,000 MC schedule simulations |
| `results/mc_summary.json` | Key cost statistics |
| `results/schedule_summary.json` | Key schedule statistics |

## Config
| File | Purpose |
|------|---------|
| `config/model_config.yaml` | Parameters — change between scenario runs |
| `config/pipeline_config.yaml` | Structure — stage order, file paths |
