# Fuel Aggregator — Quick Start & README

This README explains **what to edit in `params.yaml`** and **how to run** the `aggregator.py` orchestrator from the command line.

---

## 1) What this repo does (at a glance)

`aggregator.py` is an end‑to‑end orchestrator that:

1. Loads your configuration from **`params.yaml`**.
2. Initializes the SQLite database and tables from your schema.
3. Pulls fuel & price data:
   - Tries to load a cached EIA dataframe from `cache/dataframes.pkl`.
   - If the cache is missing, it fetches from the EIA API using your **`EIA_API_KEY`** environment variable.
4. Builds runtime frames, dimensions (commodities & technologies), efficiencies, costs, and emissions.
5. Adds metadata and **writes all result tables to your SQLite database**.

> The orchestrator logs progress to the console and writes all tables via `pandas.DataFrame.to_sql()`.

---

## 2) What you need to edit in `params.yaml`

The aggregator expects a few configuration keys. If your `params.yaml` is missing any of these, add them. If you have additional keys used by your local modules, keep them as-is.

Below is a **template** with the **minimum** and **commonly used** fields. Adjust values to match your local setup.

```yaml
# params.yaml (template)

# == General ==
project_name: "CAN Fuel Aggregator"
eia_year: 2024              # The EIA data vintage to use when fetching (int)

# == Database / Schema ==
output_db: "output/CAN_fuel.sqlite"  # Where the final SQLite DB will be written
schema_version: "3.1"                 # Used by your schema loader (if applicable)
schema_file: "input/schema_3_1.sql"   # Absolute or relative path to your SQL schema

# == Geography & Periods ==
# Periods: list of model years; include at least one year you plan to compute.
periods: [2025]                       # e.g., [2020, 2025, 2030]
# Provinces/regions your pipeline expects. Include 'CAN' if your code uses it for national totals.
provinces: ["AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT", "CAN"]

# == Input / Output folders ==
paths:
  input_dir: "input"
  output_dir: "output"
  cache_dir: "cache"                  # where dataframes.pkl is stored/loaded

# == Optional knobs ==
# If your local modules use any of these, expose them here so you can adjust without code changes.
costs:
  currency_base_year: 2024            # base year for currency normalization (if used)
  deflator_target_year: 2025          # target year for deflation (if used)
metadata:
  author: "Your Name"
  description: "Run produced by aggregator.py"
```

### Field-by-field notes

- **`eia_year`**: When the cache is missing, the script fetches data for this year from EIA. Must be an **integer** (e.g., `2024`).  
- **`output_db`**: Final SQLite file path. The script appends to tables; delete or move the file if you want a clean run.
- **`schema_file` / `schema_version`**: Your `init_database(...)` helper typically uses these to (re)create tables. Keep them aligned with your SQL schema file.
- **`periods`** & **`provinces`**: Consumed by your `build_runtime_frames(...)` and downstream builders. Ensure these match the tech/fuel coverage in your inputs.
- **`paths.cache_dir`**: The EIA cache is expected at `cache/dataframes.pkl`. You can change the folder here, but keep the filename the same unless you also change the code.
- **Optional** blocks (`costs`, `metadata`): If your local modules read these, put the tunables here rather than in code.

> If you maintain your own `params.yaml`, **do not remove** any project‑specific fields you already rely on—just add or update the keys above as needed.

---

## 3) One‑time setup

1. **Create a virtual environment** (recommended):
   ```bash
   python -m venv .venv
   # Windows
   .venv\Scripts\activate
   # macOS/Linux
   # source .venv/bin/activate
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
   If you don’t have a `requirements.txt`, install your modules’ dependencies manually (e.g., `pandas`, `pyyaml`, etc.).

3. **Set your EIA API key** (only needed if the cache is missing or you want to force a fresh fetch):
   - Windows (PowerShell):
     ```powershell
     $Env:EIA_API_KEY = "YOUR_KEY_HERE"
     ```
   - macOS/Linux (bash/zsh):
     ```bash
     export EIA_API_KEY="YOUR_KEY_HERE"
     ```

---

## 4) Running from the command line

From the repository root (where `aggregator.py` lives):

```bash
# Basic run
python aggregator.py
```

What happens during a run:

- Tries `cache/dataframes.pkl`. If found, it loads and proceeds.  
- If not found, it calls the EIA API using `EIA_API_KEY` and **creates** the cache file.  
- Builds out all tables and **appends** them into your SQLite database at `output_db`.  
- Logs progress to the console (INFO level).

### Optional: clean runs
- **Delete the SQLite file** at `output_db` if you want to start fresh.
- **Delete `cache/dataframes.pkl`** if you want to force re‑fetch from EIA.

### Optional: change logging level
Edit `aggregator.py` to modify `logging.basicConfig(level=logging.INFO, ...)` if you want more/less verbosity.

---

## 5) Expected outputs

- **SQLite DB** at `output_db` containing tables like commodities/technologies, efficiency, costs, emissions, and metadata (exact table names depend on your schema).
- **Cache** at `cache/dataframes.pkl` after the first successful fetch.

---

## 6) Troubleshooting

- **`FileNotFoundError: cache/dataframes.pkl`** → This is normal on first run; the script will fetch from EIA **if** `EIA_API_KEY` is set.
- **EIA fetch fails** → Confirm `EIA_API_KEY` is exported in your shell and valid.
- **Duplicate rows in SQLite** → The script **appends**. Remove the DB file for a clean slate.
- **Schema mismatches** → Ensure `schema_file` and your code’s expected table/column names are aligned. Recreate the DB from the latest schema if necessary.
- **Pandas `FutureWarning` about concat** → Caused by upstream `pandas` changes; not fatal. Update local code to drop empty frames before concatenation if desired.

---

## 7) Repo tips

- Commit your `params.yaml` to version control (without secrets).
- Keep `schema_*.sql` under `input/` and bump `schema_version` when you change it.
- Use small test runs (single `period` and a subset of `provinces`) to iterate faster.

---

## 8) FAQ

**Q: Do I have to set `EIA_API_KEY` if I already have the cache?**  
A: No. The script will skip the API call if `cache/dataframes.pkl` exists.

**Q: Where is the DB written?**  
A: Wherever `output_db` points to in `params.yaml` (default shown above is `output/CAN_fuel.sqlite`).

**Q: Can I place `params.yaml` somewhere else?**  
A: Yes, if your `setup.load_config()` supports a custom path. Otherwise, keep it at the repo root or where your modules expect it.
