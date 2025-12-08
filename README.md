# Beacon Data Lake CLI

Beacon Data Lake CLI is a thin, `cmd2`-powered shell that sits on top of
the Beacon Python SDK. It gives analysts and operators a quick way to
connect to a Beacon node, browse registered datasets, inspect logical
tables, and execute SQL or JSON-based queries without writing scripts.

## Highlights

- **Instant Beacon sessions**: `beacon-datalake-cli --url https://...`
	auto-connects and drops you into an interactive prompt with command
	completion, history, and colorized output courtesy of Rich.
- **Schema-aware exploration**: list or inspect datasets and logical
	tables, including per-column data types and format metadata.
- **Flexible querying**: run ad-hoc SQL with preview/stream/export
	options or use the fluent query builder to add select, filter, sort,
	spatial, and distinct clauses via flags.
- **Admin workflows**: upload, download, or delete dataset files plus
	create or remove logical tables when you have the right credentials.
- **Rich previews**: tabular results show column data types, memory
	usage estimates, and query timing metrics; export to Parquet, Arrow,
	CSV, NetCDF, or GeoParquet when needed.

## Requirements

- Python 3.10 or newer
- Access to a Beacon node plus any JWT token or basic auth credentials
	required by that deployment
- `beacon-api` compatible server version (the CLI ships with SDK as a
	dependency)

## Installation

```bash
pip install beacon-datalake-cli
```

Prefer to keep tools isolated? Install via `pipx install
beacon-datalake-cli` and you will get the same `beacon-datalake-cli`
entry point on your PATH.

## Quick start

```bash
# Connect immediately using startup flags
beacon-datalake-cli --url https://beacon.example.com --jwt-token "$TOKEN"

# Or launch the shell first, then run connect inside the prompt
beacon-datalake-cli
beacon_cli> connect https://beacon.example.com --basic-auth user pass
```

Once connected:

1. Run `dashboard` for a full-screen command palette.
2. Check your session with `status` to confirm version and admin access.
3. Explore data with commands such as:
	 - `list_datasets --pattern *.parquet`
	 - `dataset_schema /data/sst/sample.parquet`
	 - `list_tables` and `list_table_schema climate_daily`
4. Query live data:

```bash
beacon_cli> sql "SELECT station_id, temp_c FROM climate_daily LIMIT 10" --rows 10

# JSON query builder example (select + filters + streaming preview)
beacon_cli> query_builder --table climate_daily \
		--select station_id --select temp_c:temperature_c \
		--range observation_date "2024-01-01" "2024-01-31" \
		--stream --rows 20
```

## Command reference

| Area      | Command (args)                                   | Purpose |
|-----------|--------------------------------------------------|---------|
| Session   | `connect`, `status`, `dashboard`, `quit`         | Manage CLI session and connection |
| Datasets  | `list_datasets`, `dataset_schema`, `upload_dataset`, `download_dataset`, `delete_dataset` | Discover and manage registered dataset files |
| Tables    | `list_tables`, `list_table_schema`, `create_table`, `delete_table` | Inspect logical tables or administer them |
| Querying  | `sql`, `query_builder`                           | Execute SQL or JSON builder queries with preview/export/stream options |

Use `help <command>` for detailed usage, supported flags, and examples.

## Development notes

- The package exposes a single console script named `beacon-datalake-cli`
	defined in `pyproject.toml`.
- To test local changes, install in editable mode: `pip install -e .`
	then run `beacon-datalake-cli` from your virtual environment.