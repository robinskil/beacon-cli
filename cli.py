"""Simple cmd2-based CLI wrapper for the Beacon Python SDK."""

from __future__ import annotations

import argparse
import ast
import shutil
import time
from io import BytesIO
from pathlib import Path
from typing import Any, Optional, cast

import cmd2
import pandas as pd
from cmd2 import Cmd2ArgumentParser
from rich import box
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from beacon_api import Client, Dataset
from beacon_api.table import DataTable
from beacon_api.query import (
    Arrow,
    CSV as QueryCSV,
    GeoParquet,
    JSONQuery,
    NetCDF,
    Parquet,
    SQLQuery,
    BaseQuery,
)


def _build_startup_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Beacon CLI")
    parser.add_argument(
        "--url",
        dest="url",
        help="Automatically connect to the given Beacon base URL on startup.",
    )
    parser.add_argument(
        "--jwt-token",
        dest="jwt_token",
        help="Optional bearer token to use for the startup connection.",
    )
    return parser


def _connect_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Connect to a Beacon node")
    parser.add_argument("url", help="Beacon base URL, e.g. https://beacon.example.com")
    parser.add_argument(
        "--jwt-token",
        dest="jwt_token",
        help="Optional bearer token for Authorization header",
    )
    parser.add_argument(
        "--basic-auth",
        nargs=2,
        metavar=("USER", "PASSWORD"),
        help="Optional username/password for basic auth",
    )
    return parser


def _sql_arg_parser() -> Cmd2ArgumentParser:
    parser = Cmd2ArgumentParser(
        description="Execute a SQL query against the connected Beacon node"
    )
    parser.add_argument(
        "sql",
        nargs=argparse.REMAINDER,
        help="SQL statement to run (wrap in quotes for complex expressions)",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=5,
        help="Number of rows to display in the preview table (default: 5)",
    )
    parser.add_argument(
        "--page",
        action="store_true",
        help="Open a pager with the full result table (preview format only)",
    )
    parser.add_argument(
        "--format",
        choices=["preview", "parquet", "csv", "arrow", "netcdf", "geoparquet"],
        default="preview",
        help="preview renders a table; other options stream raw bytes for piping or saving",
    )
    parser.add_argument(
        "--output-path",
        help="Optional destination file when exporting (defaults to stdout for piping)",
    )
    parser.add_argument(
        "--stream",
        action="store_true",
        help="Use streaming execution and report time-to-first-row (preview format only)",
    )
    parser.add_argument(
        "--geo-columns",
        nargs=2,
        metavar=("LON_COLUMN", "LAT_COLUMN"),
        help="Required when --format geoparquet to map longitude/latitude columns",
    )
    return parser


def _dataset_list_arg_parser() -> Cmd2ArgumentParser:
    parser = Cmd2ArgumentParser(
        description="List datasets registered with the Beacon node"
    )
    parser.add_argument(
        "--pattern", help="Optional glob/regex pattern resolved by the server"
    )
    parser.add_argument(
        "--limit", type=int, help="Maximum number of datasets to return"
    )
    parser.add_argument("--offset", type=int, help="Offset for pagination")
    return parser


def _dataset_schema_arg_parser() -> Cmd2ArgumentParser:
    parser = Cmd2ArgumentParser(description="Inspect a dataset schema")
    parser.add_argument(
        "file_path", help="Dataset path exactly as registered on the Beacon node"
    )
    return parser


def _table_schema_arg_parser() -> Cmd2ArgumentParser:
    parser = Cmd2ArgumentParser(description="Inspect a logical table schema")
    parser.add_argument("table_name", help="Name of the logical table to inspect")
    return parser


def _dataset_upload_arg_parser() -> Cmd2ArgumentParser:
    parser = Cmd2ArgumentParser(
        description="Upload a dataset file to the Beacon node (admin only)"
    )
    parser.add_argument("local_path", help="Local file path to upload")
    parser.add_argument(
        "destination_path",
        help="Destination path on the Beacon node, e.g. /data/datasets/file.parquet",
    )
    parser.add_argument("--force", help="Force the action even when beacon version mismatches.", default=False)
    return parser


def _dataset_download_arg_parser() -> Cmd2ArgumentParser:
    parser = Cmd2ArgumentParser(
        description="Download a dataset file from the Beacon node (admin only)"
    )
    parser.add_argument("dataset_path", help="Path to the dataset on the Beacon node")
    parser.add_argument(
        "local_path", help="Local path where the downloaded file should be saved"
    )
    parser.add_argument("--force", help="Force the action even when beacon version mismatches.", default=False)
    return parser


def _dataset_delete_arg_parser() -> Cmd2ArgumentParser:
    parser = Cmd2ArgumentParser(
        description="Delete a dataset file from the Beacon node (admin only)"
    )
    parser.add_argument(
        "dataset_path", help="Path to the dataset on the Beacon node to delete"
    )
    parser.add_argument("--force", help="Force the action even when beacon version mismatches.", default=False)
    return parser


def _create_table_arg_parser() -> Cmd2ArgumentParser:
    parser = Cmd2ArgumentParser(
        description="Create a logical table on the Beacon node (admin only)"
    )
    parser.add_argument("table_name", help="Name of the logical table to create")
    parser.add_argument(
        "dataset_glob_paths",
        nargs="+",
        help="One or more dataset glob paths that back the logical table",
    )
    parser.add_argument(
        "file_format", help="Dataset file format (parquet, csv, zarr, ...)"
    )
    parser.add_argument("--description", help="Optional table description")
    parser.add_argument("--delimiter", help="CSV delimiter (only for csv format)")
    parser.add_argument(
        "--statistics-columns",
        nargs="+",
        help="Statistics columns for Zarr datasets",
    )
    parser.add_argument(
        "--option",
        dest="options",
        action="append",
        metavar="KEY=VALUE",
        help="Additional format-specific kwargs (may be repeated)",
    )
    return parser


def _delete_table_arg_parser() -> Cmd2ArgumentParser:
    parser = Cmd2ArgumentParser(
        description="Delete a logical table from the Beacon node (admin only)"
    )
    parser.add_argument("table_name", help="Name of the logical table to delete")
    return parser
def _parse_option_pairs(pairs: list[str] | None) -> dict[str, Any]:
    if not pairs:
        return {}
    parsed: dict[str, Any] = {}
    for pair in pairs:
        if "=" not in pair:
            raise ValueError(f"Expected KEY=VALUE format, got '{pair}'")
        key, raw_value = pair.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError("Option keys cannot be empty")
        value: Any = raw_value
        try:
            value = ast.literal_eval(raw_value)
        except (ValueError, SyntaxError):
            value = raw_value
        parsed[key] = value
    return parsed


def _coerce_value(raw: str) -> Any:
    lowered = raw.strip().lower()
    if lowered in {"none", "null"}:
        return None
    try:
        return ast.literal_eval(raw)
    except (ValueError, SyntaxError):
        return raw


def _parse_key_value(spec: str) -> tuple[str, str]:
    if "=" not in spec:
        raise ValueError(f"Expected COLUMN=VALUE format, got '{spec}'")
    column, value = spec.split("=", 1)
    column = column.strip()
    if not column:
        raise ValueError("Column name cannot be empty")
    return column, value.strip()


def _parse_select_spec(spec: str) -> tuple[str, Optional[str]]:
    column, _, alias = spec.strip().partition(":")
    column = column.strip()
    alias = alias.strip() or None
    if not column:
        raise ValueError("Select columns cannot be empty")
    return column, alias


def _parse_optional_value(raw: str) -> Any:
    return None if raw.strip().lower() in {"none", "null"} else _coerce_value(raw)


def _query_builder_arg_parser() -> Cmd2ArgumentParser:
    parser = Cmd2ArgumentParser(
        description="Build and run a JSON query using the fluent Beacon builder"
    )
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--table", help="Logical table name to query")
    target.add_argument(
        "--dataset",
        help="Dataset path registered on the Beacon node (supports suffix matching)",
    )
    parser.add_argument(
        "--select",
        action="append",
        metavar="COLUMN[:ALIAS]",
        help="Select column definition; repeat to add multiple columns",
    )
    parser.add_argument(
        "--equals",
        action="append",
        metavar="COLUMN=VALUE",
        help="Equality filter; repeat for multiple filters",
    )
    parser.add_argument(
        "--not-equals",
        dest="not_equals",
        action="append",
        metavar="COLUMN=VALUE",
        help="Not-equals filter; repeat for multiple filters",
    )
    parser.add_argument(
        "--range",
        dest="ranges",
        action="append",
        nargs=3,
        metavar=("COLUMN", "MIN", "MAX"),
        help="Range filter; use 'none' for open bounds",
    )
    parser.add_argument(
        "--bbox",
        dest="bboxes",
        action="append",
        nargs=6,
        metavar=("LON_COL", "LAT_COL", "MIN_LON", "MAX_LON", "MIN_LAT", "MAX_LAT"),
        help="Bounding box filter (repeatable)",
    )
    parser.add_argument(
        "--is-null",
        dest="is_null",
        action="append",
        metavar="COLUMN",
        help="IS NULL filter",
    )
    parser.add_argument(
        "--is-not-null",
        dest="is_not_null",
        action="append",
        metavar="COLUMN",
        help="IS NOT NULL filter",
    )
    parser.add_argument(
        "--distinct",
        action="append",
        metavar="COLUMN",
        help="Add DISTINCT on the provided columns",
    )
    parser.add_argument(
        "--sort",
        action="append",
        metavar="COLUMN[:asc|desc]",
        help="Add ORDER BY clauses (default ascending)",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=5,
        help="Number of rows to show in preview output",
    )
    parser.add_argument(
        "--page",
        action="store_true",
        help="Open a pager with the full preview table",
    )
    parser.add_argument(
        "--format",
        choices=["preview", "parquet", "csv", "arrow", "netcdf", "geoparquet"],
        default="preview",
        help="preview renders a table; other options stream raw bytes for piping or saving",
    )
    parser.add_argument(
        "--output-path",
        help="Destination file path when exporting parquet/csv/arrow/netcdf/geoparquet",
    )
    parser.add_argument(
        "--stream",
        action="store_true",
        help="Use streaming execution for preview results",
    )
    parser.add_argument(
        "--geo-columns",
        nargs=2,
        metavar=("LON_COLUMN", "LAT_COLUMN"),
        help="Required when --format geoparquet to map longitude/latitude columns",
    )
    return parser


CONNECT_PARSER = _connect_arg_parser()
SQL_PARSER = _sql_arg_parser()
DATASET_LIST_PARSER = _dataset_list_arg_parser()
DATASET_SCHEMA_PARSER = _dataset_schema_arg_parser()
TABLE_SCHEMA_PARSER = _table_schema_arg_parser()
QUERY_BUILDER_PARSER = _query_builder_arg_parser()
DATASET_UPLOAD_PARSER = _dataset_upload_arg_parser()
DATASET_DOWNLOAD_PARSER = _dataset_download_arg_parser()
DATASET_DELETE_PARSER = _dataset_delete_arg_parser()
CREATE_TABLE_PARSER = _create_table_arg_parser()
DELETE_TABLE_PARSER = _delete_table_arg_parser()


class BeaconCli(cmd2.Cmd):
    """Interactive shell for the Beacon Python SDK."""

    prompt = "beacon_cli> "
    intro = (
        "beacon_cli shell. Type 'help'/'?' for commands. Start with 'connect <url>'."
    )

    def __init__(self) -> None:
        super().__init__(allow_cli_args=False)
        self.client: Optional[Client] = None
        self.console = Console()

    def _resolve_dataset(self, file_path: str) -> Dataset | None:
        client = cast(Client, self.client)
        try:
            datasets = client.list_datasets(pattern=file_path)
        except Exception as exc:  # pragma: no cover - CLI helper
            self.perror(f"Failed to resolve dataset '{file_path}': {exc}")
            return None

        if file_path in datasets:
            return datasets[file_path]

        for path, dataset in datasets.items():
            if path.endswith(file_path):
                return dataset

        self.perror(f"Dataset '{file_path}' was not found on the server.")
        return None

    def _resolve_table(self, table_name: str) -> DataTable | None:
        client = cast(Client, self.client)
        try:
            tables = client.list_tables()
        except Exception as exc:  # pragma: no cover - CLI helper
            self.perror(f"Failed to resolve table '{table_name}': {exc}")
            return None

        if table_name in tables:
            return tables[table_name]

        matches = [table for name, table in tables.items() if name.lower() == table_name.lower()]
        if matches:
            return matches[0]

        available = ", ".join(sorted(tables)) if tables else "none"
        self.perror(
            f"Table '{table_name}' was not found on the server. Available tables: {available}"
        )
        return None

    def _render_status(self) -> None:
        client = cast(Client, self.client)
        try:
            info = client.get_server_info()
        except Exception as exc:  # pragma: no cover - thin wrapper
            self.perror(f"Unable to fetch server info: {exc}")
            return

        admin_state = "unknown"
        try:
            admin_state = "yes" if client.session.is_admin() else "no"
        except Exception as exc:  # pragma: no cover - best-effort
            admin_state = f"error: {exc}"

        table = Table(box=box.SIMPLE_HEAVY)
        table.add_column("Property", style="bold cyan")
        table.add_column("Value", overflow="fold")
        table.add_row("Base URL", client.session.base_url)
        table.add_row("Beacon version", str(info.get("beacon_version", "unknown")))
        table.add_row("Datasets enabled", str(info.get("datasets", "?")))
        table.add_row("Admin access", admin_state)

        panel = Panel(table, title="Connection status", border_style="green")
        self.console.print(panel)

    def _format_bytes(self, num_bytes: int | None) -> str:
        if num_bytes is None:
            return "unknown"
        units = ["B", "KB", "MB", "GB", "TB"]
        value = float(num_bytes)
        for unit in units:
            if value < 1024 or unit == units[-1]:
                return f"{value:.2f} {unit}"
            value /= 1024
        return f"{value:.2f} TB"

    def _build_query_metrics(
        self,
        total_seconds: float,
        first_row_seconds: float,
        *,
        streamed: bool,
        response_bytes: int | None = None,
    ) -> list[tuple[str, str]]:
        label = "First rows" if streamed else "Data ready"
        return [
            ("Total duration", f"{total_seconds:.2f}s"),
            (label, f"{first_row_seconds:.2f}s"),
            ("Response size", self._format_bytes(response_bytes)),
        ]

    def _print_query_metrics(
        self,
        total_seconds: float,
        first_row_seconds: float,
        *,
        streamed: bool,
        use_stderr: bool = False,
        response_bytes: int | None = None,
    ) -> None:
        console = self.console if not use_stderr else Console(stderr=True)
        metrics_table = Table(show_header=False, box=box.SIMPLE)
        for name, value in self._build_query_metrics(
            total_seconds,
            first_row_seconds,
            streamed=streamed,
            response_bytes=response_bytes,
        ):
            metrics_table.add_row(name, value)
        console.print(Panel(metrics_table, title="Query metrics", border_style="cyan"))

    def _build_dataframe_panel(
        self,
        df: pd.DataFrame,
        title: str,
        *,
        max_rows: int | None,
        metrics: list[tuple[str, str]] | None = None,
        source_df: pd.DataFrame | None = None,
        subtitle: str | None = None,
    ) -> Panel:
        source = source_df if source_df is not None else df
        if df.empty:
            message = "Query returned no rows"
            if metrics:
                metric_lines = "\n".join(f"{name}: {value}" for name, value in metrics)
                message = f"{message}\n{metric_lines}"
            return Panel(message, title=title, border_style="red")

        display_df = df if max_rows is None else df.head(max_rows)
        try:
            column_mem = source.memory_usage(index=False, deep=True)
        except Exception:
            column_mem = None

        label_map: dict[str, str] = {}
        for column in display_df.columns:
            dtype = (
                source[column].dtype
                if column in source.columns
                else display_df[column].dtype
            )
            size_bytes: int | None = None
            if column_mem is not None and column in column_mem:
                size_value = column_mem[column]
                if isinstance(size_value, (int, float)):
                    size_bytes = int(size_value)
            size_str = self._format_bytes(size_bytes)
            label_map[column] = f"{column}({dtype}, {size_str})"
        display_df = display_df.rename(columns=label_map)

        table = Table(
            title=title,
            show_header=True,
            header_style="bold green",
            box=box.MINIMAL_DOUBLE_HEAD,
            expand=True,
        )
        column_names = list(display_df.columns)
        if not column_names:
            table.add_column("Value")
            column_names = ["Value"]
        else:
            for column in column_names:
                table.add_column(str(column))

        for _, row in display_df.iterrows():
            table.add_row(*(str(row[col]) for col in column_names))

        if metrics:
            table.add_section()
            column_count = len(column_names)
            for name, value in metrics:
                cells = [f"{name}: {value}"] + [""] * (column_count - 1)
                table.add_row(*cells)

        if subtitle is not None:
            subtitle_text = subtitle
        elif max_rows is None or len(display_df) == len(source):
            subtitle_text = f"{len(source)} rows"
        else:
            subtitle_text = f"showing {len(display_df)} of {len(source)} rows"

        return Panel(table, border_style="green", subtitle=subtitle_text)

    def _run_query_to_dataframe(
        self, query: BaseQuery
    ) -> tuple[pd.DataFrame, float, int]:
        query.set_output(Parquet())
        start = time.perf_counter()
        response = query.execute()
        payload = response.content
        total_elapsed = time.perf_counter() - start
        df = pd.read_parquet(BytesIO(payload))
        return df, total_elapsed, len(payload)

    def _export_query_results(
        self,
        query: BaseQuery,
        fmt: str,
        output_path: str | None,
        *,
        geo_columns: tuple[str, str] | None = None,
    ) -> None:
        output_map: dict[str, Any] = {
            "parquet": Parquet,
            "csv": QueryCSV,
            "arrow": Arrow,
            "netcdf": NetCDF,
        }

        if fmt == "geoparquet":
            lon_col, lat_col = cast(tuple[str, str], geo_columns)
            query.set_output(
                GeoParquet(longitude_column=lon_col, latitude_column=lat_col)
            )
        else:
            output_cls = output_map[fmt]
            query.set_output(output_cls())

        start = time.perf_counter()
        bytes_written = 0
        try:
            response = query.execute()
        except Exception as exc:  # pragma: no cover - CLI wrapper
            self.perror(f"Query execution failed: {exc}")
            return
        total_elapsed = time.perf_counter() - start

        if output_path:
            destination = Path(output_path).expanduser()
            destination.parent.mkdir(parents=True, exist_ok=True)
            with open(destination, "wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)
                        bytes_written += len(chunk)
            self.console.print(
                Panel(
                    f"Saved {fmt.upper()} results to [green]{destination}[/green]",
                    border_style="green",
                )
            )
            self._print_query_metrics(
                total_elapsed,
                total_elapsed,
                streamed=False,
                response_bytes=bytes_written,
            )
            return

        output_stream = getattr(self.stdout, "buffer", self.stdout)
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                output_stream.write(chunk)
                bytes_written += len(chunk)
        if hasattr(output_stream, "flush"):
            output_stream.flush()
        self._print_query_metrics(
            total_elapsed,
            total_elapsed,
            streamed=False,
            use_stderr=True,
            response_bytes=bytes_written,
        )

    def _collect_streaming_preview(
        self,
        query: BaseQuery,
        max_rows: int,
    ) -> tuple[pd.DataFrame, float, float, int]:
        start = time.perf_counter()
        preview_frames: list[pd.DataFrame] = []
        rows_collected = 0
        first_row_elapsed: float | None = None
        total_bytes = 0

        stream = query.execute_streaming()
        for batch in stream:
            if batch.num_rows == 0:
                continue
            total_bytes += batch.nbytes
            if first_row_elapsed is None:
                first_row_elapsed = time.perf_counter() - start
            if rows_collected < max_rows:
                preview_frames.append(batch.to_pandas())
                rows_collected += batch.num_rows

        total_elapsed = time.perf_counter() - start
        preview_df = (
            pd.concat(preview_frames, ignore_index=True)
            if preview_frames
            else pd.DataFrame()
        )
        return (
            preview_df.head(max_rows),
            total_elapsed,
            first_row_elapsed or total_elapsed,
            total_bytes,
        )

    def _validate_output_flags(self, fmt: str, page: bool, stream: bool) -> bool:
        if page and fmt != "preview":
            self.perror("--page is only supported when using --format preview")
            return False
        if stream and fmt != "preview":
            self.perror("--stream is only supported when --format preview is selected")
            return False
        if page and stream:
            self.perror("--page cannot be combined with --stream")
            return False
        return True

    def _render_query_results(
        self,
        query: BaseQuery,
        *,
        fmt: str,
        rows: int,
        page: bool,
        stream: bool,
        output_path: str | None,
        geo_columns: tuple[str, str] | None = None,
    ) -> None:
        if fmt == "geoparquet" and geo_columns is None:
            self.perror("GeoParquet output requires --geo-columns LON_COLUMN LAT_COLUMN")
            return

        if fmt != "preview":
            self._export_query_results(query, fmt, output_path, geo_columns=geo_columns)
            return

        if stream:
            try:
                df, total_elapsed, first_row, total_bytes = self._collect_streaming_preview(
                    query, max_rows=rows
                )
            except Exception as exc:  # pragma: no cover - streaming helper
                self.perror(f"Streaming query execution failed: {exc}")
                return
            metrics_rows = self._build_query_metrics(
                total_elapsed,
                first_row,
                streamed=True,
                response_bytes=total_bytes,
            )
            self._render_dataframe_preview(
                df,
                title="JSON query results" if isinstance(query, JSONQuery) else "Query results",
                max_rows=rows,
                metrics=metrics_rows,
            )
            return

        try:
            df, total_elapsed, response_bytes = self._run_query_to_dataframe(query)
        except Exception as exc:  # pragma: no cover - CLI wrapper
            self.perror(f"Query execution failed: {exc}")
            return

        metrics_rows = self._build_query_metrics(
            total_elapsed,
            total_elapsed,
            streamed=False,
            response_bytes=response_bytes,
        )

        title = "JSON query results" if isinstance(query, JSONQuery) else "SQL results"
        if page:
            self._render_dataframe_pager(
                df,
                title=title,
                metrics=metrics_rows,
            )
        else:
            self._render_dataframe_preview(
                df,
                title=title,
                max_rows=rows,
                metrics=metrics_rows,
            )

    def _connect_to_beacon(
        self,
        url: str,
        jwt_token: str | None = None,
        basic_auth: list[str] | None = None,
    ) -> None:
        auth_tuple = tuple(basic_auth) if basic_auth else None

        try:
            client = Client(
                url=url,
                jwt_token=jwt_token,
                basic_auth=auth_tuple,  # type: ignore[arg-type]
            )
        except Exception as exc:  # pragma: no cover - thin CLI wrapper
            self.perror(f"Failed to connect: {exc}")
            return

        self.client = client
        self.poutput(f"Connected to {url}")

    def show_dashboard(self) -> None:
        """Render a full-screen command overview using Rich."""

        terminal_width = shutil.get_terminal_size((100, 40)).columns
        self.console.clear()

        header = Panel(
            "[bold cyan]beacon_cli[/bold cyan]\nConnect to Beacon nodes and explore/manage the content.",
            border_style="cyan",
            width=terminal_width,
        )
        self.console.print(header)

        table = Table(
            expand=True, box=None, show_header=True, header_style="bold magenta"
        )
        table.add_column("Command")
        table.add_column("Description")
        table.add_row(
            "connect <url> [--jwt-token TOKEN] [--basic-auth USERNAME PASSWORD]", "Connect to a Beacon node via flags"
        )
        table.add_row("status", "Show connection health, version, and admin state")
        table.add_row(
            "list_datasets [--pattern --limit --offset]",
            "List datasets registered with the server",
        )
        table.add_row(
            "dataset_schema <path>", "Inspect a dataset's schema in a table view"
        )
        table.add_row("list_tables", "List logical tables, types, and descriptions")
        table.add_row(
            "list_table_schema <name>", "Inspect a logical table's schema"
        )
        table.add_row(
            "create_table <name> <globs...> <format>", "Create a logical table (admin)"
        )
        table.add_row("delete_table <name>", "Delete a logical table (admin)")
        table.add_row("upload_dataset <local> <dest>", "Upload a dataset file (admin)")
        table.add_row(
            "download_dataset <remote> <local>", "Download a dataset file (admin)"
        )
        table.add_row("delete_dataset <remote>", "Delete a dataset file (admin)")
        table.add_row(
            "query_builder [--table/--dataset]",
            "Build a fluent JSON query with select/filter flags",
        )
        table.add_row(
            "sql <statement> [--rows]", "Execute a SQL query and preview the result"
        )
        table.add_row("dashboard", "Reopen this overview screen")
        table.add_row("help", "List available commands and their usage. To get help on a specific command, run 'help <command>'.")
        table.add_row("quit", "Exit beacon_cli")

        panel = Panel(
            table, title="Command palette", border_style="magenta", width=terminal_width
        )
        self.console.print(panel)
        self.console.print(
            "[dim]Tip: run 'connect https://host' to start working with Beacon data.[/dim]",
            justify="center",
        )

    def _require_connection(self) -> bool:
        if self.client is None:
            self.perror("Not connected. Run 'connect <url>' first.")
            return False
        return True

    @cmd2.with_category("Session")
    @cmd2.with_argparser(CONNECT_PARSER)
    def do_connect(self, args: argparse.Namespace) -> None:
        """Connect to a Beacon node."""

        self._connect_to_beacon(
            url=args.url,
            jwt_token=args.jwt_token,
            basic_auth=list(args.basic_auth) if args.basic_auth else None,
        )

    @cmd2.with_category("Session")
    def do_status(self, _: cmd2.Statement) -> None:
        """Show connection status including Beacon version and admin state."""

        if not self._require_connection():
            return
        self._render_status()

    @cmd2.with_category("Tables")
    def do_list_tables(self, _: cmd2.Statement) -> None:
        """List available tables and their metadata."""

        if not self._require_connection():
            return

        client = cast(Client, self.client)
        try:
            tables = client.list_tables()
        except Exception as exc:  # pragma: no cover - CLI wrapper
            self.perror(f"Failed to list tables: {exc}")
            return

        if not tables:
            self.console.print(
                Panel("No tables were returned by the server.", border_style="yellow")
            )
            return

        rich_table = Table(title="Tables", show_lines=True)
        rich_table.add_column("Table name", style="bold")
        rich_table.add_column("Type", overflow="fold")
        rich_table.add_column("Description", overflow="fold")

        for name, table_obj in sorted(tables.items()):
            table_type = table_obj.get_table_type()
            if isinstance(table_type, dict):
                table_type_str = ", ".join(table_type.keys()) or "dict"
            else:
                table_type_str = str(table_type)
            rich_table.add_row(name, table_type_str, table_obj.get_table_description())

        self.console.print(rich_table)

    @cmd2.with_category("Tables")
    @cmd2.with_argparser(CREATE_TABLE_PARSER)
    def do_create_table(self, args: argparse.Namespace) -> None:
        """Create a logical table on the Beacon node (admin only)."""

        if not self._require_connection():
            return

        client = cast(Client, self.client)
        try:
            extra_kwargs = _parse_option_pairs(args.options)
        except ValueError as exc:
            self.perror(str(exc))
            return

        if args.delimiter:
            extra_kwargs["delimiter"] = args.delimiter
        if args.statistics_columns:
            extra_kwargs["statistics"] = {
                "columns": args.statistics_columns
            }

        try:
            client.create_logical_table(
                table_name=args.table_name,
                dataset_glob_paths=args.dataset_glob_paths,
                file_format=args.file_format,
                description=args.description,
                **extra_kwargs,
            )
        except Exception as exc:  # pragma: no cover - CLI wrapper
            self.perror(f"Failed to create table: {exc}")
            return

        self.console.print(
            Panel(
                f"Created logical table [bold]{args.table_name}[/bold]",
                border_style="green",
            )
        )

    @cmd2.with_category("Tables")
    @cmd2.with_argparser(DELETE_TABLE_PARSER)
    def do_delete_table(self, args: argparse.Namespace) -> None:
        """Delete a logical table from the Beacon node (admin only)."""

        if not self._require_connection():
            return

        client = cast(Client, self.client)
        try:
            client.delete_table(args.table_name)
        except Exception as exc:  # pragma: no cover - CLI wrapper
            self.perror(f"Failed to delete table: {exc}")
            return

        self.console.print(
            Panel(
                f"Deleted logical table [bold]{args.table_name}[/bold]",
                border_style="red",
            )
        )

    @cmd2.with_category("Datasets")
    @cmd2.with_argparser(DATASET_LIST_PARSER)
    def do_list_datasets(self, args: argparse.Namespace) -> None:
        """List datasets registered on the Beacon node."""

        if not self._require_connection():
            return

        client = cast(Client, self.client)
        try:
            datasets = client.list_datasets(
                pattern=args.pattern, limit=args.limit, offset=args.offset
            )
        except Exception as exc:  # pragma: no cover - CLI wrapper
            self.perror(f"Failed to list datasets: {exc}")
            return

        if not datasets:
            self.console.print(
                Panel(
                    "No datasets matched the provided filters.", border_style="yellow"
                )
            )
            return

        table = Table(title="Datasets", show_lines=True)
        table.add_column("File path", overflow="fold")
        table.add_column("Format", justify="center")
        table.add_column("File name")

        for path, dataset in datasets.items():
            table.add_row(path, dataset.get_file_format(), dataset.get_file_name())

        self.console.print(table)

    @cmd2.with_category("Datasets")
    @cmd2.with_argparser(DATASET_SCHEMA_PARSER)
    def do_dataset_schema(self, args: argparse.Namespace) -> None:
        """Display the schema for a specific dataset."""

        if not self._require_connection():
            return

        dataset = self._resolve_dataset(args.file_path)
        if dataset is None:
            return

        try:
            schema = cast(Any, dataset.get_schema())
        except Exception as exc:  # pragma: no cover - CLI wrapper
            self.perror(f"Failed to fetch schema: {exc}")
            return

        table = Table(title=f"Schema for {dataset.get_file_name()}", show_lines=True)
        table.add_column("Column", style="bold")
        table.add_column("Type")

        for field in schema:
            table.add_row(field.name, str(field.type))

        self.console.print(table)

    @cmd2.with_category("Tables")
    @cmd2.with_argparser(TABLE_SCHEMA_PARSER)
    def do_list_table_schema(self, args: argparse.Namespace) -> None:
        """Display the schema for a logical table."""

        if not self._require_connection():
            return

        table_obj = self._resolve_table(args.table_name)
        if table_obj is None:
            return

        try:
            schema = cast(Any, table_obj.get_table_schema())
        except Exception as exc:  # pragma: no cover - CLI wrapper
            self.perror(f"Failed to fetch table schema: {exc}")
            return

        table = Table(title=f"Schema for table {args.table_name}", show_lines=True)
        table.add_column("Column", style="bold")
        table.add_column("Type")

        for field in schema:
            table.add_row(field.name, str(field.type))

        self.console.print(table)

    @cmd2.with_category("Datasets")
    @cmd2.with_argparser(DATASET_UPLOAD_PARSER)
    def do_upload_dataset(self, args: argparse.Namespace) -> None:
        """Upload a dataset file to the Beacon node (admin only)."""

        if not self._require_connection():
            return

        local_path = Path(args.local_path).expanduser()
        if not local_path.exists() or not local_path.is_file():
            self.perror(f"Local file '{local_path}' does not exist or is not a file.")
            return

        client = cast(Client, self.client)
        try:
            client.upload_dataset(str(local_path), args.destination_path, force=args.force)
        except Exception as exc:  # pragma: no cover - CLI wrapper
            self.perror(f"Upload failed: {exc}")
            return

        self.console.print(
            Panel(
                f"Uploaded [bold]{local_path.name}[/bold] to [green]{args.destination_path}[/green]",
                border_style="green",
            )
        )

    @cmd2.with_category("Datasets")
    @cmd2.with_argparser(DATASET_DOWNLOAD_PARSER)
    def do_download_dataset(self, args: argparse.Namespace) -> None:
        """Download a dataset file from the Beacon node (admin only)."""

        if not self._require_connection():
            return

        local_path = Path(args.local_path).expanduser()
        client = cast(Client, self.client)
        try:
            client.download_dataset(args.dataset_path, str(local_path), force=args.force)
        except Exception as exc:  # pragma: no cover - CLI wrapper
            self.perror(f"Download failed: {exc}")
            return

        self.console.print(
            Panel(
                f"Downloaded [bold]{args.dataset_path}[/bold] to [green]{local_path}[/green]",
                border_style="green",
            )
        )

    @cmd2.with_category("Datasets")
    @cmd2.with_argparser(DATASET_DELETE_PARSER)
    def do_delete_dataset(self, args: argparse.Namespace) -> None:
        """Delete a dataset from the Beacon node (admin only)."""

        if not self._require_connection():
            return

        client = cast(Client, self.client)
        try:
            client.delete_dataset(args.dataset_path, force=args.force)
        except Exception as exc:  # pragma: no cover - CLI wrapper
            self.perror(f"Delete failed: {exc}")
            return

        self.console.print(
            Panel(
                f"Deleted dataset [bold]{args.dataset_path}[/bold]",
                border_style="red",
            )
        )

    def _render_dataframe_table(
        self,
        df: pd.DataFrame,
        title: str,
        *,
        max_rows: int | None,
        pager: bool = False,
        metrics: list[tuple[str, str]] | None = None,
    ) -> None:
        """Render a DataFrame with Rich, optionally piping output through a pager."""

        panel: Panel
        if df.empty:
            message = "Query returned no rows"
            if metrics:
                metric_lines = "\n".join(f"{name}: {value}" for name, value in metrics)
                message = f"{message}\n{metric_lines}"
            panel = Panel(message, title=title, border_style="red")
        else:
            display_df = df if max_rows is None else df.head(max_rows)
            try:
                column_mem = df.memory_usage(index=False, deep=True)
            except Exception:
                column_mem = None
            label_map: dict[str, str] = {}
            for column in display_df.columns:
                dtype = (
                    df[column].dtype
                    if column in df.columns
                    else display_df[column].dtype
                )
                size_bytes: int | None = None
                if column_mem is not None and column in column_mem:
                    size_value = column_mem[column]
                    size_bytes = int(size_value)
                size_str = self._format_bytes(size_bytes)
                label_map[column] = f"{column}({dtype}, {size_str})"
            display_df = display_df.rename(columns=label_map)
            table = Table(
                title=title,
                show_header=True,
                header_style="bold green",
                box=box.MINIMAL_DOUBLE_HEAD,
                expand=True,
            )
            column_names = list(display_df.columns)
            if not column_names:
                table.add_column("Value")
                column_names = ["Value"]
            else:
                for column in column_names:
                    table.add_column(str(column))
            for _, row in display_df.iterrows():
                table.add_row(*(str(row[col]) for col in column_names))

            if metrics:
                table.add_section()
                column_count = len(column_names)
                for name, value in metrics:
                    cells = [f"{name}: {value}"] + [""] * (column_count - 1)
                    table.add_row(*cells)

            if max_rows is None or len(display_df) == len(df):
                subtitle = f"{len(df)} rows"
            else:
                subtitle = f"showing {len(display_df)} of {len(df)} rows"
            panel = Panel(table, border_style="green", subtitle=subtitle)

        if pager:
            with self.console.pager(styles=True):
                self.console.print(panel)
        else:
            self.console.print(panel)

    def _render_dataframe_preview(
        self,
        df: pd.DataFrame,
        title: str,
        max_rows: int,
        metrics: list[tuple[str, str]] | None = None,
    ) -> None:
        self._render_dataframe_table(
            df,
            title,
            max_rows=max_rows,
            metrics=metrics,
        )

    def _render_dataframe_pager(
        self, df: pd.DataFrame, title: str, metrics: list[tuple[str, str]] | None = None
    ) -> None:
        self._render_dataframe_table(
            df,
            title,
            max_rows=None,
            pager=True,
            metrics=metrics,
        )

    @cmd2.with_category("Querying")
    @cmd2.with_argparser(SQL_PARSER)
    def do_sql(self, args: argparse.Namespace) -> None:
        """Execute a SQL statement against the connected Beacon node."""

        if not self._require_connection():
            return

        if not args.sql:
            self.perror(
                "Provide a SQL statement, e.g. sql SELECT * FROM default LIMIT 5"
            )
            return

        sql_text = " ".join(args.sql).strip()
        if not sql_text:
            self.perror("SQL statement cannot be empty")
            return

        if not self._validate_output_flags(args.format, args.page, args.stream):
            return

        client = cast(Client, self.client)
        try:
            query = client.sql_query(sql_text)
        except Exception as exc:  # pragma: no cover - CLI wrapper
            self.perror(f"SQL execution failed: {exc}")
            return
        self._render_query_results(
            query,
            fmt=args.format,
            rows=args.rows,
            page=args.page,
            stream=args.stream,
            output_path=args.output_path,
            geo_columns=tuple(args.geo_columns) if args.geo_columns else None,
        )

    @cmd2.with_category("Querying")
    @cmd2.with_argparser(QUERY_BUILDER_PARSER)
    def do_query_builder(self, args: argparse.Namespace) -> None:
        """Build and execute a JSON query using the fluent query builder."""

        if not self._require_connection():
            return

        if not args.select:
            self.perror("Provide at least one --select COLUMN[:ALIAS] to define the projection")
            return

        if not self._validate_output_flags(args.format, args.page, args.stream):
            return

        query: JSONQuery
        if args.table:
            table_obj = self._resolve_table(args.table)
            if table_obj is None:
                return
            query = table_obj.query()
        else:
            dataset = self._resolve_dataset(str(args.dataset))
            if dataset is None:
                return
            query = dataset.query()

        # Select columns
        for select_spec in args.select or []:
            try:
                column, alias = _parse_select_spec(select_spec)
            except ValueError as exc:
                self.perror(str(exc))
                return
            query.add_select_column(column, alias=alias)

        # Equality filters
        for equals_spec in args.equals or []:
            try:
                column, raw_value = _parse_key_value(equals_spec)
            except ValueError as exc:
                self.perror(str(exc))
                return
            query.add_equals_filter(column, _coerce_value(raw_value))

        # Not-equals filters
        for not_equals_spec in args.not_equals or []:
            try:
                column, raw_value = _parse_key_value(not_equals_spec)
            except ValueError as exc:
                self.perror(str(exc))
                return
            query.add_not_equals_filter(column, _coerce_value(raw_value))

        # Range filters
        for column, raw_min, raw_max in args.ranges or []:
            min_value = _parse_optional_value(raw_min)
            max_value = _parse_optional_value(raw_max)
            query.add_range_filter(column, gt_eq=min_value, lt_eq=max_value)

        # Bounding boxes
        for bbox_args in args.bboxes or []:
            if len(bbox_args) != 6:
                self.perror("--bbox expects LON_COL LAT_COL MIN_LON MAX_LON MIN_LAT MAX_LAT")
                return
            lon_col, lat_col, min_lon, max_lon, min_lat, max_lat = bbox_args
            try:
                bbox = (
                    float(min_lon),
                    float(max_lon),
                    float(min_lat),
                    float(max_lat),
                )
            except ValueError:
                self.perror("Bounding box coordinates must be numeric")
                return
            query.add_bbox_filter(lon_col, lat_col, bbox)

        for column in args.is_null or []:
            query.add_is_null_filter(column)

        for column in args.is_not_null or []:
            query.add_is_not_null_filter(column)

        if args.distinct:
            query.set_distinct(args.distinct)

        for sort_spec in args.sort or []:
            column, _, direction = sort_spec.partition(":")
            column = column.strip()
            if not column:
                self.perror("--sort values must include a column name")
                return
            ascending = True
            if direction:
                direction_lower = direction.strip().lower()
                if direction_lower in {"desc", "descending"}:
                    ascending = False
                elif direction_lower in {"asc", "ascending", ""}:
                    ascending = True
                else:
                    self.perror("--sort direction must be 'asc' or 'desc'")
                    return
            query.add_sort(column, ascending=ascending)

        self._render_query_results(
            query,
            fmt=args.format,
            rows=args.rows,
            page=args.page,
            stream=args.stream,
            output_path=args.output_path,
            geo_columns=tuple(args.geo_columns) if args.geo_columns else None,
        )

    @cmd2.with_category("Interface")
    def do_dashboard(self, _: cmd2.Statement) -> None:
        """Display the full-screen command overview."""

        self.show_dashboard()


def main() -> None:
    startup_parser = _build_startup_parser()
    startup_args = startup_parser.parse_args()

    app = BeaconCli()
    app.show_dashboard()
    if startup_args.url:
        namespace = argparse.Namespace(
            url=startup_args.url,
            jwt_token=startup_args.jwt_token,
            basic_auth=None,
        )
        app.do_connect(namespace)
        app.poutput("(auto-connect) REPL ready - try 'list_tables' or 'status'.")

    app.cmdloop()


if __name__ == "__main__":
    main()
