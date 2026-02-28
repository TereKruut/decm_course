"""CLI entry points for the Airviro ETL pipeline."""

from __future__ import annotations

from argparse import ArgumentParser
from dataclasses import asdict
from datetime import date, datetime, time
from pathlib import Path
import json
import sys
import uuid

from .config import Settings, load_env_file
from .db import (
    apply_schema,
    connect_warehouse,
    log_ingestion_audit,
    refresh_dimensions,
    upsert_measurements,
)
from .pipeline import (
    DataQualityError,
    PipelineError,
    SourceRunSummary,
    build_source_records,
    get_source_configs,
    parse_iso_date,
    summarize_indicator_counts,
)


SCHEMA_SQL_PATH = Path("sql/warehouse/airviro_schema.sql")


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(description="Airviro ETL runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    bootstrap_parser = subparsers.add_parser(
        "bootstrap-db", help="Create/update warehouse schema objects"
    )
    bootstrap_parser.add_argument(
        "--schema-sql",
        default=str(SCHEMA_SQL_PATH),
        help="Path to schema SQL file",
    )

    run_parser = subparsers.add_parser(
        "run", help="Extract, transform, and load a custom date range"
    )
    run_parser.add_argument("--from", dest="from_date", required=True, help="YYYY-MM-DD")
    run_parser.add_argument("--to", dest="to_date", required=True, help="YYYY-MM-DD")
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run extraction + validation only, without database writes",
    )
    run_parser.add_argument(
        "--schema-sql",
        default=str(SCHEMA_SQL_PATH),
        help="Path to schema SQL file",
    )

    backfill_parser = subparsers.add_parser(
        "backfill", help="Backfill from a start date to today (or provided end date)"
    )
    backfill_parser.add_argument("--from", dest="from_date", default="2020-01-01")
    backfill_parser.add_argument(
        "--to",
        dest="to_date",
        default=date.today().strftime("%Y-%m-%d"),
    )
    backfill_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run extraction + validation only, without database writes",
    )
    backfill_parser.add_argument(
        "--schema-sql",
        default=str(SCHEMA_SQL_PATH),
        help="Path to schema SQL file",
    )

    return parser


def run_pipeline(settings: Settings, start_date: date, end_date: date, *, dry_run: bool, schema_sql: Path) -> dict[str, object]:
    if end_date < start_date:
        raise ValueError("--to must be on or after --from")

    batch_id = str(uuid.uuid4())
    summaries: list[SourceRunSummary] = []

    connection = None
    selected_host = None
    if not dry_run:
        connection, selected_host = connect_warehouse(settings)
        apply_schema(connection, schema_sql)

    try:
        for source in get_source_configs(settings):
            summary = SourceRunSummary(source_type=source.source_type)
            summaries.append(summary)
            records = build_source_records(settings, source, start_date, end_date, summary)
            summary.measurements_upserted = len(records)

            if dry_run:
                indicator_counts = summarize_indicator_counts(records)
                print(
                    json.dumps(
                        {
                            "source_type": source.source_type,
                            "mode": "dry_run",
                            "rows_read": summary.rows_read,
                            "measurements_normalized": len(records),
                            "indicator_counts": indicator_counts,
                            "split_events": summary.split_events,
                            "duplicate_measurements": summary.duplicate_measurements,
                        },
                        indent=2,
                    )
                )
                continue

            assert connection is not None
            loaded_count = upsert_measurements(connection, records)
            summary.measurements_upserted = loaded_count
            log_ingestion_audit(
                connection,
                batch_id=batch_id,
                source_type=source.source_type,
                window_start=datetime.combine(start_date, time.min),
                window_end=datetime.combine(end_date, time.max),
                rows_read=summary.rows_read,
                records_upserted=loaded_count,
                duplicate_records=summary.duplicate_measurements,
                split_events=summary.split_events,
                status="success",
                message=None,
            )

        if not dry_run:
            assert connection is not None
            refresh_dimensions(connection)

    except Exception as exc:
        if not dry_run and connection is not None:
            for summary in summaries:
                log_ingestion_audit(
                    connection,
                    batch_id=batch_id,
                    source_type=summary.source_type,
                    window_start=datetime.combine(start_date, time.min),
                    window_end=datetime.combine(end_date, time.max),
                    rows_read=summary.rows_read,
                    records_upserted=summary.measurements_upserted,
                    duplicate_records=summary.duplicate_measurements,
                    split_events=summary.split_events,
                    status="failed",
                    message=str(exc)[:500],
                )
        raise
    finally:
        if connection is not None:
            connection.close()

    summary_payload = {
        "batch_id": batch_id,
        "from_date": start_date.isoformat(),
        "to_date": end_date.isoformat(),
        "dry_run": dry_run,
        "database_host": selected_host,
        "sources": [asdict(item) for item in summaries],
    }
    return summary_payload


def main(argv: list[str] | None = None) -> int:
    load_env_file(Path(".env"))
    args = build_parser().parse_args(argv)
    settings = Settings.from_env()

    try:
        if args.command == "bootstrap-db":
            connection, selected_host = connect_warehouse(settings)
            try:
                apply_schema(connection, Path(args.schema_sql))
            finally:
                connection.close()
            print(f"Warehouse schema ensured on host '{selected_host}'.")
            return 0

        if args.command in {"run", "backfill"}:
            start_date = parse_iso_date(args.from_date)
            end_date = parse_iso_date(args.to_date)
            result = run_pipeline(
                settings,
                start_date,
                end_date,
                dry_run=args.dry_run,
                schema_sql=Path(args.schema_sql),
            )
            print(json.dumps(result, indent=2))
            return 0

        raise ValueError(f"Unsupported command: {args.command}")
    except (PipelineError, ValueError, RuntimeError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

