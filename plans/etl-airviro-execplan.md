# Airviro ETL + Warehouse Bootstrap (2020-2025)

This ExecPlan is a living document. Update `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` as work advances.

Reference: `PLANS.md` (repository root) for standards.

## Purpose / Big Picture

Build an instructional, idempotent ETL pipeline that ingests Airviro station CSV data (air quality + pollen), applies type normalization and integrity checks, and loads warehouse tables that are easy to visualize in Superset.

The design should also prepare for the Airflow lecture (incremental updates + backfill from 2020 to present).

## Student Learning Impact

- Lecture 2: students run a real ETL pipeline with defensive extraction, validation, and loading.
- Lecture 3: the same pipeline can be orchestrated in Airflow with backfill and idempotent reruns.
- Students learn practical batch-windowing strategy for unstable APIs.

## Scope

In scope:
- Airviro extraction and transformation code.
- Warehouse DDL + curated views/dimensions for Superset.
- CLI and Make target(s) for local runs.
- Documentation for usage and precipitation data source recommendations.

Out of scope:
- Full Airflow DAG implementation (prepared for later lecture, not fully wired now).
- dbt model set (pipeline outputs will be dbt-ready).

## Progress

- [x] Investigate Airviro query behavior and failure modes
- [x] Investigate precipitation open-data options for Tartu
- [x] Implement ETL extraction/transform/load package
- [x] Add warehouse schema bootstrap SQL
- [x] Add run commands and docs
- [x] Run validation checks
- [x] Final review and cleanup

## Surprises & Discoveries

- Discovery: Airviro uses semicolon CSV with localized numeric formats.
  Evidence: Sample responses contain decimal commas (`1,8`) and thousands separators with non-breaking spaces (`3 061`).

- Discovery: Large query windows can fail with HTTP 503.
  Evidence: probing station `8` from `01.01.2025` to `31.12.2025` returned HTTP 503, while windows up to 330 days returned HTTP 200.

- Discovery: Pollen endpoints tolerate much larger windows than air quality queries.
  Evidence: multi-year pollen query for station `25` returned HTTP 200 with valid CSV.

- Discovery: The referenced design-pattern article is member-gated in publication view.
  Evidence: full pattern text was accessible via the provided Medium friend link variant (`?sk=...`).

- Discovery: Warehouse DB access from devcontainer required host gateway fallback.
  Evidence: `postgres`, `localhost`, and `host.docker.internal` failed in this environment; connection succeeded via the default gateway (`172.17.0.1`) after exposing Postgres port.

- Discovery: Attaching devcontainer to compose network enables direct `postgres` DNS resolution.
  Evidence: after network attach, ETL connector selects `postgres` host before gateway fallback.

## Decision Log

- Decision: Use adaptive chunked batch extraction (split window on server failures/timeouts).
  Rationale: Handles Airviro large-window failures while minimizing request count in normal path.
  Date: 2026-02-28

- Decision: Store raw measurements in long format plus curated views for serving.
  Rationale: Source columns differ by endpoint and may evolve; long format is robust and easier for idempotent upserts.
  Date: 2026-02-28

- Decision: Build idempotency on natural keys (`source_type`, `station_id`, `observed_at`, `indicator_code`).
  Rationale: Safe reruns/backfills without duplicate facts.
  Date: 2026-02-28

- Decision: Add a helper Make target to attach the devcontainer to compose network.
  Rationale: Keeps student workflow in devcontainer while making DB host resolution deterministic.
  Date: 2026-02-28

## Outcomes & Retrospective

Implemented:
- Airviro ETL package with adaptive extraction, parsing, quality checks, and idempotent upserts.
- Warehouse SQL bootstrap with raw table, audit table, dimensions, and curated serving views.
- Makefile targets and documentation for dry-runs and full backfills.
- Makefile helper target for devcontainer network attachment (`make devcontainer-join-course-network`).
- Precipitation-source investigation note with recommended and alternative sources.

Validation summary:
- Dry run (`2025-01-01..2025-01-31`) succeeded for both sources.
- Full load (`2020-01-01..2025-12-31`) succeeded with `488742` raw measurements.
- Reruns are idempotent by natural key conflict handling.
- Curated views are populated and queryable for Superset.

## Context and Orientation

Key files:
- `docker-compose.yml` (Postgres + warehouse DB already provisioned).
- `.env` / `.env.example` (warehouse credentials).
- `Makefile` (entry points for local workflows).
- `postgres/init/01-create-app-databases.sh` (creates `warehouse` DB).

Planned new files:
- `etl/airviro/*.py` (pipeline implementation).
- `sql/warehouse/airviro_schema.sql` (tables/views/dimensions).
- `docs/etl-airviro.md` (usage + design notes + precipitation sources).

## Plan of Work

### Phase 1: Baseline and Design

- Confirm Airviro CSV shapes and parsing edge cases.
- Lock down keying strategy and idempotent merge behavior.

### Phase 2: Implementation

- Implement extractor with retries + adaptive date-range splitting.
- Implement parser/normalizer and quality checks.
- Implement database loader with upserts.
- Add warehouse bootstrap SQL.

### Phase 3: Validation and Documentation

- Dry-run parse checks on sample windows.
- End-to-end load to warehouse.
- Document commands and design principles for lectures.

## Concrete Steps

- Run: `make up-superset`
  Expected: Postgres/Superset services are available.
- Run: `.venv/bin/python -m etl.airviro.cli backfill --from 2020-01-01 --to 2025-12-31`
  Expected: warehouse tables filled with idempotent upserts.
- Run: `.venv/bin/python -m etl.airviro.cli run --from 2025-01-01 --to 2025-01-31 --dry-run`
  Expected: quality checks pass and summary prints.

## Validation and Acceptance

- Rerunning the same window does not create duplicates.
- Type conversion handles localized numbers and datetime values.
- Failed large windows recover via chunk splitting.
- Superset-readable curated views exist in `warehouse`.

## Idempotence and Recovery

- Upsert on natural keys makes retries safe.
- Loader is transactional by chunk.
- Failed chunks are logged and can be re-run independently.

## Artifacts and Notes

- Research notes:
  - Airviro sample query tests and window-limit probing.
  - Precipitation-source discovery links to be documented in `docs/etl-airviro.md`.

## Interfaces and Dependencies

- Dependencies: Python 3 stdlib + `psycopg2-binary`.
- Inputs: Airviro CSV endpoint query parameters (`stationId`, `dateFrom`, `dateUntil`, optional pollen type fields).
- Outputs: warehouse schemas/tables/views for Superset and later Airflow/dbt layers.
