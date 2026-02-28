-- Airviro warehouse bootstrap objects.
-- Safe to run repeatedly.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS raw.airviro_measurement (
  source_type text NOT NULL,
  station_id integer NOT NULL,
  observed_at timestamp without time zone NOT NULL,
  indicator_code text NOT NULL,
  indicator_name text NOT NULL,
  value_numeric double precision,
  source_row_hash text NOT NULL,
  extracted_at timestamp with time zone NOT NULL DEFAULT now(),
  CONSTRAINT airviro_measurement_pk
    PRIMARY KEY (source_type, station_id, observed_at, indicator_code)
);

CREATE INDEX IF NOT EXISTS idx_airviro_measurement_observed_at
  ON raw.airviro_measurement (observed_at);

CREATE INDEX IF NOT EXISTS idx_airviro_measurement_source_indicator
  ON raw.airviro_measurement (source_type, indicator_code);

CREATE TABLE IF NOT EXISTS raw.airviro_ingestion_audit (
  ingestion_audit_id bigserial PRIMARY KEY,
  batch_id text NOT NULL,
  source_type text NOT NULL,
  window_start timestamp with time zone NOT NULL,
  window_end timestamp with time zone NOT NULL,
  rows_read integer NOT NULL,
  records_upserted integer NOT NULL,
  duplicate_records integer NOT NULL,
  split_events integer NOT NULL,
  status text NOT NULL,
  message text,
  created_at timestamp with time zone NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mart.dim_indicator (
  source_type text NOT NULL,
  indicator_code text NOT NULL,
  indicator_name text NOT NULL,
  PRIMARY KEY (source_type, indicator_code)
);

CREATE TABLE IF NOT EXISTS mart.dim_datetime_hour (
  observed_at timestamp without time zone PRIMARY KEY,
  date_value date NOT NULL,
  year_number integer NOT NULL,
  quarter_number integer NOT NULL,
  month_number integer NOT NULL,
  month_name text NOT NULL,
  day_number integer NOT NULL,
  hour_number integer NOT NULL,
  iso_week_number integer NOT NULL,
  day_of_week_number integer NOT NULL,
  day_name text NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.dim_wind_direction (
  sector_id integer PRIMARY KEY,
  sector_code text NOT NULL UNIQUE,
  sector_name text NOT NULL,
  min_degree double precision NOT NULL,
  max_degree double precision NOT NULL,
  wraps_around boolean NOT NULL
);

INSERT INTO mart.dim_wind_direction (
  sector_id, sector_code, sector_name, min_degree, max_degree, wraps_around
)
VALUES
  (1,  'N',   'North',             348.75, 11.25,  true),
  (2,  'NNE', 'North-Northeast',    11.25, 33.75,  false),
  (3,  'NE',  'Northeast',          33.75, 56.25,  false),
  (4,  'ENE', 'East-Northeast',     56.25, 78.75,  false),
  (5,  'E',   'East',               78.75, 101.25, false),
  (6,  'ESE', 'East-Southeast',    101.25, 123.75, false),
  (7,  'SE',  'Southeast',         123.75, 146.25, false),
  (8,  'SSE', 'South-Southeast',   146.25, 168.75, false),
  (9,  'S',   'South',             168.75, 191.25, false),
  (10, 'SSW', 'South-Southwest',   191.25, 213.75, false),
  (11, 'SW',  'Southwest',         213.75, 236.25, false),
  (12, 'WSW', 'West-Southwest',    236.25, 258.75, false),
  (13, 'W',   'West',              258.75, 281.25, false),
  (14, 'WNW', 'West-Northwest',    281.25, 303.75, false),
  (15, 'NW',  'Northwest',         303.75, 326.25, false),
  (16, 'NNW', 'North-Northwest',   326.25, 348.75, false)
ON CONFLICT (sector_id) DO NOTHING;

CREATE OR REPLACE VIEW mart.v_airviro_measurements_long AS
SELECT
  m.source_type,
  m.station_id,
  m.observed_at,
  m.indicator_code,
  m.indicator_name,
  m.value_numeric,
  m.extracted_at
FROM raw.airviro_measurement AS m;

CREATE OR REPLACE VIEW mart.v_air_quality_hourly AS
WITH air_quality AS (
  SELECT
    station_id,
    observed_at,
    MAX(value_numeric) FILTER (WHERE indicator_code = 'so2') AS so2,
    MAX(value_numeric) FILTER (WHERE indicator_code = 'no2') AS no2,
    MAX(value_numeric) FILTER (WHERE indicator_code = 'co') AS co,
    MAX(value_numeric) FILTER (WHERE indicator_code = 'o3') AS o3,
    MAX(value_numeric) FILTER (WHERE indicator_code = 'pm10') AS pm10,
    MAX(value_numeric) FILTER (WHERE indicator_code = 'pm2_5') AS pm2_5,
    MAX(value_numeric) FILTER (WHERE indicator_code = 'temp') AS temp,
    MAX(value_numeric) FILTER (WHERE indicator_code = 'wd10') AS wd10,
    MAX(value_numeric) FILTER (WHERE indicator_code = 'ws10') AS ws10
  FROM raw.airviro_measurement
  WHERE source_type = 'air_quality'
  GROUP BY station_id, observed_at
)
SELECT
  aq.station_id,
  aq.observed_at,
  dt.date_value,
  dt.year_number,
  dt.month_number,
  dt.day_number,
  dt.hour_number,
  aq.so2,
  aq.no2,
  aq.co,
  aq.o3,
  aq.pm10,
  aq.pm2_5,
  aq.temp,
  aq.wd10,
  aq.ws10,
  wd.sector_code AS wind_sector,
  wd.sector_name AS wind_sector_name
FROM air_quality AS aq
LEFT JOIN mart.dim_datetime_hour AS dt
  ON dt.observed_at = aq.observed_at
LEFT JOIN mart.dim_wind_direction AS wd
  ON (
    (wd.wraps_around IS TRUE AND (aq.wd10 >= wd.min_degree OR aq.wd10 < wd.max_degree))
    OR
    (wd.wraps_around IS FALSE AND aq.wd10 >= wd.min_degree AND aq.wd10 < wd.max_degree)
  );

CREATE OR REPLACE VIEW mart.v_pollen_daily AS
SELECT
  m.station_id,
  m.observed_at,
  m.observed_at::date AS observed_date,
  m.indicator_code,
  m.indicator_name,
  m.value_numeric
FROM raw.airviro_measurement AS m
WHERE m.source_type = 'pollen';

