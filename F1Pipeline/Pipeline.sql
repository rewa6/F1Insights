-- Databricks notebook source
-- CIRCUITS
CREATE OR REFRESH STREAMING TABLE bronze_circuits
COMMENT 'Raw circuits data'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, CURRENT_TIMESTAMP AS ingestion_timestamp
FROM STREAM(
  read_files(
    '/Volumes/f1project/data/raw/circuits.csv',
    format => 'csv',
    header => true
  )
);

-- RACES
CREATE OR REFRESH STREAMING TABLE bronze_races
COMMENT 'Raw races data'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, CURRENT_TIMESTAMP AS ingestion_timestamp
FROM STREAM(
  read_files(
    '/Volumes/f1project/data/raw/races.csv',
    format => 'csv',
    header => true
  )
);

-- CONSTRUCTORS
CREATE OR REFRESH STREAMING TABLE bronze_constructors
COMMENT 'Raw constructors data'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, CURRENT_TIMESTAMP AS ingestion_timestamp
FROM STREAM(
  read_files(
    '/Volumes/f1project/data/raw/constructors.csv',
    format => 'csv',
    header => true
  )
);

-- DRIVERS
CREATE OR REFRESH STREAMING TABLE bronze_drivers
COMMENT 'Raw drivers data'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, CURRENT_TIMESTAMP AS ingestion_timestamp
FROM STREAM(
  read_files(
    '/Volumes/f1project/data/raw/drivers.csv',
    format => 'csv',
    header => true
  )
);

-- LAPTIMES
CREATE OR REFRESH STREAMING TABLE bronze_laptimes
COMMENT 'Raw laptimes data'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, CURRENT_TIMESTAMP AS ingestion_timestamp
FROM STREAM(
  read_files(
    '/Volumes/f1project/data/raw/lap_times.csv',
    format => 'csv',
    header => true
  )
);

-- QUALIFYING
CREATE OR REFRESH STREAMING TABLE bronze_qualifying
COMMENT 'Raw qualifying data'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, CURRENT_TIMESTAMP AS ingestion_timestamp
FROM STREAM(
  read_files(
    '/Volumes/f1project/data/raw/qualifying.csv',
    format => 'csv',
    header => true
  )
);

-- PITSTOPS
CREATE OR REFRESH STREAMING TABLE bronze_pitstops
COMMENT 'Raw pitstops data'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, CURRENT_TIMESTAMP AS ingestion_timestamp
FROM STREAM(
  read_files(
    '/Volumes/f1project/data/raw/pit_stops.csv',
    format => 'csv',
    header => true
  )
);

-- RESULTS
CREATE OR REFRESH STREAMING TABLE bronze_results
COMMENT 'Raw results data'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, CURRENT_TIMESTAMP AS ingestion_timestamp
FROM STREAM(
  read_files(
    '/Volumes/f1project/data/raw/results.csv',
    format => 'csv',
    header => true
  )
);

-- COMMAND ----------

-- CIRCUITS CLEAN
CREATE OR REFRESH STREAMING TABLE silver_circuits
COMMENT 'Cleaned circuits data'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT DISTINCT
    CAST(circuitId AS INT) AS circuit_id,
    INITCAP(name) AS circuit_name,
    INITCAP(location) AS location,
    INITCAP(country) AS country,
    ingestion_timestamp
FROM STREAM(bronze_circuits)
WHERE circuitId IS NOT NULL;

-- RACES CLEAN
CREATE OR REFRESH STREAMING TABLE silver_races
COMMENT 'Cleaned races data'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT DISTINCT
    CAST(raceId AS INT) AS race_id,
    CAST(year AS INT) AS race_year,
    CAST(round AS INT) AS round,
    INITCAP(name) AS race_name,
    CAST(date AS DATE) AS race_date,
    circuitId AS circuit_id,
    ingestion_timestamp
FROM STREAM(bronze_races)
WHERE raceId IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE silver_drivers
COMMENT 'Cleaned drivers data'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT DISTINCT
    CAST(driverId AS INT) AS driver_id,
    INITCAP(forename) || ' ' || INITCAP(surname) AS driver_name,
    nationality,
    dob AS date_of_birth,
    ingestion_timestamp
FROM STREAM(bronze_drivers)
WHERE driverId IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE silver_constructors
COMMENT 'Cleaned constructors data'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT DISTINCT
    CAST(constructorId AS INT) AS constructor_id,
    INITCAP(name) AS constructor_name,
    nationality,
    ingestion_timestamp
FROM STREAM(bronze_constructors)
WHERE constructorId IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE silver_results
COMMENT 'Cleaned race results data'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT DISTINCT
    CAST(resultId AS INT) AS result_id,
    CAST(raceId AS INT) AS race_id,
    CAST(driverId AS INT) AS driver_id,
    CAST(constructorId AS INT) AS constructor_id,
    CAST(grid AS INT) AS grid_position,
    CAST(position AS INT) AS finish_position,
    CAST(points AS DOUBLE) AS points,
    CAST(laps AS INT) AS laps_completed,
    ingestion_timestamp
FROM STREAM(bronze_results)
WHERE resultId IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE silver_laptimes
COMMENT 'Cleaned lap times data'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT DISTINCT
    CAST(raceId AS INT) AS race_id,
    CAST(driverId AS INT) AS driver_id,
    CAST(lap AS INT) AS lap_number,
    CAST(position AS INT) AS position,
    CAST(milliseconds AS BIGINT) AS lap_time_ms,
    ingestion_timestamp
FROM STREAM(bronze_laptimes)
WHERE raceId IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE silver_pitstops
COMMENT 'Cleaned pit stops data'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT DISTINCT
    CAST(raceId AS INT) AS race_id,
    CAST(driverId AS INT) AS driver_id,
    CAST(stop AS INT) AS stop_number,
    CAST(lap AS INT) AS lap_number,
    CAST(duration AS DOUBLE) AS duration_sec,
    ingestion_timestamp
FROM STREAM(bronze_pitstops)
WHERE raceId IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE silver_qualifying
COMMENT 'Cleaned qualifying data'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT DISTINCT
    CAST(qualifyId AS INT) AS qualify_id,
    CAST(raceId AS INT) AS race_id,
    CAST(driverId AS INT) AS driver_id,
    CAST(constructorId AS INT) AS constructor_id,
    CAST(position AS INT) AS qualifying_position,
    ingestion_timestamp
FROM STREAM(bronze_qualifying)
WHERE qualifyId IS NOT NULL;

-- COMMAND ----------

-- DRIVER PERFORMANCE
CREATE OR REFRESH LIVE TABLE gold_driver_performance
COMMENT 'Driver performance metrics'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT d.driver_id, d.driver_name, r.race_year, SUM(res.points) AS total_points,
       COUNT(DISTINCT r.race_id) AS races_participated,
       AVG(res.points) AS avg_points
FROM LIVE.silver_results res
JOIN LIVE.silver_drivers d ON res.driver_id = d.driver_id
JOIN LIVE.silver_races r ON res.race_id = r.race_id
GROUP BY d.driver_id, d.driver_name, r.race_year;

-- TEAM STANDINGS
CREATE OR REFRESH LIVE TABLE gold_team_standings
COMMENT 'Constructor standings per season'
AS
SELECT c.constructor_name, r.race_year, SUM(res.points) AS team_points
FROM LIVE.silver_results res
JOIN LIVE.silver_constructors c ON res.constructor_id = c.constructor_id
JOIN LIVE.silver_races r ON res.race_id = r.race_id
GROUP BY c.constructor_name, r.race_year;

-- PIT STOP ANALYSIS
CREATE OR REFRESH LIVE TABLE gold_pit_stop_analysis
COMMENT 'Average pit stop duration per driver per race'
AS
SELECT ps.driver_id, r.race_year, AVG(ps.duration_sec) AS avg_pit_duration
FROM LIVE.silver_pitstops ps
JOIN LIVE.silver_races r ON ps.race_id = r.race_id
GROUP BY ps.driver_id, r.race_year;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE gold_f1_insights
COMMENT 'Unified F1 analytics table combining race, driver, constructor, and performance metrics'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
    r.race_id,
    r.race_year,
    r.race_name,
    c.circuit_name,
    c.country,
    d.driver_id,
    d.driver_name,
    d.nationality AS driver_nationality,
    cons.constructor_id,
    cons.constructor_name,
    res.grid_position,
    res.finish_position,
    res.points,
    res.laps_completed,
    q.qualifying_position,
    ps.avg_pit_duration,
    lt.avg_lap_time_ms,
    CURRENT_TIMESTAMP AS updated_at
FROM LIVE.silver_results res
JOIN LIVE.silver_races r ON res.race_id = r.race_id
JOIN LIVE.silver_circuits c ON r.circuit_id = c.circuit_id
JOIN LIVE.silver_drivers d ON res.driver_id = d.driver_id
JOIN LIVE.silver_constructors cons ON res.constructor_id = cons.constructor_id
LEFT JOIN (
    SELECT race_id, driver_id, AVG(duration_sec) AS avg_pit_duration
    FROM LIVE.silver_pitstops
    GROUP BY race_id, driver_id
) ps ON res.race_id = ps.race_id AND res.driver_id = ps.driver_id
LEFT JOIN (
    SELECT race_id, driver_id, AVG(lap_time_ms) AS avg_lap_time_ms
    FROM LIVE.silver_laptimes
    GROUP BY race_id, driver_id
) lt ON res.race_id = lt.race_id AND res.driver_id = lt.driver_id
LEFT JOIN LIVE.silver_qualifying q ON res.race_id = q.race_id AND res.driver_id = q.driver_id;