
-- Disable locking to allow reading and writing the same table
-- in one statement. DummyTxnManager does not release SHARED locks
-- between statements, causing EXCLUSIVE lock acquisition to fail.
SET hive.support.concurrency=false;

CREATE TABLE IF NOT EXISTS {{ params.metadata_table }} (
    run_id STRING,
    placement_id BIGINT,
    abtest_id BIGINT,
    owner STRING,
    version BIGINT,
    abtest_instance_id BIGINT,
    start_ts TIMESTAMP,
    end_ts TIMESTAMP,
    record_type STRING,
    observed_ts TIMESTAMP,
    observed_test_option INT,
    observed_traffic_pct DOUBLE,
    alert_type STRING,
    alert_key STRING,
    alert_sent_ts TIMESTAMP,
    container_id BIGINT,
    container_name STRING,
    abtest_name STRING
)
PARTITIONED BY (
    dt STRING COMMENT 'latest, alerts, or observation date (yyyyMMdd)'
)
STORED AS PARQUET
LOCATION 's3://s3-cdp-prod-hive/cmg/xpc_monitor_metadata_v3';

WITH monitored_containers AS (
    {% if params.monitored_containers %}
    {% for container in params.monitored_containers %}
    {% if container.placement_ids %}
    SELECT
        CAST({{ container.container_id }} AS BIGINT) AS container_id,
        placement_id
    FROM (
        SELECT EXPLODE(ARRAY(
            {% for placement_id in container.placement_ids %}
            CAST({{ placement_id }} AS BIGINT){% if not loop.last %},{% endif %}
            {% endfor %}
        )) AS placement_id
    ) placements
    {% else %}
    SELECT
        CAST({{ container.container_id }} AS BIGINT) AS container_id,
        CAST(NULL AS BIGINT) AS placement_id
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    {% else %}
    SELECT
        CAST(NULL AS BIGINT) AS container_id,
        CAST(NULL AS BIGINT) AS placement_id
    WHERE 1 = 0
    {% endif %}
),
source_versions AS (
    SELECT DISTINCT
        m.placement_id,
        CAST(d.abtest_id AS BIGINT) AS abtest_id,
        CAST(m.container_id AS BIGINT) AS container_id,
        CAST(c.name AS STRING) AS container_name,
        CAST(t.name AS STRING) AS abtest_name,
        CAST(t.owner AS STRING) AS owner,
        CAST(d.version AS BIGINT) AS version,
        CAST(d.abtest_instance_id AS BIGINT) AS abtest_instance_id,
        CAST(d.start_date AS STRING) AS start_date,
        CAST(d.start_time AS STRING) AS start_time,
        CAST(d.end_date AS STRING) AS end_date,
        CAST(d.end_time AS STRING) AS end_time,
        CASE
            WHEN d.start_date IS NULL
                OR d.start_time IS NULL
                OR LOWER(TRIM(CAST(d.start_date AS STRING))) IN ('', 'null')
                OR LOWER(TRIM(CAST(d.start_time AS STRING))) IN ('', 'null')
            THEN NULL
            ELSE CONCAT(
                LPAD(TRIM(CAST(d.start_date AS STRING)), 8, '0'),
                LPAD(TRIM(CAST(d.start_time AS STRING)), 6, '0')
            )
        END AS start_timestamp_raw,
        CASE
            WHEN d.end_date IS NULL
                OR d.end_time IS NULL
                OR LOWER(TRIM(CAST(d.end_date AS STRING))) IN ('', 'null')
                OR LOWER(TRIM(CAST(d.end_time AS STRING))) IN ('', 'null')
            THEN NULL
            ELSE CONCAT(
                LPAD(TRIM(CAST(d.end_date AS STRING)), 8, '0'),
                LPAD(TRIM(CAST(d.end_time AS STRING)), 6, '0')
            )
        END AS end_timestamp_raw
    FROM monitored_containers m
    JOIN abtest.abt_buckets b
        ON m.container_id = CAST(b.containerid AS BIGINT)
    JOIN abtest.abt_containers c
        ON m.container_id = CAST(c.containerid AS BIGINT)
    JOIN abtest.c_abtest_def d
        ON d.bucket_id = b.bucketid
    LEFT JOIN abtest.abt_abtest t
        ON d.abtest_id = t.abtestid
),
active_versions AS (
    SELECT
        placement_id,
        abtest_id,
        container_id,
        container_name,
        abtest_name,
        owner,
        version,
        abtest_instance_id,
        start_timestamp_raw,
        end_timestamp_raw,
        ROW_NUMBER() OVER (
            PARTITION BY placement_id, abtest_id
            ORDER BY start_timestamp_raw DESC, version DESC
        ) AS rn
    FROM source_versions
    WHERE start_timestamp_raw IS NOT NULL
        AND start_timestamp_raw <= '{{ params.run_ts_kst }}'
        AND (
            end_timestamp_raw IS NULL
            OR '{{ params.run_ts_kst }}' < end_timestamp_raw
        )
),
current_metadata AS (
    SELECT
        '{{ params.run_id }}' AS run_id,
        placement_id,
        abtest_id,
        owner,
        version,
        abtest_instance_id,
        CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(start_timestamp_raw, 'yyyyMMddHHmmss')) AS TIMESTAMP) AS start_ts,
        CASE
            WHEN end_timestamp_raw IS NULL THEN NULL
            ELSE CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(end_timestamp_raw, 'yyyyMMddHHmmss')) AS TIMESTAMP)
        END AS end_ts,
        'metadata' AS record_type,
        CAST(NULL AS TIMESTAMP) AS observed_ts,
        CAST(NULL AS INT) AS observed_test_option,
        CAST(NULL AS DOUBLE) AS observed_traffic_pct,
        CAST(NULL AS STRING) AS alert_type,
        CAST(NULL AS STRING) AS alert_key,
        CAST(NULL AS TIMESTAMP) AS alert_sent_ts,
        container_id,
        container_name,
        abtest_name
    FROM active_versions
    WHERE rn = 1
),
existing_alerts AS (
    SELECT
        run_id,
        placement_id,
        abtest_id,
        owner,
        version,
        abtest_instance_id,
        start_ts,
        end_ts,
        record_type,
        observed_ts,
        observed_test_option,
        observed_traffic_pct,
        alert_type,
        alert_key,
        alert_sent_ts,
        container_id,
        container_name,
        abtest_name
    FROM {{ params.metadata_table }}
    WHERE dt = 'latest'
        AND record_type = 'alert'
)


INSERT OVERWRITE TABLE {{ params.metadata_table }}
PARTITION (dt='latest')
SELECT * FROM current_metadata
UNION ALL
SELECT * FROM existing_alerts

