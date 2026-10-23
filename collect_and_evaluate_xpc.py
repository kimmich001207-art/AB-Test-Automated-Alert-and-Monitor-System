"""Collect and evaluate XPC traffic + revenue + engagement metrics.

Scans spark_log_streaming (by dt) and ue_hourly (by dt + hr) once each,
then computes per-group metrics and evaluates alert rules in PySpark.

All ads XPCs are nested under a master AB test (e.g. abtest_id=14554,
test_option=2). Only users in that master cohort are counted, and global_users
is the size of that cohort — not ue_hourly distinct pcids.

Metrics per (container_id, abtest_id, test_option):
  - traffic_pct: distinct users across the container's placements / global_users
  - rev_per_user: total_revenue across placements / users_with_ad_event
  - clicks_per_user, views_per_user, impressions_per_user

Alert rule (threshold via CLI):
  - revenue_drop: treatment ARPU dropped > threshold vs control
"""
import argparse
import logging
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, functions as F

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s %(levelname)s %(message)s',
    stream=sys.stderr,
)
LOGGER = logging.getLogger(__name__)

# All ads XPCs are nested under this master AB test cohort.
GLOBAL_ABTEST_ID = 14554
GLOBAL_TEST_OPTION = 2

RESULT_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {result_table} (
    placement_id BIGINT,
    container_id BIGINT,
    placement_ids STRING,
    abtest_id BIGINT,
    version INT,
    test_option INT,
    users BIGINT,
    global_users BIGINT,
    traffic_pct DOUBLE,
    users_with_ad_event BIGINT,
    rev_per_user DOUBLE,
    clicks BIGINT,
    views BIGINT,
    impressions BIGINT,
    clicks_per_user DOUBLE,
    views_per_user DOUBLE,
    impressions_per_user DOUBLE,
    rev_drop DOUBLE,
    should_alert BOOLEAN,
    alert_rules STRING,
    start_ts TIMESTAMP,
    end_ts TIMESTAMP
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
"""

# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--metadata_table', required=True)
    p.add_argument('--result_table', required=True)
    p.add_argument('--run_id', required=True)
    p.add_argument('--run_ts_kst', required=True)
    p.add_argument('--control_option', type=int, default=0)
    p.add_argument('--revenue_drop_threshold', type=float, default=0.05)
    p.add_argument('--low_traffic_threshold', type=float, default=0.01)
    p.add_argument('--very_low_traffic_threshold', type=float, default=0.001)
    p.add_argument('--low_traffic_revenue_drop_threshold', type=float, default=0.25)
    p.add_argument('--min_global_users', type=int, default=1000)
    return p.parse_args()

# -----------------------------------------------------------------------------
# Data collection
# -----------------------------------------------------------------------------

METADATA_S3_BASE = 's3://s3-cdp-prod-hive/cmg/xpc_monitor_metadata_v3'


def read_metadata(spark, table, run_id, run_date):
    """Read active XPC metadata directly from S3.

    Hive INSERT ... UNION ALL writes Parquet files under
    HIVE_UNION_SUBDIR_* subdirectories. Spark's native Parquet
    reader does not recurse into subdirs, so reading via the Hive
    metastore returns 0 rows. We bypass the metastore and read
    from S3 with recursiveFileLookup=true.
    """
    return (
        spark.read
        .option('recursiveFileLookup', 'true')
        .parquet(f'{METADATA_S3_BASE}/dt=latest/')
        .filter("record_type = 'metadata'")
        .select('placement_id', 'abtest_id', 'owner', 'version',
                'abtest_instance_id', 'start_ts', 'end_ts',
                'container_id', 'container_name', 'abtest_name')
    )


def collect_global_user_ids(spark, run_date):
    """Distinct user IDs in the master AB test cohort (14554 test option 2).

    Returns DataFrame(pcid: string).
    """
    prev_date = (datetime.strptime(run_date, '%Y%m%d') - timedelta(days=1)).strftime('%Y%m%d')
    return spark.sql(f"""
        SELECT DISTINCT user_id AS pcid
        FROM abtest.spark_log_streaming
        WHERE dt BETWEEN '{prev_date}' AND '{run_date}'
          AND abtest_id = {GLOBAL_ABTEST_ID}
          AND test_option = {GLOBAL_TEST_OPTION}
          AND user_id IS NOT NULL
    """)


def collect_assigned_users(spark, metadata_df, global_user_ids_df, run_date):
    """Scan spark_log_streaming once for all monitored XPCs.

    Only includes users who are also in the master AB test cohort.
    Uses dt partition only (table has no hr partition).
    Returns DataFrame(container_id, placement_id, abtest_id, test_option, pcid).
    A null placement_id is a wildcard that covers all placements.
    """
    rows = metadata_df.select('abtest_id').distinct().collect()
    if not rows:
        return spark.createDataFrame(
            [], 'container_id:bigint, placement_id:bigint, abtest_id:bigint, '
                'test_option:int, pcid:string')

    abtest_list_str = ','.join(str(r.abtest_id) for r in rows)
    prev_date = (datetime.strptime(run_date, '%Y%m%d') - timedelta(days=1)).strftime('%Y%m%d')
    metadata_df.createOrReplaceTempView('_xpc_meta')
    global_user_ids_df.createOrReplaceTempView('_global_users')

    return spark.sql(f"""
        SELECT /*+ BROADCAST(_xpc_meta) */ DISTINCT
            m.container_id,
            m.placement_id,
            CAST(l.abtest_id AS BIGINT) AS abtest_id,
            CAST(l.test_option AS INT) AS test_option,
            l.user_id AS pcid
        FROM abtest.spark_log_streaming l
        JOIN _xpc_meta m ON l.abtest_id = m.abtest_id
        LEFT SEMI JOIN _global_users g ON l.user_id = g.pcid
        WHERE l.dt BETWEEN '{prev_date}' AND '{run_date}'
          AND l.abtest_id IN ({abtest_list_str})
          AND l.user_id IS NOT NULL
    """)


def collect_revenue_events(spark, metadata_df, global_user_ids_df, run_date, run_hr):
    """Scan ue_hourly once for all monitored placements.

    Uses dt + hr partitions (prunes future hours on run_date) and restricts
    events to the master cohort. If metadata contains a null placement_id,
    all placements are scanned for those users.
    Returns DataFrame(placement_id, pcid, revenue, clicks, views, impressions).
    """
    rows = metadata_df.select('placement_id').distinct().collect()
    if not rows:
        return spark.createDataFrame(
            [],
            'placement_id:bigint, pcid:string, revenue:double, '
            'clicks:bigint, views:bigint, impressions:bigint')

    placement_ids = [r.placement_id for r in rows if r.placement_id is not None]
    all_placements = any(r.placement_id is None for r in rows)
    placement_filter = (
        '' if all_placements
        else f"AND placement_id IN ({','.join(str(p) for p in placement_ids)})"
    )
    global_user_ids_df.createOrReplaceTempView('_global_users')
    prev_date_int = int((datetime.strptime(run_date, '%Y%m%d') - timedelta(days=1)).strftime('%Y%m%d'))
    run_date_int = int(run_date)

    return spark.sql(f"""
        SELECT placement_id, pcid,
               SUM(CAST(ad_spend AS DOUBLE) / 1000000.0) AS revenue,
               SUM(CASE WHEN event_type = 'clicks' THEN 1 ELSE 0 END) AS clicks,
               SUM(CASE WHEN event_type = 'views' THEN 1 ELSE 0 END) AS views,
               SUM(CASE WHEN event_type = 'impressions' THEN 1 ELSE 0 END) AS impressions
        FROM cmg.ue_hourly
        WHERE dt BETWEEN {prev_date_int} AND {run_date_int}
          AND NOT (dt = {run_date_int} AND hr > {run_hr})
          {placement_filter}
          AND pcid IN (SELECT pcid FROM _global_users)
          AND ad_type = 'search'
          AND traffic_source = 0
          AND event_type IN ('clicks', 'views', 'impressions')
          AND pcid IS NOT NULL
        GROUP BY placement_id, pcid
    """)


# -----------------------------------------------------------------------------
# Metric computation
# -----------------------------------------------------------------------------

def compute_metrics(users_df, events_df, global_users_count):
    """Join events and aggregate one distinct-user metric row per AB test.

    Placement IDs remain in the join condition so events are attributed only to
    the placement where the user was assigned. The final aggregation removes
    placement from the grouping key, which merges all placements in a container
    while keeping users distinct across placements.
    """
    events = events_df.withColumnRenamed('placement_id', 'event_placement_id')
    users = users_df.alias('users')
    events = events.alias('events')
    join_condition = (
        (users['pcid'] == events['pcid'])
        & (
            users['placement_id'].isNull()
            | (users['placement_id'] == events['event_placement_id'])
        )
    )
    joined = users.join(events, on=join_condition, how='left').select(
        users['container_id'].alias('container_id'),
        users['placement_id'].alias('placement_id'),
        users['abtest_id'].alias('abtest_id'),
        users['test_option'].alias('test_option'),
        users['pcid'].alias('pcid'),
        events['revenue'].alias('revenue'),
        events['clicks'].alias('clicks'),
        events['views'].alias('views'),
        events['impressions'].alias('impressions'),
    )
    group_metrics = (
        joined
        .groupBy('container_id', 'abtest_id', 'test_option')
        .agg(
            F.countDistinct('pcid').alias('users'),
            F.countDistinct(
                F.when(F.col('revenue').isNotNull(), F.col('pcid'))
            ).alias('users_with_ad_event'),
            F.coalesce(F.sum('revenue'), F.lit(0.0)).alias('total_revenue'),
            F.coalesce(F.sum('clicks'), F.lit(0)).alias('clicks'),
            F.coalesce(F.sum('views'), F.lit(0)).alias('views'),
            F.coalesce(F.sum('impressions'), F.lit(0)).alias('impressions'),
            F.max(F.when(F.col('placement_id').isNull(), 1).otherwise(0))
                .alias('all_placements'),
            F.sort_array(F.collect_set(
                F.when(F.col('placement_id').isNotNull(),
                       F.col('placement_id').cast('string'))
            )).alias('placement_ids_array'),
        )
    )
    return (
        group_metrics
        .withColumn('placement_id', F.lit(None).cast('bigint'))
        .withColumn(
            'placement_ids',
            F.when(F.col('all_placements') == 1, F.lit('all placements'))
             .otherwise(F.concat_ws(', ', F.col('placement_ids_array'))),
        )
        .withColumn('global_users', F.lit(global_users_count))
        .withColumn('traffic_pct',
            F.when(F.col('global_users') > 0,
                   F.col('users').cast('double') / F.col('global_users')))
        .withColumn('rev_per_user',
            F.when(F.col('users_with_ad_event') > 0,
                   F.col('total_revenue') / F.col('users_with_ad_event')))
        .withColumn('clicks_per_user',
            F.when(F.col('users') > 0,
                   F.col('clicks').cast('double') / F.col('users')))
        .withColumn('views_per_user',
            F.when(F.col('users') > 0,
                   F.col('views').cast('double') / F.col('users')))
        .withColumn('impressions_per_user',
            F.when(F.col('users') > 0,
                   F.col('impressions').cast('double') / F.col('users')))
        .drop('total_revenue', 'all_placements', 'placement_ids_array')
    )


# -----------------------------------------------------------------------------
# Rule evaluation
# -----------------------------------------------------------------------------

def evaluate_rules(metrics_df, metadata_df, run_ts, control_option,
                   revenue_drop_thr, low_traffic_threshold,
                   very_low_traffic_threshold, low_traffic_revenue_drop_thr,
                   min_global_users):
    """Compare treatment vs control, compute rev_drop and flag alerts.

    Returns DataFrame matching the result table schema.
    """
    # rev_drop = (control_rpu - treatment_rpu) / control_rpu
    control_rpu = (
        metrics_df
        .filter(F.col('test_option') == control_option)
        .select('container_id', 'abtest_id',
                F.col('rev_per_user').alias('control_rpu'))
    )
    with_drop = (
        metrics_df
        .join(control_rpu, on=['container_id', 'abtest_id'], how='left')
        .withColumn('rev_drop',
            F.when(
                (F.col('test_option') != control_option)
                & F.col('control_rpu').isNotNull()
                & (F.col('control_rpu') > 0)
                & F.col('rev_per_user').isNotNull(),
                (F.col('control_rpu') - F.col('rev_per_user')) / F.col('control_rpu')
            ))
        .drop('control_rpu')
    )

    # Enrich with metadata: version, observation window
    run_ts_str = run_ts.strftime('%Y-%m-%d %H:%M:%S')
    meta = (
        metadata_df
        .withColumn('xpc_start_ts', F.col('start_ts'))
        .withColumn('start_ts',
            F.greatest(F.col('xpc_start_ts'),
                       F.expr(f"TIMESTAMP '{run_ts_str}' - INTERVAL 24 HOURS")))
        .withColumn('end_ts', F.lit(run_ts_str).cast('timestamp'))
        .drop('xpc_start_ts')
        # The original screenshot obscures lines 332-334; verify this selection.
        .select('container_id', 'abtest_id', 'version', 'start_ts', 'end_ts')
        .dropDuplicates()
    )
    evaluated = with_drop.join(meta, on=['container_id', 'abtest_id'], how='left')

    # Flag revenue_drop alert using treatment traffic bands. Very small
    # treatments are suppressed to avoid noisy, low-sample alerts.
    is_treat = F.col('test_option') != control_option
    has_users = F.col('global_users') >= min_global_users
    has_sufficient_traffic = F.col('traffic_pct') >= very_low_traffic_threshold
    effective_revenue_drop_thr = F.when(
        F.col('traffic_pct') < low_traffic_threshold,
        F.lit(low_traffic_revenue_drop_thr),
    ).otherwise(F.lit(revenue_drop_thr))
    rd = (
        is_treat
        & has_users
        & has_sufficient_traffic
        & F.col('rev_drop').isNotNull()
        & (F.col('rev_drop') > effective_revenue_drop_thr)
    )

    return (
        evaluated
        .withColumn('should_alert', rd)
        .withColumn('alert_rules', F.when(rd, F.lit('revenue_drop')))
        .select(
            'placement_id', 'container_id', 'placement_ids',
            'abtest_id', 'version', 'test_option',
            'users', 'global_users', 'traffic_pct', 'users_with_ad_event',
            'rev_per_user', 'clicks', 'views', 'impressions',
            'clicks_per_user', 'views_per_user', 'impressions_per_user',
            'rev_drop', 'should_alert', 'alert_rules',
            'start_ts', 'end_ts')
    )


def write_metadata_observations(spark, table, metadata_df, results, run_id, run_date, run_ts):
    columns = [
        'run_id', 'placement_id', 'abtest_id', 'owner', 'version',
        'abtest_instance_id', 'start_ts', 'end_ts', 'record_type',
        'observed_ts', 'observed_test_option', 'observed_traffic_pct',
        'alert_type', 'alert_key', 'alert_sent_ts',
        'container_id', 'container_name', 'abtest_name',
    ]
    version_metadata = metadata_df.select(
        'abtest_id',
        'owner',
        'version',
        'abtest_instance_id',
        F.col('start_ts').alias('version_start_ts'),
        F.col('end_ts').alias('version_end_ts'),
        'container_id',
        'container_name',
        'abtest_name',
    ).dropDuplicates()
    observations = results.join(
        version_metadata,
        on=['container_id', 'abtest_id', 'version'],
        how='left',
    ).select(
        F.lit(run_id).alias('run_id'),
        'placement_id',
        'abtest_id',
        'owner',
        'version',
        'abtest_instance_id',
        F.col('version_start_ts').alias('start_ts'),
        F.col('version_end_ts').alias('end_ts'),
        F.lit('observation').alias('record_type'),
        F.lit(run_ts.strftime('%Y-%m-%d %H:%M:%S')).cast('timestamp').alias('observed_ts'),
        F.col('test_option').alias('observed_test_option'),
        F.col('traffic_pct').alias('observed_traffic_pct'),
        F.lit(None).cast('string').alias('alert_type'),
        F.lit(None).cast('string').alias('alert_key'),
        F.lit(None).cast('timestamp').alias('alert_sent_ts'),
        'container_id',
        'container_name',
        'abtest_name',
    ).select(columns)

    existing = spark.sql(
        f"SELECT {', '.join(columns)} FROM {table} "
        f"WHERE dt = '{run_date}' AND record_type IS NOT NULL "
        f"AND NOT (run_id = '{run_id}' AND record_type = 'observation')"
    )
    metadata_history = existing.unionByName(observations).localCheckpoint()
    metadata_history.createOrReplaceTempView('_metadata_history')
    spark.sql(
        f"INSERT OVERWRITE TABLE {table} PARTITION (dt = '{run_date}') "
        f"SELECT {', '.join(columns)} FROM _metadata_history"
    )


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    args = parse_args()
    run_ts = datetime.strptime(args.run_ts_kst, '%Y%m%d%H%M%S')
    run_date = run_ts.strftime('%Y%m%d')

    spark = (
        SparkSession.builder
        .appName('xpc-monitor-v3')
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.conf.set('spark.sql.session.timeZone', 'Asia/Seoul')

    try:
        spark.sql(RESULT_TABLE_DDL.format(result_table=args.result_table))

        metadata = read_metadata(spark, args.metadata_table, args.run_id, run_date)
        metadata.cache()
        metadata_count = metadata.count()
        print(f'[XPC-DIAG] metadata rows={metadata_count}, run_id={args.run_id}',
              flush=True, file=sys.stderr)
        if metadata_count > 0:
            metadata.show(truncate=False)

        global_user_ids = collect_global_user_ids(spark, run_date)
        global_user_ids.cache()
        global_users_count = global_user_ids.count()
        print(f'[XPC-DIAG] global_users={global_users_count} '
              f'(abtest={GLOBAL_ABTEST_ID}, test_option={GLOBAL_TEST_OPTION})',
              flush=True, file=sys.stderr)

        users = collect_assigned_users(spark, metadata, global_user_ids, run_date)
        users.cache()
        users_count = users.count()
        print(f'[XPC-DIAG] assigned_users={users_count}', flush=True, file=sys.stderr)

        events = collect_revenue_events(
            spark, metadata, global_user_ids, run_date, run_ts.hour
        )
        events.cache()
        events_count = events.count()
        print(f'[XPC-DIAG] revenue_events={events_count}', flush=True, file=sys.stderr)

        metrics = compute_metrics(users, events, global_users_count)
        results = evaluate_rules(
            metrics, metadata, run_ts, args.control_option,
            args.revenue_drop_threshold, args.low_traffic_threshold,
            args.very_low_traffic_threshold,
            args.low_traffic_revenue_drop_threshold,
            args.min_global_users)

        results.cache()
        results_count = results.count()
        print(f'[XPC-DIAG] results={results_count}', flush=True, file=sys.stderr)
        if results_count > 0:
            results.show(truncate=False)

        results.createOrReplaceTempView('_results')
        spark.sql(
            f"INSERT OVERWRITE TABLE {args.result_table} PARTITION (dt = '{run_date}') "
            "SELECT placement_id, container_id, placement_ids, abtest_id, version, "
            "test_option, users, global_users, "
            "traffic_pct, users_with_ad_event, rev_per_user, "
            "clicks, views, impressions, "
            "clicks_per_user, views_per_user, impressions_per_user, "
            "rev_drop, should_alert, alert_rules, start_ts, end_ts FROM _results"
        )
        write_metadata_observations(
            spark, args.metadata_table, metadata, results,
            args.run_id, run_date, run_ts
        )
        print(f'[XPC-DIAG] Wrote results to {args.result_table} (dt={run_date})',
              flush=True, file=sys.stderr)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
