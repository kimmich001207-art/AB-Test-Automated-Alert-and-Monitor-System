import logging
from datetime import datetime
from typing import Any, Mapping

from ads_quality.app.xpc_revenue_monitor import config

LOGGER = logging.getLogger(__name__)
from ads_quality.app.xpc_revenue_monitor.ramp_policy import as_naive_datetime

# Dedicated partition for alert dedup records.  Neither resolve_xpc_metadata
# (which overwrites dt='latest') nor write_metadata_observations (which
# overwrites dt=YYYYMMDD) will ever touch this partition, so Presto-written
# alert records are safe from cross-engine INSERT OVERWRITE visibility issues.
ALERTS_PARTITION = 'alerts'


def _qualified_table(table: str) -> str:
    return f'hive.{table}'


def run_query(sql: str) -> list[dict[str, Any]]:
    from lib.coupang.hooks.presto_hook import PrestoHook

    conn = PrestoHook(presto_conn_id=config.PRESTO_CONN_ID).get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _run_write_query(sql: str) -> None:
    from lib.coupang.hooks.presto_hook import PrestoHook

    conn = PrestoHook(presto_conn_id=config.PRESTO_CONN_ID).get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    # Fetch the result to ensure the INSERT completes before the cursor
    # is closed.  Without this, the cursor may be garbage-collected while
    # the query is still running asynchronously.
    try:
        cursor.fetchone()
    except Exception:
        LOGGER.debug('fetchone after INSERT raised (expected for DML)', exc_info=True)
    if hasattr(conn, 'commit'):
        conn.commit()


def _sql_string(value: Any) -> str:
    return "'{}'".format(str(value).replace("'", "''"))


def fetch_current_versions(metadata_table: str) -> list[dict[str, Any]]:
    return run_query(
        f"""
        SELECT placement_id, abtest_id, owner, version, abtest_instance_id,
               start_ts, end_ts, container_id, container_name, abtest_name
        FROM {_qualified_table(metadata_table)}
        WHERE dt = 'latest'
            AND record_type = 'metadata'
        """
    )


def fetch_observations(
    metadata_table: str,
    history_start_date: str,
) -> list[dict[str, Any]]:
    return run_query(
        f"""
        SELECT placement_id, abtest_id, version, observed_ts,
               observed_test_option, observed_traffic_pct, start_ts
        FROM {_qualified_table(metadata_table)}
        WHERE dt >= {_sql_string(history_start_date)}
            AND dt <> 'latest'
            AND record_type = 'observation'
        """
    )


def fetch_new_version_alert_keys(metadata_table: str) -> set[str]:
    rows = run_query(
        f"""
        SELECT alert_key
        FROM {_qualified_table(metadata_table)}
        WHERE dt = 'alerts'
            AND record_type = 'alert'
            AND alert_type = 'new_version'
            AND alert_key IS NOT NULL
        """
    )
    return {str(row['alert_key']) for row in rows}


def _placement_key(placement_id: Any) -> str:
    return 'all' if placement_id is None else str(int(placement_id))


def version_key(row: Mapping[str, Any]) -> tuple[str, int, int, int]:
    return (
        _placement_key(row.get('placement_id')),
        int(row['abtest_id']),
        int(row['version']),
        int(row['abtest_instance_id']),
    )


def alert_key(row: Mapping[str, Any]) -> str:
    return 'new_version:{}:{}:{}:{}'.format(*version_key(row))


def record_alert(
    metadata_table: str,
    alert_type: str,
    key: str,
    run_id: str,
    sent_at: datetime,
    placement_id: int | None,
    abtest_id: int,
    version: int,
    owner: str | None = None,
    abtest_instance_id: int = 0,
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
    container_id: int | None = None,
    container_name: str | None = None,
    abtest_name: str | None = None,
) -> None:
    """Write an alert/report dedup record to the dedicated alerts partition.

    Uses dt='alerts' — a partition that is never overwritten by any other
    engine (Hive overwrites dt='latest', Spark overwrites dt=YYYYMMDD),
    so Presto-written files are safe from cross-engine visibility issues.
    """
    start_sql = 'NULL' if start_ts is None else f"TIMESTAMP '{start_ts:%Y-%m-%d %H:%M:%S}'"
    end_sql = 'NULL' if end_ts is None else f"TIMESTAMP '{end_ts:%Y-%m-%d %H:%M:%S}'"
    values = [
        _sql_string(run_id),
        'NULL' if placement_id is None else str(placement_id),
        str(abtest_id),
        'NULL' if owner is None else _sql_string(owner),
        str(version),
        str(abtest_instance_id),
        start_sql,
        end_sql,
        "'alert'",
        'NULL',
        'NULL',
        'NULL',
        _sql_string(alert_type),
        _sql_string(key),
        f"TIMESTAMP '{sent_at:%Y-%m-%d %H:%M:%S}'",
        'NULL' if container_id is None else str(container_id),
        'NULL' if container_name is None else _sql_string(container_name),
        'NULL' if abtest_name is None else _sql_string(abtest_name),
        _sql_string(ALERTS_PARTITION),
    ]
    columns = (
        'run_id, placement_id, abtest_id, owner, version, abtest_instance_id, '
        'start_ts, end_ts, record_type, observed_ts, observed_test_option, '
        'observed_traffic_pct, alert_type, alert_key, alert_sent_ts, '
        'container_id, container_name, abtest_name, dt'
    )
    _run_write_query(
        f"INSERT INTO {_qualified_table(metadata_table)} ({columns}) "
        f"VALUES ({', '.join(values)})"
    )


def fetch_all_results(result_table: str, run_date: str) -> list[dict[str, Any]]:
    """Fetch all metric rows (all options including control) for a run date."""
    return run_query(
        f"""
        SELECT
            placement_id, container_id, placement_ids, abtest_id, version,
            test_option, users, global_users, traffic_pct,
            users_with_ad_event, rev_per_user,
            clicks, views, impressions,
            clicks_per_user, views_per_user, impressions_per_user,
            rev_drop, should_alert, alert_rules,
            start_ts, end_ts
        FROM {_qualified_table(result_table)}
        WHERE dt = '{run_date}'
        """
    )


def report_key(
    placement_id: int | None, abtest_id: int, version: int, run_ts_kst: str,
) -> str:
    """Build a deduplication key for a scheduled business metric report."""
    return f'report:{_placement_key(placement_id)}:{abtest_id}:{version}:{run_ts_kst}'


def container_report_key(container_id: int, run_ts_kst: str) -> str:
    """Build a deduplication key for a container-level metric report."""
    return f'report:container:{container_id}:{run_ts_kst}'


def fetch_sent_report_keys(metadata_table: str) -> set[str]:
    """Fetch all previously sent report keys."""
    rows = run_query(
        f"""
        SELECT alert_key
        FROM {_qualified_table(metadata_table)}
        WHERE dt = 'alerts'
            AND record_type = 'alert'
            AND alert_type = 'report'
            AND alert_key IS NOT NULL
        """
    )
    return {str(row['alert_key']) for row in rows}
