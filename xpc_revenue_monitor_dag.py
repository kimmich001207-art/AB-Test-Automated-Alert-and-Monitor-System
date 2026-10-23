from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from datainfra.airflow.operators.hive_operator import HiveOperator
from datainfra.airflow.operators.spark_operator import SparkOperator

from ads_quality.utils.constants import (
    TEAM_AQ_CONN_ID,
    HIVE_6_15_CONN_ID,
    EMR_6_15_CONN_ID,
)
from ads_quality.app.xpc_revenue_monitor import config
from ads_quality.app.xpc_revenue_monitor.alerts import send_alerts


DAG_ID = 'cmg_ads_quality_xpc_revenue_monitor_v3'
PYSPARK_SCRIPT = (
    's3://s3-cdp-prod-hive/cmg/ads_quality/temp/chhuang21/collect_and_evaluate_xpc.py'
)
METADATA_TABLE = 'cmg.xpc_monitor_metadata_v3'
RESULT_TABLE = 'cmg.xpc_monitor_results_v3'
RUN_TS_KST = "{{ (execution_date + macros.timedelta(hours=9)).strftime('%Y%m%d%H%M%S') }}"
RUN_DATE_KST = "{{ (execution_date + macros.timedelta(hours=9)).strftime('%Y%m%d') }}"

default_args = {
    'owner': 'chhuang21',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=20),
    'start_date': datetime(2026, 8, 1),
    'email': ['chhuang21@coupang.com'],
    'email_on_failure': False,
    'execution_timeout': timedelta(hours=3),
}

SPARK_OPTIONS = [
    ('num-executors', '40'),
    ('driver-cores', '4'),
    ('driver-memory', '16g'),
    ('executor-cores', '5'),
    ('executor-memory', '32g'),
    ('conf', '"spark.executor.memoryOverhead=4g"'),
    ('conf', '"spark.sql.adaptive.enabled=true"'),
    ('conf', '"spark.sql.adaptive.skewJoin.enabled=true"'),
    ('conf', '"spark.sql.shuffle.partitions=800"'),
    ('conf', '"spark.shuffle.compress=true"'),
    ('conf', '"spark.sql.autoBroadcastJoinThreshold=64m"'),
]

SPARK_ARGS = (
    f'{PYSPARK_SCRIPT} '
    f'--metadata_table={METADATA_TABLE} '
    f'--result_table={RESULT_TABLE} '
    f'--run_ts_kst={RUN_TS_KST} '
    '--run_id={{ run_id }} '
    f'--control_option={config.CONTROL_OPTION} '
    f'--revenue_drop_threshold={config.REVENUE_DROP_THRESHOLD} '
    f'--low_traffic_threshold={config.LOW_TRAFFIC_THRESHOLD} '
    f'--very_low_traffic_threshold={config.VERY_LOW_TRAFFIC_THRESHOLD} '
    f'--low_traffic_revenue_drop_threshold='
    f'{config.LOW_TRAFFIC_REVENUE_DROP_THRESHOLD} '
    f'--min_global_users={config.MIN_GLOBAL_USERS}'
)

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval='0 * * * *',
    catchup=False,
    concurrency=5,
    max_active_runs=1,
    tags=['ads_quality', 'xpc', 'revenue_monitor', 'spark'],
)

resolve_xpc_metadata = HiveOperator(
    task_id='resolve_xpc_metadata',
    hive_cli_conn_id=HIVE_6_15_CONN_ID,
    team_conn_id=TEAM_AQ_CONN_ID,
    hiveconf_jinja_translate=True,
    hql='resolve_xpc_metadata.hql',
    params={
        'monitored_containers': config.MONITORED_CONTAINERS,
        'metadata_table': METADATA_TABLE,
        'run_id': '{{ run_id }}',
        'run_ts_kst': RUN_TS_KST,
        'run_date_kst': RUN_DATE_KST,
    },
    dag=dag,
)

collect_and_evaluate_xpc = SparkOperator(
    task_id='collect_and_evaluate_xpc',
    team_conn_id=TEAM_AQ_CONN_ID,
    emr_conn_id=EMR_6_15_CONN_ID,
    spark_args=SPARK_ARGS,
    spark_options=SPARK_OPTIONS,
    dag=dag,
)

send_xpc_alerts = PythonOperator(
    task_id='send_xpc_alerts',
    python_callable=send_alerts,
    op_kwargs={
        'result_table': RESULT_TABLE,
        'metadata_table': METADATA_TABLE,
        'run_ts_kst': RUN_TS_KST,
        'run_id': '{{ run_id }}',
    },
    dag=dag,
)

resolve_xpc_metadata >> collect_and_evaluate_xpc >> send_xpc_alerts

globals()[DAG_ID] = dag
