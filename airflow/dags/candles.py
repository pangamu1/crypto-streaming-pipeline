from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "trades_pg"          # auto-created from AIRFLOW_CONN_TRADES_PG
SQL_DIR = Path("/opt/airflow/sql")


def _run_sql_file(name: str, **fmt) -> None:
    """Read a .sql file, optionally .format() it, and execute it."""
    sql = (SQL_DIR / name).read_text()
    if fmt:
        sql = sql.format(**fmt)
    PostgresHook(postgres_conn_id=CONN_ID).run(sql)


@dag(
    dag_id="crypto_candles",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["crypto", "candles", "cp4"],
)
def crypto_candles():

    @task
    def create_tables():
        _run_sql_file("candles.sql")

    @task
    def build(table: str, grain: str):
        _run_sql_file("aggregate_candles.sql", table=table, grain=grain)

    @task
    def dq_gate():
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        bad_rows = hook.get_first((SQL_DIR / "dq_candles.sql").read_text())[0]
        if bad_rows:
            raise AirflowException(f"DQ gate failed: {bad_rows} invalid candle rows")

    created = create_tables()
    built = [build.override(task_id="build_1m")("candles_1m", "minute"),
             build.override(task_id="build_1h")("candles_1h", "hour")]
    created >> built >> dq_gate()


crypto_candles()