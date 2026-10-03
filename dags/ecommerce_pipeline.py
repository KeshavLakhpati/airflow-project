"""All data lives in Snowflake. Airflow only orchestrates SQL that is stored in git.

init tables -> seed dimensions -> generate that day's orders -> staging -> marts -> quality checks
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

try:  # Airflow 3 (Astro Runtime 3.x)
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

CONN_ID = "snowflake_default"
SQL_DIR = Path(__file__).resolve().parents[1] / "include" / "sql"


def run_sql(file_name: str, **replacements: str) -> None:
    sql = (SQL_DIR / file_name).read_text()
    for key, value in replacements.items():
        sql = sql.replace("${" + key + "}", value)
    SnowflakeHook(snowflake_conn_id=CONN_ID).run(sql)


@dag(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2026, 9, 20),
    schedule="@daily",
    catchup=True,          # backfills one run per day since start_date
    max_active_runs=1,     # runs one day at a time (order ids are sequential)
    default_args={"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["learning", "snowflake"],
)
def ecommerce_pipeline():
    @task
    def init_tables():
        run_sql("00_create_tables.sql")

    @task
    def seed_dimensions():
        run_sql("01_seed_dimensions.sql")

    @task
    def generate_orders(ds=None):
        run_sql("02_generate_daily_orders.sql", RUN_DATE=ds)

    @task
    def build_staging():
        run_sql("03_staging.sql")

    @task
    def build_marts():
        run_sql("04_marts.sql")

    @task
    def quality_checks():
        hook = SnowflakeHook(snowflake_conn_id=CONN_ID)
        errors = []
        for t in ["STAGING.STG_CUSTOMERS", "STAGING.STG_PRODUCTS", "STAGING.STG_ORDERS",
                  "STAGING.STG_ORDER_ITEMS", "MARTS.DAILY_SALES", "MARTS.CUSTOMER_SUMMARY",
                  "MARTS.PRODUCT_PERFORMANCE"]:
            if hook.get_first(f"SELECT COUNT(*) FROM {t}")[0] == 0:
                errors.append(f"{t} is empty")
        if hook.get_first("SELECT COUNT(*) FROM STAGING.STG_ORDERS WHERE order_id IS NULL")[0]:
            errors.append("NULL order_id in STG_ORDERS")
        if hook.get_first("SELECT COUNT(*) FROM (SELECT order_id FROM STAGING.STG_ORDERS GROUP BY 1 HAVING COUNT(*) > 1)")[0]:
            errors.append("Duplicate order_id in STG_ORDERS")
        if hook.get_first("SELECT COUNT(*) FROM STAGING.STG_ORDER_ITEMS i LEFT JOIN STAGING.STG_ORDERS o ON o.order_id = i.order_id WHERE o.order_id IS NULL")[0]:
            errors.append("Orphan order items")
        mart = hook.get_first("SELECT COALESCE(SUM(revenue), 0) FROM MARTS.DAILY_SALES")[0]
        src = hook.get_first("""SELECT COALESCE(SUM(i.line_total), 0) FROM STAGING.STG_ORDER_ITEMS i
                                JOIN STAGING.STG_ORDERS o ON o.order_id = i.order_id
                                WHERE o.status <> 'CANCELLED'""")[0]
        if abs(float(mart) - float(src)) > 0.01:
            errors.append(f"Revenue mismatch: mart={mart} source={src}")
        if errors:
            raise ValueError("Data quality failed: " + "; ".join(errors))

    (init_tables() >> seed_dimensions() >> generate_orders()
     >> build_staging() >> build_marts() >> quality_checks())


ecommerce_pipeline()
