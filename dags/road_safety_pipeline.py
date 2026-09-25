from airflow.sdk import dag, task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime


@dag(
    dag_id="road_safety_pipeline",
    schedule=None,
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["road-safety", "data-engineering"],
)
def road_safety_pipeline():

    # Real Databricks Bronze task
    bronze = DatabricksRunNowOperator(
        task_id="build_bronze",
        databricks_conn_id="databricks_default",
        job_id=793280363847136,
    )

    silver = DatabricksRunNowOperator(
        task_id="build_silver",
        databricks_conn_id="databricks_default",
        job_id=687788175977179,
    )

    gold = DatabricksRunNowOperator(
        task_id="build_gold",
        databricks_conn_id="databricks_default",
        job_id=614723887505050,
    )

    dq = DatabricksRunNowOperator(
        task_id="data_quality_check",
        databricks_conn_id="databricks_default",
        job_id=846410899293770,
    )

    bronze >> silver >> gold >> dq


road_safety_pipeline()