from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.decorators import task
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "src"))

from trafikk_pipeline import config
from trafikk_pipeline.traffic_points import fetch_and_store_points
from trafikk_pipeline.traffic_volumes import fetch_and_store_volumes
from trafikk_pipeline.rain import fetch_and_store_rain

default_args = {"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="ingest_trafikkdata",
    start_date=datetime(2025, 9, 20),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["bergen"],
) as dag:

    @task
    def fetch_registration_points_task() -> str:
        return fetch_and_store_points()

    @task
    def fetch_traffic_volumes_task(points_dir: str) -> str | None:
        return fetch_and_store_volumes(points_dir)

    @task
    def fetch_rain_task(points_dir: str, points_json: str | None = None):
        return fetch_and_store_rain(points_dir, points_json)

    pts_dir = fetch_registration_points_task()
    vols_json = fetch_traffic_volumes_task(pts_dir)
    rain = fetch_rain_task(pts_dir, vols_json)

    run_bronze_to_silver = DatabricksSubmitRunOperator(
        task_id="run_bronze_to_silver",
        databricks_conn_id="databricks_default",
        new_cluster={
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "Standard_D2ads_v6",
            "num_workers": 1,
            "spark_conf": {
                "spark.databricks.cluster.profile": "singleNode",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.shuffle.partitions": "auto",
            },
            "custom_tags": {"ResourceClass": "SingleNode"},
            "azure_attributes": {"availability": "ON_DEMAND_AZURE"},
            "spark_env_vars": {
                "STORAGE_ACCOUNT": os.environ["AZURE_STORAGE_ACCOUNT"],
                "CONTAINER_NAME": os.environ["AZURE_CONTAINER"],
                "ACCOUNT_KEY": os.environ["AZURE_ACCOUNT_KEY"],
            },
        },
        notebook_task={"notebook_path": "/Users/ms@puriotech.org/bronze_to_gold"},
        timeout_seconds=60 * 45,
    )

    [vols_json, rain] >> run_bronze_to_silver
