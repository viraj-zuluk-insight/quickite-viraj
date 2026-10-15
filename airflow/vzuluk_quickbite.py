from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.sensors.python import PythonSensor

FILES = ["restaurants", "customers", "orders", "order_items"]
name = "vzuluk"
PREFIX = f"capstone/{name}"

TOKEN_URL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
DRIVE_API = "https://www.googleapis.com/drive/v3/files"

PROJECT = Variable.get(f"{name}_gcp_project")
RAW_DS = Variable.get(f"{name}_bq_dataset_raw")
DATAFORM_REPO = Variable.get(f"{name}_dataform_repo")
REGION = Variable.get(f"{name}_gcp_region")

COLUMNS = {
    "restaurants": [
        "restaurant_id",
        "name",
        "city",
        "cuisine",
        "is_active",
        "onboarded_date",
    ],
    "customers": [
        "customer_id",
        "name",
        "email",
        "phone",
        "city",
        "signup_date",
        "updated_at",
    ],
    "orders": [
        "order_id",
        "customer_id",
        "restaurant_id",
        "order_ts",
        "status",
        "payment_method",
        "updated_at",
    ],
    "order_items": [
        "order_id",
        "line_no",
        "item_name",
        "quantity",
        "unit_price",
    ],
}


def _drive_headers():
  import requests

  r = requests.get(
      TOKEN_URL,
      params={"scopes": "https://www.googleapis.com/auth/drive.readonly"},
      headers={"Metadata-Flavor": "Google"},
      timeout=10,
  )
  r.raise_for_status()
  return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _find_file_id(folder_id, file_name):
  import requests

  folder_id = folder_id.strip()
  r = requests.get(
      DRIVE_API,
      params={
          "q": (
              f"name = '{file_name}' and '{folder_id}' in parents and trashed ="
              " false"
          ),
          "fields": "files(id,name)",
          "orderBy": "modifiedTime desc",
          "supportsAllDrives": "true",
          "includeItemsFromAllDrives": "true",
          "corpora": "allDrives",
      },
      headers=_drive_headers(),
      timeout=30,
  )
  r.raise_for_status()
  files = r.json().get("files", [])
  return files[0]["id"] if files else None


def file_exists(folder_id, file_name):
  return _find_file_id(folder_id, file_name) is not None


def drive_to_gcs(folder_id, file_name, bucket_name, object_name):
  import requests
  from airflow.providers.google.cloud.hooks.gcs import GCSHook

  file_id = _find_file_id(folder_id, file_name)
  if file_id is None:
    raise FileNotFoundError(
        f"File '{file_name}' not found in Drive folder '{folder_id}'"
    )

  r = requests.get(
      f"{DRIVE_API}/{file_id}",
      params={"alt": "media", "supportsAllDrives": "true"},
      headers=_drive_headers(),
      timeout=120,
  )
  r.raise_for_status()

  GCSHook().upload(
      bucket_name=bucket_name,
      object_name=object_name,
      data=r.content,
      mime_type="text/csv",
  )


def raw_sql(table):
  raw = f"`{PROJECT}.{RAW_DS}.{table}`"
  tmp = f"`{PROJECT}.{RAW_DS}.tmp_{table}`"
  return f"""
CREATE TABLE IF NOT EXISTS {raw}
PARTITION BY _source_date AS
SELECT *, CAST(NULL AS DATE) AS _source_date, CAST(NULL AS STRING) AS _source_file, CAST(NULL AS TIMESTAMP) AS _ingested_at
FROM {tmp} WHERE FALSE;

BEGIN TRANSACTION;
DELETE FROM {raw} WHERE _source_date = DATE('{{{{ ds }}}}');
INSERT INTO {raw}
SELECT *, DATE('{{{{ ds }}}}'), '{table}_{{{{ ds }}}}.csv', CURRENT_TIMESTAMP() FROM {tmp};
COMMIT TRANSACTION;
"""


with DAG(
    dag_id=f"{name}_quickbite",
    start_date=datetime(2026, 10, 10),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 0, "retry_delay": timedelta(minutes=5)},
    tags=[f"{name}", "quickbite"],
) as dag:

  files_landed = EmptyOperator(task_id="files_landed")

  for table_name in FILES:
    drive_kwargs = {
        "folder_id": "{{ var.value." + name + "_drive_folder_id }}",
        "file_name": f"{table_name}_{{{{ ds }}}}.csv",
    }

    wait = PythonSensor(
        task_id=f"wait_for_{table_name}",
        python_callable=file_exists,
        op_kwargs=drive_kwargs,
        mode="reschedule",
        poke_interval=120,
        timeout=6 * 60 * 60,
    )

    copy = PythonOperator(
        task_id=f"drive_to_gcs_{table_name}",
        python_callable=drive_to_gcs,
        op_kwargs={
            **drive_kwargs,
            "bucket_name": "{{ var.value." + name + "_gcs_bucket }}",
            "object_name": f"{PREFIX}/landing/{{{{ ds }}}}/{table_name}.csv",
        },
    )

    wait >> copy >> files_landed

  raw_loaded = EmptyOperator(task_id="raw_loaded")

  for table, cols in COLUMNS.items():
    load_tmp = GCSToBigQueryOperator(
        task_id=f"load_{table}_tmp",
        bucket="{{ var.value." + name + "_gcs_bucket }}",
        source_objects=[f"{PREFIX}/landing/{{{{ ds }}}}/{table}.csv"],
        destination_project_dataset_table=f"{PROJECT}.{RAW_DS}.tmp_{table}",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=False,
        schema_fields=[{"name": c, "type": "STRING"} for c in cols],
        write_disposition="WRITE_TRUNCATE",
    )

    load_raw = BigQueryInsertJobOperator(
        task_id=f"load_{table}_raw",
        configuration={
            "query": {"query": raw_sql(table), "useLegacySql": False}
        },
        project_id=PROJECT,
    )

    files_landed >> load_tmp >> load_raw >> raw_loaded

  create_compilation = DataformCreateCompilationResultOperator(
      task_id="create_compilation_result",
      project_id=PROJECT,
      region=REGION,
      repository_id=DATAFORM_REPO,
      compilation_result={
          "workspace": (
              f"projects/{PROJECT}/locations/{REGION}"
              f"/repositories/{DATAFORM_REPO}/workspaces/{name}-dev"
          ),
      },
  )

  execute_dataform = DataformCreateWorkflowInvocationOperator(
      task_id="execute_dataform_workflow",
      project_id=PROJECT,
      region=REGION,
      repository_id=DATAFORM_REPO,
      workflow_invocation={
          "compilation_result": (
              "{{ task_instance.xcom_pull(task_ids='create_compilation_result')['name'] }}"
          ),
          "invocation_config": {
              "service_account": "adhith-dataform-sa@sada-seed-2025-sandbox.iam.gserviceaccount.com"
          },
      },
  )

  raw_loaded >> create_compilation >> execute_dataform