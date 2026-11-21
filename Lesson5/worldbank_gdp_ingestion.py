from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import storage

from worldbank_extract import COUNTRIES, extract_to_csv

BUCKET = "project-aa4aeaed-2897-4b21-a77-worlbank-raw"   # <-- à modifier
LOCATION = "europe-west9"  # région du bucket et du dataset

SCHEMA = [
    {"name": "country_code", "type": "STRING", "mode": "NULLABLE"},
    {"name": "country_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "indicator_code", "type": "STRING", "mode": "NULLABLE"},
    {"name": "indicator_label", "type": "STRING", "mode": "NULLABLE"},
    {"name": "year", "type": "INTEGER", "mode": "NULLABLE"},   # INTEGER = INT64
    {"name": "value", "type": "FLOAT", "mode": "NULLABLE"},    # FLOAT = FLOAT64
    {"name": "load_date", "type": "DATE", "mode": "NULLABLE"},
]


def extract_and_upload_to_gcs(ds, **context):
    """ds = date d'exécution Airflow (YYYY-MM-DD), injectée automatiquement."""
    local_path = f"/tmp/indicators_{ds}.csv"
    n_rows = extract_to_csv(local_path, load_date=ds)
    blob = storage.Client().bucket(BUCKET).blob(f"worldbank/{ds}/indicators.csv")
    blob.upload_from_filename(local_path, content_type="text/csv")
    print(f"{n_rows} lignes envoyées vers gs://{BUCKET}/worldbank/{ds}/indicators.csv")


with DAG(
    dag_id="worldbank_gdp_ingestion",
    schedule="@weekly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["bootcamp", "worldbank"],
) as dag:

    extract_and_upload = PythonOperator(
        task_id="extract_and_upload",
        python_callable=extract_and_upload_to_gcs,
    )

    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=BUCKET,
        source_objects=["worldbank/{{ ds }}/indicators.csv"],  # Jinja : ici, pas de f-string
        destination_project_dataset_table="worlbank_raw.indicators",
        schema_fields=SCHEMA,
        write_disposition="WRITE_TRUNCATE",
        source_format="CSV",
        skip_leading_rows=1,
        location=LOCATION,
        # Bonus 14 : partitionnement par load_date + clustering par country_code
        time_partitioning={"type": "DAY", "field": "load_date"},
        cluster_fields=["country_code"],
    )

    # Bonus 11 : contrôle qualité (échoue si un des pays n'a aucune ligne)
    check_rows_per_country = BigQueryCheckOperator(
        task_id="check_rows_per_country",
        sql=f"SELECT COUNT(DISTINCT country_code) = {len(COUNTRIES)} "
            "FROM `worlbank_raw.indicators`",
        use_legacy_sql=False,
        location=LOCATION,
    )

    extract_and_upload >> load_to_bigquery >> check_rows_per_country