from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from pyspark.sql import SparkSession, functions as F

RAW_DIR = "/data/raw"


def any_csv_exists() -> bool:
    try:
        return any(name.lower().endswith(".csv") for name in os.listdir(RAW_DIR))
    except FileNotFoundError:
        return False


def log_latest_csv() -> None:
    csv_files = [
        os.path.join(RAW_DIR, f)
        for f in os.listdir(RAW_DIR)
        if f.lower().endswith(".csv")
    ]
    if not csv_files:
        raise RuntimeError(f"No CSV files found in {RAW_DIR}")

    latest = max(csv_files, key=os.path.getmtime)
    print(f"Detected CSV file: {latest}")


def spark_clean_csv_to_parquet() -> None:
    raw_dir = Path("/data/raw")
    processed_dir = Path("/data/processed")

    csv_files = sorted(raw_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime)
    if not csv_files:
        raise RuntimeError("No CSV files found in /data/raw")

    latest = csv_files[-1]
    out_dir = processed_dir / latest.stem  # e.g. /data/processed/test1/

    spark = (
        SparkSession.builder.appName("local_airflow_pyspark_clean")
        .master("local[*]")
        .getOrCreate()
    )

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(str(latest))
    )

    # SSN cleaning:
    # 1) remove everything except digits
    # 2) keep only exactly 9 digits
    # 3) format as ###-##-####
    digits = F.regexp_replace(F.col("ssn").cast("string"), r"[^0-9]", "")
    df2 = (
        df.withColumn("ssn_digits", digits)
        .filter(F.length("ssn_digits") == 9)
        .withColumn(
            "ssn",
            F.concat_ws(
                "-",
                F.substring("ssn_digits", 1, 3),
                F.substring("ssn_digits", 4, 2),
                F.substring("ssn_digits", 6, 4),
            ),
        )
        .drop("ssn_digits")
    )

    df_in = df.count()
    df_out = df2.count()

    df2.write.mode("overwrite").parquet(str(out_dir))

    print(f"Input:  {latest}")
    print(f"Output: {out_dir} (parquet)")
    print(f"Rows in:  {df_in}")
    print(f"Rows out: {df_out}")

    spark.stop()


with DAG(
    dag_id="local_dropzone_watch",
    start_date=datetime(2026, 1, 1),
    schedule="*/1 * * * *",
    catchup=False,
    default_args={"retries": 0},
    tags=["local", "dropzone"],
) as dag:
    wait_for_csv = PythonSensor(
        task_id="wait_for_csv",
        python_callable=any_csv_exists,
        poke_interval=10,
        timeout=60 * 10,
        mode="poke",
    )

    show_what_we_found = PythonOperator(
        task_id="show_what_we_found",
        python_callable=log_latest_csv,
    )

    spark_transform = PythonOperator(
        task_id="spark_clean_csv_to_parquet",
        python_callable=spark_clean_csv_to_parquet,
    )

    wait_for_csv >> show_what_we_found >> spark_transform
