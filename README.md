# Simple Local Airflow + PySpark Dropzone Pipeline

A small project that runs locally with Airflow:
Just drop a CSV into a folder, Airflow schedules a PySpark job, cleans SSNs format, drops invalid rows, and writes output in Parquet format.

## Architecture

Raw CSV (local folder) → Airflow (Docker) → PySpark transform → Parquet (local folder)

Folders on host machine:
- `D:\files\raw` (input)
- `D:\files\processed` (output)

Mounted into containers as:
- `/data/raw`
- `/data/processed`

## What it does

- Checks every one minutes and Detects latest CSV in `/data/raw`
- Reads using Spark (local mode)
- Cleans SSN:
  - removes non-digits
  - keeps only exactly 9 digits
  - formats as `###-##-####`
- Rows with invalid SSNs are dropped
- Writes Parquet to `/data/processed/<csv_stem>/`

## Prereqs

- Docker Desktop (Linux containers)
- Windows folder created:
  - `D:\files\raw`
  - `D:\files\processed`

## Run

Initialize + start Airflow:
