from datetime import datetime
from pathlib import Path
import pandas as pd

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator


# -----------------------------
# Python Task
# -----------------------------
def calculate_stats(input_path: str, output_path: str):
    """
    Reads event JSON for a single interval,
    aggregates per-date per-user counts,
    and writes results to CSV.
    """

    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Read data
    events = pd.read_json(input_path)

    # Handle empty intervals safely
    if events.empty:
        print("No events found for this interval.")
        return

    # Aggregate
    stats = (
        events
        .groupby(["date", "user"])
        .size()
        .reset_index(name="event_count")
    )

    # Save output
    stats.to_csv(output_path, index=False)


# -----------------------------
# DAG Definition
# -----------------------------
with DAG(
    dag_id="backfilling",
    start_date=datetime(2026, 2, 1),
    schedule="@daily",
    catchup=True,   # Required for backfilling
    tags=["backfill", "etl"],
) as dag:

    # Fetch only this run's interval
    fetch_events = BashOperator(
        task_id="fetch_events",
        bash_command=(
            "mkdir -p /tmp/data && "
            "echo 'Fetching {{ data_interval_start }} to {{ data_interval_end }}' && "
            "curl -s -o /tmp/data/events_{{ ds }}.json "
            "'http://events_api:5000/events?"
            "start_date={{ data_interval_start | ds }}&"
            "end_date={{ data_interval_end | ds }}'"
        ),
    )

    calculate_statistics = PythonOperator(
        task_id="calculate_stats",
        python_callable=calculate_stats,
        op_kwargs={
            "input_path": "/tmp/data/events_{{ ds }}.json",
            "output_path": "/tmp/data/output_{{ ds }}.csv",
        },
    )

    fetch_events >> calculate_statistics