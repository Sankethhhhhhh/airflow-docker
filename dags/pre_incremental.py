from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

#dag = DAG(
#    dag_id="01_Unscheduled",
#    start_date=datetime(2026, 1, 1),
#    schedule=None,
#)

# dag = DAG(
#     dag_id="02_scheduled",
#     start_date=datetime(2026, 1, 1),
#     schedule="@daily",    
#)

#dag = DAG(
#    dag_id="04_time_delta",
#    start_date=datetime(2026, 1, 1),
#    end_date=datetime(2026, 1, 31),
#    schedule=timedelta(days=3),
#)

dag = DAG(
    dag_id="pre_incremental_data_processing",
    start_date=datetime(2026, 2, 24),
    schedule="@daily",
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command="curl -o /tmp/events.json http://events_api:5000/events?""start_date=2026-02-25&end_date=2026-02-26",
    dag=dag,
)

def _calculate_stats(input_path, output_path):
    Path(output_path).parent.mkdir(exist_ok=True)

    events = pd.read_json(input_path)
    stats = events.groupby(["date","user"]).size().reset_index()

    stats.to_csv(output_path, index=False)

calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={
        "input_path": "/tmp/data/events.json", 
        "output_path": "/tmp/data/output.csv"
    },
)

fetch_events >> calculate_stats