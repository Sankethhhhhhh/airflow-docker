from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="fan_in_out",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    task_1 = EmptyOperator(task_id="task_1")
    task_2 = EmptyOperator(task_id="task_2")
    task_3 = EmptyOperator(task_id="task_3")

    join = EmptyOperator(task_id="join")

    start >> [task_1, task_2, task_3]
    [task_1, task_2, task_3] >> join