import json
import pathlib
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id="download_rocket_launches",
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * 1,3,5",
    catchup=False,
)

download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag,
)


def get_picture():
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

    with open("/tmp/launches.json") as f:
        launches = json.load(f)

    img_urls = [launch["image"] for launch in launches["results"] if launch["image"]]

    for img_url in img_urls:
        try:
            response = requests.get(img_url)
            image_filename = img_url.split("/")[-1]
            target_file = f"/tmp/images/{image_filename}"

            with open(target_file, "wb") as img:
                img.write(response.content)

            print(f"Downloaded {img_url}")

        except requests.exceptions.MissingSchema:
            print(f"{img_url} invalid URL")

        except requests.exceptions.ConnectionError:
            print(f"Connection error {img_url}")


get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=get_picture,
    dag=dag,
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images | wc -l) images"',
    dag=dag,
)

download_launches >> get_pictures >> notify