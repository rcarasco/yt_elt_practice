from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime, timedelta
import pendulum

from api.video_stats import get_playlist_id, get_video_ids, extract_video_details, save_to_json

local_tz = pendulum.timezone("America/New_York")

default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    "max_active_runs": 1,
}

with DAG(
    dag_id="produce_json",
    default_args=default_args,
    schedule="0 14 * * *",
    catchup=False,
) as dag:

    playlist_id_task = get_playlist_id()
    video_ids_task = get_video_ids(playlist_id_task)
    extract_task = extract_video_details(video_ids_task)
    save_task = save_to_json(extract_task)

    playlist_id_task >> video_ids_task >> extract_task >> save_task