import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.dates import days_ago
from datetime import timedelta
from pendulum import duration

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

dag = DAG(
    dag_id='master-controller',
    default_args=default_args,
    description='master controller dag',
    schedule_interval=None,
    max_active_runs=1,
    concurrency=1,
    catchup=False,
    dagrun_timeout=None,
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
wait_after_ingest = TimeDeltaSensor(task_id="wait_after_ingest", delta=duration(seconds=60), dag=dag)
wait_after_model = TimeDeltaSensor(task_id="wait_after_model", delta=duration(seconds=180), dag=dag)
wait_after_key = TimeDeltaSensor(task_id="wait_after_key", delta=duration(seconds=120), dag=dag)

mpaa_rating_controller = TriggerDagRunOperator(
    task_id="mpaa_rating_controller",
    trigger_dag_id="model-controller4 mpaa_rating",  
    dag=dag)
    
box_office_controller = TriggerDagRunOperator(
    task_id="box_office_controller",
    trigger_dag_id="model-controller4 box_office",  
    dag=dag)

budget_controller = TriggerDagRunOperator(
    task_id="budget_controller",
    trigger_dag_id="model-controller4 budget",  
    dag=dag)

release_date_controller = TriggerDagRunOperator(
    task_id="release_date_controller",
    trigger_dag_id="model-controller4 release_date",  
    dag=dag)

rotten_tomatoes_controller = TriggerDagRunOperator(
    task_id="rotten_tomatoes_controller",
    trigger_dag_id="model-controller4 rotten_tomatoes",  
    dag=dag)

metascore_controller = TriggerDagRunOperator(
    task_id="metascore_controller",
    trigger_dag_id="model-controller4 metascore",  
    dag=dag)

imdb_controller = TriggerDagRunOperator(
    task_id="imdb_controller",
    trigger_dag_id="model-controller4 imdb",  
    dag=dag)

start >> mpaa_rating_controller >> end
start >> box_office_controller >> end
start >> budget_controller >> end
start >> release_date_controller >> end
start >> rotten_tomatoes_controller >> end
start >> metascore_controller >> end
start >> imdb_controller >> end

