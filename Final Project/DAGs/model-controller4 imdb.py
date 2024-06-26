import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.dates import days_ago
from datetime import timedelta
from pendulum import duration

# Set project and dataset details
project_id = 'team1-final'
stg_dataset_name = 'disney_stg_af'
fp_ai_dataset_name = 'disney_fp_af_ai'
remote_model = 'remote_models_central'
region = 'us-central1'

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    dag_id='model-controller',
    default_args=default_args,
    description='Controller DAG for data operations',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=20),
)

create_remote_models_central = BigQueryCreateEmptyDatasetOperator(
    task_id='create_remote_models_central',
    project_id=project_id,
    dataset_id=remote_model,
    location=region,
    if_exists='ignore',
    dag=dag)


# Define tasks
start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
wait1 = TimeDeltaSensor(task_id="wait1", delta=duration(seconds=5), dag=dag)
wait2 = TimeDeltaSensor(task_id="wait2", delta=duration(seconds=5), dag=dag)
wait3 = TimeDeltaSensor(task_id="wait3", delta=duration(seconds=5), dag=dag)
wait4 = TimeDeltaSensor(task_id="wait4", delta=duration(seconds=5), dag=dag)
wait5 = TimeDeltaSensor(task_id="wait5", delta=duration(seconds=5), dag=dag)
wait6 = TimeDeltaSensor(task_id="wait6", delta=duration(seconds=5), dag=dag)
wait7 = TimeDeltaSensor(task_id="wait7", delta=duration(seconds=5), dag=dag)
wait8 = TimeDeltaSensor(task_id="wait8", delta=duration(seconds=5), dag=dag)

gemini_sql = f"""
create or replace model {remote_model}.gemini_pro1
  remote with connection `projects/team1-final/locations/us-central1/connections/vertex_connection1`
  options (endpoint = 'gemini-pro');

"""


# Task to execute the BigQuery SQL query
gemini = BigQueryInsertJobOperator(
    task_id="gemini",
    configuration={
        "query": {
            "query": gemini_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# imdb

imdb1_sql = f"""
declare prompt_query STRING default "Based on the provided information about the movie and what you know about the movie, predict the IMDB score of the movie. For example, 7.0, 8.0, 4.0, 6.0, etc. Predict a value even if you are unsure. Return the output as json, include the item_id in the output as well";

create or replace table {stg_dataset_name}.imdb1 as
  select *
  from ML.generate_text(
    model remote_models_central.gemini_pro1,
    (
    select concat(prompt_query, to_json_string(json_object("item_id", dm.item_id, "title", dm.title,
"director", dm.director, "cast", dm.cast,
"country", dm.country, "release_year", dm.release_year,
"release_date", dm.release_date, "rating", dm.rating,
"movie_runtime", dm.movie_runtime, "genre1", dm.genre1,
"genre2", dm.genre2, "genre3", dm.genre3,
"description", dm.description, "budget", dm.budget,
"box_office", dm.box_office, "imdb", dm.imdb,
"metascore", dm.metascore, "rotten_tomatoes", dm.rotten_tomatoes,
"language", dm.language, "primary_production_company", dm.primary_production_company,
"additional_production_companies", dm.additional_production_companies, "produced_by", dm.produced_by,
"based_on", dm.based_on, "music_by", dm.music_by,
"distributed_by", dm.distributed_by, "cinematography", dm.cinematography,
"edited_by", dm.edited_by, "screenplay_by", dm.screenplay_by,
"date_added", dm.date_added))) as prompt
    from {stg_dataset_name}.disney_movies as dm
    where imdb is NULL
    order by item_id
  ),
  struct(0 as temperature, 8192 as max_output_tokens, 0.0 as top_p, 1 as top_k, TRUE as flatten_json_output)
);

"""

# Task to execute the BigQuery SQL query
imdb1 = BigQueryInsertJobOperator(
    task_id="imdb1",
    configuration={
        "query": {
            "query": imdb1_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


update_data_source_imdb_sql = f"""
update {stg_dataset_name}.disney_movies
  set data_source = 'imdb_ai' where imdb is null
"""

# Task to execute the BigQuery SQL query
update_data_source_imdb = BigQueryInsertJobOperator(
    task_id="update_data_source_imdb",
    configuration={
        "query": {
            "query": update_data_source_imdb_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

update_imdb_sql = f"""
UPDATE {stg_dataset_name}.disney_movies
SET imdb = CAST(
    (SELECT json_value(ml_generate_text_llm_result, '$.imdb')
     FROM {stg_dataset_name}.imdb1
     WHERE item_id = json_value(ml_generate_text_llm_result, '$.item_id')) AS FLOAT64
)
WHERE imdb IS NULL;
"""

# Task to execute the BigQuery SQL query
update_imdb = BigQueryInsertJobOperator(
    task_id="update_imdb",
    configuration={
        "query": {
            "query": update_imdb_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# Define the task sequence
start >> create_remote_models_central >> wait1 >> gemini >> wait2 >> imdb1 >> wait3 >> update_data_source_imdb >> wait4 >> update_imdb >> end
