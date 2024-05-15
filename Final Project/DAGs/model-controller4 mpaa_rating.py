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
    dagrun_timeout=timedelta(minutes=10),
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


mpaa_rating1_sql = f"""
declare prompt_query STRING default "Given a dataset containing information about box office sales and ratings for movies, some entries are missing the mpaa_rating field. Your task is to predict the missing MPAA ratings based on your knowledge of the movie and your knowledge of the standard MPAA ratings such as PG and PG-13. Use this information to infer the likely MPAA rating, which classifies films to indicate suitability for certain audiences based on their content. Only return the actual rating, no reasoning. For example, 'G', 'PG, 'PG-13'. Return the output as JSON, include the item_id in the output.";

create or replace table {stg_dataset_name}.mpaa_rating1 as
  select *
  from ML.generate_text(
    model remote_models_central.gemini_pro1,
    (select concat(prompt_query, to_json_string(json_object("item_id", item_id, "movie_title", movie_title,
"release_date", release_date, "genre", genre,
"mpaa_rating", mpaa_rating, "total_gross", total_gross,
"inflation_adjusted_gross", inflation_adjusted_gross))) as prompt
    from {stg_dataset_name}.movies_total_gross2
    where mpaa_rating is NULL
    order by item_id #limit 50
  ),
  struct(0 as temperature, 8192 as max_output_tokens, 0.0 as top_p, 1 as top_k, TRUE as flatten_json_output)
);

"""

# Task to execute the BigQuery SQL query
mpaa_rating1 = BigQueryInsertJobOperator(
    task_id="mpaa_rating1",
    configuration={
        "query": {
            "query": mpaa_rating1_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

update_data_source_sql = f"""
update {stg_dataset_name}.movies_total_gross2
  set data_source = 'mpaa_rating_ai' where mpaa_rating is null
"""

# Task to execute the BigQuery SQL query
update_data_source = BigQueryInsertJobOperator(
    task_id="update_data_source",
    configuration={
        "query": {
            "query": update_data_source_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

update_mpaa_rating_sql = f"""
update {stg_dataset_name}.movies_total_gross2 set mpaa_rating =
  (select json_value(ml_generate_text_llm_result, '$.mpaa_rating')
   from {stg_dataset_name}.mpaa_rating1
   where item_id = json_value(ml_generate_text_llm_result, '$.item_id'))
where mpaa_rating is NULL
"""

# Task to execute the BigQuery SQL query
update_mpaa_rating = BigQueryInsertJobOperator(
    task_id="update_mpaa_rating",
    configuration={
        "query": {
            "query": update_mpaa_rating_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# Define the task sequence
start >> create_remote_models_central >> wait1 >> gemini >> wait2 >> mpaa_rating1 >> wait3 >> update_data_source >> update_mpaa_rating >> end


