from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (BigQueryInsertJobOperator,
                                                               BigQueryCreateEmptyDatasetOperator,
                                                               BigQueryUpdateTableSchemaOperator)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import datetime
from datetime import datetime, timedelta
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup
from configurations.utils import (
    insert_podcasts_search_into_gcs, 
    insert_datahackers_episodes_into_gcs,
    BUCKET_SPOTIFY, 
    PROJECT_ID,
    DATASET_ID_SPOTIFY, 
    TABLE_ID_SEARCH, 
    TABLE_ID_EPISODES, 
    OBJECT_SEARCH,   
    OBJECT_EPISODES,
    ROOT_PATH,
    GB_EPISODES_TABLE,
    GB_EPISODES_SQL
    )
from schema.tables_schemas import gb_data_hackers_episodes



default_args = {
    "owner": "Grupo Boticario",
    "depends_on_past": False,
    "email": ["guilhermevalerio16@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1
}

with DAG(
    "gb_spotify_data_ingestion",
    catchup=False,
    start_date=datetime(2023, 8, 2),
    schedule_interval='@once',
    description="ETL dos dados do Spotify para o BigQuery",
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=45),
    template_searchpath=ROOT_PATH,
    tags=["Grupo Boticario", "Spotify data ingestion"],

) as dag:
    
    with TaskGroup(group_id="start_pipeline") as start_pipeline:
        start = EmptyOperator(
            task_id="start"
            )

        create_spotify_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id="create_raw_processing_dataset", 
            dataset_id=DATASET_ID_SPOTIFY
        )

        create_gcs_spotify_bucket = GCSCreateBucketOperator(
            task_id="create_gcs_spotify_bucket",
            bucket_name=BUCKET_SPOTIFY,
            storage_class="MULTI_REGIONAL",
        )

        
        chain(start, [create_spotify_dataset, create_gcs_spotify_bucket])

    ingest_podcasts_search_into_gcs = PythonOperator(
        task_id="ingest_podcasts_search_into_gcs",
        python_callable=insert_podcasts_search_into_gcs,
        op_kwargs={
            "bucket_name": BUCKET_SPOTIFY,
            "file_name": f"{OBJECT_SEARCH}.csv",
            "url": "https://api.spotify.com/v1/search",
            "params": {
                "q": "data hackers",
                "type": "show",
                "limit": "50",
                "offset": "0",
                "market": "BR"
            },
        },
    )

    ingest_datahackers_episodes_into_gcs = PythonOperator(
        task_id="ingest_datahackers_episodes_into_gcs",
        python_callable=insert_datahackers_episodes_into_gcs,
        op_kwargs={
            "bucket_name": BUCKET_SPOTIFY,
            "file_name": f"{OBJECT_EPISODES}.csv",
            "url": "https://api.spotify.com/v1/shows/1oMIHOXsrLFENAeM743g93/episodes",
            "params": {
                "limit": "50",
                "offset": "0",
                "market": "BR"
            },
        },
    )


    podcasts_search_gcs_to_bq = GCSToBigQueryOperator(
        task_id=f"podcasts_search_gcs_to_bq",
        bucket=BUCKET_SPOTIFY,
        source_objects=f"{OBJECT_SEARCH}.csv",
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID_SPOTIFY}.{TABLE_ID_SEARCH}",
        schema_fields=[
            {
                'name': 'name', 
                'type': 'STRING',
                'description': 'The name of the episode'
                
            },
            {
                'name': 'description', 
                'type': 'STRING',
                'description': 'A description of the episode'
            },
            {
                'name': 'id', 
                'type': 'STRING',
                'description': 'The Spotify ID for the show'
            },
            {
                'name': 'total_episodes', 
                'type': 'INTEGER',
                'description': 'The total number of episodes in the show.'
            }
        ],
        write_disposition="WRITE_TRUNCATE",
        create_disposition='CREATE_IF_NEEDED'
    )

    ingest_datahackers_episodes_gcs_to_bq = GCSToBigQueryOperator(
        task_id=f"ingest_datahackers_episodes_gcs_to_bq",
        bucket=BUCKET_SPOTIFY,
        source_objects=f"{OBJECT_EPISODES}.csv",
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID_SPOTIFY}.{TABLE_ID_EPISODES}",
        schema_fields=[
            {
                'name': 'id', 
                'type': 'STRING', 
                'description': 'The Spotify ID for the show'
            },
            {
                'name': 'name', 
                'type': 'STRING', 
                'description': 'The name of the episode'
            },
            {
                'name': 'description', 
                'type': 'STRING', 
                'description': 'A description of the episode'
            },
            {
                'name': 'release_date', 
                'type': 'DATE', 
                'description': 'The date the episode was first released, for example "1981-12-15. Depending on the precision, it might be shown as "1981" or "1981-12"'
            },
            {
                'name': 'duration_ms', 
                'type': 'INTEGER', 'description': 
                'The episode length in milliseconds'
            },
            {
                'name': 'languages', 
                'type': 'STRING', 
                'description': 'A list of the languages used in the episode, identified by their ISO 639-1 code'
            },
            {
                'name': 'explicit', 
                'type': 'BOOL', 
                'description': 'Whether or not the episode has explicit content (true = yes it does; false = no it does not OR unknown)'
            },
            {
                'name': 'type', 
                'type': 'STRING', 
                'description': 'The object type'
            }
        ],
        write_disposition="WRITE_TRUNCATE",
        create_disposition='CREATE_IF_NEEDED'
    )


    with TaskGroup(group_id="ingest_gb_episodes_data_into_bq") as ingest_gb_episodes_data_into_bq:
        datahackers_gb_episodes = BigQueryInsertJobOperator(
            task_id="datahackers_gb_episodes",
            configuration={
                "jobType": "QUERY",
                "query": {
                    "query": f"{{% include '/sql/{GB_EPISODES_SQL}' %}}",
                    "destinationTable": {
                            "projectId": f"{PROJECT_ID}",
                            "datasetId": f"{DATASET_ID_SPOTIFY}",
                            "tableId": GB_EPISODES_TABLE
                        },
                        "createDisposition": "CREATE_IF_NEEDED",
                        "writeDisposition": "WRITE_TRUNCATE",
                    "useLegacySql": False,
                },
            },
        )

        schema_gb_data_hackers_episodes = BigQueryUpdateTableSchemaOperator(
            task_id="schema_gb_data_hackers_episodes",
            dataset_id="spotify",
            table_id="gb_datahackers_episodes",
            schema_fields_updates=gb_data_hackers_episodes,
        )

        chain(datahackers_gb_episodes, schema_gb_data_hackers_episodes)


    end = EmptyOperator(
        task_id="end"
        )

chain(start_pipeline,
      ingest_podcasts_search_into_gcs,
      podcasts_search_gcs_to_bq,
      end
      )

chain(start_pipeline,
      ingest_datahackers_episodes_into_gcs,
      ingest_datahackers_episodes_gcs_to_bq,
      ingest_gb_episodes_data_into_bq,
      end
      )
