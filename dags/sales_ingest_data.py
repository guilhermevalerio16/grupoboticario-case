from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (BigQueryInsertJobOperator, 
                                                               BigQueryCreateEmptyDatasetOperator,
                                                               BigQueryDeleteTableOperator,
                                                               BigQueryUpdateTableSchemaOperator,
                                                               BigQueryTableCheckOperator)
from airflow.providers.google.cloud.operators.gcs import (GCSFileTransformOperator, 
                                                          GCSCreateBucketOperator,
                                                          GCSDeleteBucketOperator)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from airflow.models.baseoperator import chain
from configurations.utils import (
    SALES_FILES,
    QUERIES_FILES,
    RAW_DATASET_SALES,
    REFINED_DATASET_SALES,
    SALES_BUCKET,
    PROJECT_ID,
    PROCESSING_BUCKET,
    RAW_PROCESSING_TABLE,
    ROOT_PATH, 
    
)
from schema.tables_schemas import SCHEMAS





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
    "gb_sales_data_ingestion",
    start_date=datetime(2023, 4, 7),
    description="ETL das bases xlsx de vendas para o BigQuery",
    catchup=False,
    schedule_interval='@once',
    template_searchpath=ROOT_PATH,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=45),    
    tags=["Grupo Boticario", "Sales data ingestion"],

) as dag:
    
    with TaskGroup(group_id="start_pipeline") as start_pipeline:
        start = EmptyOperator(
            task_id="start"
            )

        create_raw_processing_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id="create_raw_processing_dataset", 
            dataset_id=RAW_DATASET_SALES
        )

        create_gcs_processing_bucket = GCSCreateBucketOperator(
            task_id="create_gcs_processing_bucket",
            bucket_name=PROCESSING_BUCKET,
            storage_class="MULTI_REGIONAL",
            )
        
        chain(start, [create_raw_processing_dataset, create_gcs_processing_bucket])

    with TaskGroup(group_id="sensor_file_existence") as sensor_file_existence:
        for base, file_path in SALES_FILES.items():
            GCSObjectExistenceSensor(
                task_id=f"sensor_file_existence_{base}", 
                bucket=SALES_BUCKET, 
                object=file_path
            )


    with TaskGroup(group_id="transform_data_into_processing_bucket") as transform_data_into_processing_bucket:
        for key, file_path in SALES_FILES.items():
            gcs_transform_file = GCSFileTransformOperator(
                task_id=f"xlsx_{key}_to_csv",
                source_bucket=SALES_BUCKET,
                source_object=file_path,
                destination_bucket=PROCESSING_BUCKET,
                destination_object=f"{key}.csv",
                transform_script=[
                    "python",
                    "/home/airflow/gcs/dags/configurations/xlsx_to_csv.py",
                ],
            )

    with TaskGroup(group_id="ingest_data_gcs_to_bq") as ingest_data_gcs_to_bq:
        for key, file_path in SALES_FILES.items():
                gcs_to_bq = GCSToBigQueryOperator(
                    task_id=f"gcs_{key}_into_bq",
                    bucket=PROCESSING_BUCKET,
                    source_objects=f"{key}.csv",
                    destination_project_dataset_table=f"{PROJECT_ID}.{RAW_DATASET_SALES}.{RAW_PROCESSING_TABLE}",
                    write_disposition="WRITE_APPEND",
                    create_disposition="CREATE_IF_NEEDED"
                )

    table_num_rows_check = BigQueryTableCheckOperator(
        task_id="table_num_rows_check",
        table=f"{RAW_DATASET_SALES}.{RAW_PROCESSING_TABLE}",
        checks={
            "row_count_check": {"check_statement": "COUNT(*) = 3000"},
        },
    )


    with TaskGroup(group_id="ingest_refined_sales_data") as ingest_refined_sales_data:
        for key, query in QUERIES_FILES.items():
            ingest_refined_sales_data_into_bq = BigQueryInsertJobOperator(
                task_id=key,
                configuration={
                    "jobType": "QUERY",
                    "query": {
                        "query": f"{{% include '/sql/{query}' %}}",
                        "destinationTable": {
                                "projectId": f"{PROJECT_ID}",
                                "datasetId": f"{REFINED_DATASET_SALES}",
                                "tableId": key
                            },
                        "createDisposition": "CREATE_IF_NEEDED",
                        "writeDisposition": "WRITE_TRUNCATE",
                        "useLegacySql": False,
                    },
                },
            )

    with TaskGroup(group_id="update_schema") as update_schema:
        for table, schema in SCHEMAS.items():
            BigQueryUpdateTableSchemaOperator(
                task_id=f"update_schema_{table}",
                dataset_id="refined_sales",
                table_id=table,
                schema_fields_updates=schema,
            )
            

    with TaskGroup(group_id="end_pipeline") as end_pipeline:
        delete_raw_processing_table = BigQueryDeleteTableOperator(
            task_id="delete_raw_processing_table",
            deletion_dataset_table=f"{PROJECT_ID}.{RAW_DATASET_SALES}.{RAW_PROCESSING_TABLE}",
        )

        delete_gcs_processing_bucket = GCSDeleteBucketOperator(
            task_id="delete_processing_bucket",
            bucket_name=PROCESSING_BUCKET,
            )

        end = EmptyOperator(
            task_id="end"
        )

        chain([delete_gcs_processing_bucket, delete_raw_processing_table], end)

    chain(
        start_pipeline,
        sensor_file_existence,
        transform_data_into_processing_bucket,
        ingest_data_gcs_to_bq,
        table_num_rows_check,
        ingest_refined_sales_data,
        update_schema,
        end_pipeline,
    )
