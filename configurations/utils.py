from airflow.models import Variable
import pandas as pd
import requests
import json
import base64
from google.cloud import storage


ROOT_PATH = Variable.get("ROOT_PATH", default_var="/home/airflow/gcs/dags/")
GB_SA_PATH = Variable.get("GB_SA_PATH", default_var="/home/airflow/gcs/data/gb-case-project.json") 
API_CLIENT_ID = Variable.get("API_CLIENT_ID", default_var="")
API_CLIENT_SECRET = Variable.get("API_CLIENT_SECRET", default_var="")
SALES_BUCKET = "grupoboticario-case-datalake-sales"
PROCESSING_BUCKET = "grupoboticario-processing-datalake-sales"
RAW_PROCESSING_TABLE = "sales_base"
RAW_DATASET_SALES = "raw_sales"
REFINED_DATASET_SALES = "refined_sales"
BUCKET_SPOTIFY = "grupoboticario-case-datalake-spotify"
PROJECT_ID = "grupoboticario-case"
DATASET_ID_SPOTIFY = "spotify"
TABLE_ID_SEARCH = "spotify_search_podcasts_data"
TABLE_ID_EPISODES = "spotify_datahackers_episodes_data"
GB_EPISODES_TABLE = "gb_datahackers_episodes"
OBJECT_SEARCH = "data_hackers_search"
OBJECT_EPISODES = "data_hackers_episodes"
GB_EPISODES_SQL = "spotify/gb_datahackers_episodes.sql"


SALES_FILES = {
    "base_2017": "sales/Base 2017.xlsx",
    "base_2018": "sales/Base_2018 (1).xlsx",
    "base_2019": "sales/Base_2019 (2).xlsx",
}

QUERIES_FILES = {
    "sales_consolidated_for_year_month": "sales/sales_consolidated_for_year_month.sql",
    "sales_consolidated_for_brand_departament": "sales/sales_consolidated_for_brand_departament.sql",
    "sales_consolidated_for_brand_year_month": "sales/sales_consolidated_for_brand_year_month.sql",
    "sales_consolidated_for_departament_year_month": "sales/sales_consolidated_for_departament_year_month.sql"
}


def get_token():
    auth_string = API_CLIENT_ID + ":" + API_CLIENT_SECRET
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")
    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": f'Basic {auth_base64}',
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "client_credentials"
    }
    response = requests.post(url, headers=headers, data=data)
    json_result = json.loads(response.content)
    token = json_result["access_token"]

    return token



def insert_podcasts_search_into_gcs(bucket_name, file_name, url, params):
    token=get_token()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)


    response = requests.get(url=url, params=params, headers=headers)

    json_object = response.json()

    df = pd.json_normalize(json_object["shows"]["items"])
    df = df[["name", 
             "description", 
             "id", 
             "total_episodes"]]
    
    bucket.blob(file_name).upload_from_string(df.to_csv(index=False), 'text/csv')



def insert_datahackers_episodes_into_gcs(bucket_name, file_name, url, params):
    token=get_token()
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    df = pd.DataFrame()

    while (url):
        response = requests.get(url=url, params=params, headers=headers)
        json_object = response.json()
        df = df.append(pd.json_normalize(json_object["items"]), ignore_index=True)
        url = json_object["next"]
    
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    
    df = df[["id", 
             "name", 
             "description", 
             "release_date",
             "duration_ms", 
             "language", 
             "explicit", 
             "type"]]

    bucket.blob(file_name).upload_from_string(df.to_csv(index=False), 'text/csv')