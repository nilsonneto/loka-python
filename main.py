"""
Challenge

The goal of this challenge is to automate the build of a simple
yet scalable data lake and data warehouse that will enable our BI team
to answer questions like:
"What is the average distance traveled by our vehicles during an operating period?"

We would like to ask you to develop a solution that:
1. Fetches the data from the bucket on a daily basis and stores it on a data lake;
2. Processes and extracts the main events that occurred during operating periods;
3. Store the transformed data on a data warehouse. The data warehouse should be SQL-queriable (SQL database or using something like AWS Athena).

Technical assumptions
• The fetching process should only get data from a certain day on each run and should run every day;
• Files on the ”raw” S3 bucket can disappear but we might want to process them differently in the future;
• No need to answer the question stated in the introduction;
• If your solution is setup to run locally, it must be containerized;
• There is no need for paid, expensive and highly performant data warehouses. You can use a ”standard” SQL database.

Bonus points
• Sketch how you would set up the application on the cloud (AWS, GCP, etc);
• It is encouraged to simplify the data by a data model on the data warehouse layer.

"""

"""
Dag airflow ou Lambda
Fazer cópia de dados para outro bucket
Classe para cada um dos tipos de dados
Arquivos separados
Arquivo comum com lógica de ingestão de json
Arquivo de dag
Arquivo de Lambda com o tratamento por recebimento de novo arquivo no bucket 


Ingestão
Lambda de cópia, para garantir dado na mesma localização 
Planejar para ingestão manual

Considerando que cada arquivo vai ser do dia do evento, e se for necessário reprocessar, só vai ser daquele dia 

Lógica de ETL:

Descartar fora de período de operação 
Soma de vetores
Armazenar somente distância viajada em cada período, talvez granularizar por minuto ou hora

Considerar casos onde período ou começou e não terminou, ou terminou e não começou 

"""

import os
import awswrangler as wr

from geopy.distance import geodesic
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_timestamp, udf, struct
from pyspark.sql.types import FloatType
from sqlalchemy import create_engine

raw_data_bucket = "de-tech-assessment-2022"
raw_data_prefix = "data"
timestamp_format = "yyyy-MM-dd'T'HH:mm:ss.SSSX"

spark = SparkSession.builder.appName("Loka Application").getOrCreate()
engine = create_engine(
    "postgresql+psycopg2://datawarehouse:datawarehouse@localhost/datawarehouse?client_encoding=utf8"
)


def get_s3_paths(prefix: str) -> list[str]:
    s3_prefix = f"{raw_data_prefix}/{prefix}"
    s3_prefix_path = f"s3://{raw_data_bucket}/{s3_prefix}"
    event_list: list[str] = wr.s3.list_objects(s3_prefix_path)
    return event_list


def download_only_new_files(file_list: list[str]) -> str:
    # Use a folder dedicated, so it can support having a date or not
    folder = f"/tmp/loka-data"
    os.makedirs(folder, exist_ok=True)

    for s3_filepath in file_list:
        filename = s3_filepath.split("/")[-1]
        filepath = f"{folder}/{filename}"
        if os.path.exists(filepath):
            print(f"{filename} exists")
        else:
            print(f"Downloading {filename}")
            wr.s3.download(path=s3_filepath, local_file=filepath)
    return folder


@udf(returnType=FloatType())
def geodesic_udf(a, b):
    return geodesic(a, b).km


def get_events_from_date(event_date: str = "") -> str:
    """
    Downloads events of a certain date to local folder.

    :param event_date: Date of events that should be downloaded
    :returns: Folder where events were stored
    """
    event_list = get_s3_paths(event_date)
    events_folder = download_only_new_files(event_list)
    return events_folder


def read_events_into_dataframe(events_folder: str) -> DataFrame:
    df = spark.read.option("mergeSchema", "true").json(f"{events_folder}/*")
    return df


def fix_dataframe_dates(df: DataFrame) -> DataFrame:
    df = (
        df.withColumn("at", to_timestamp(df.at, timestamp_format))
        .withColumn("date_start", to_timestamp(df.data.start, timestamp_format))
        .withColumn("date_finish", to_timestamp(df.data.finish, timestamp_format))
        .withColumn("location_at", to_timestamp(df.data.location.at, timestamp_format))
        .withColumn(
            "data",
            struct(
                "data.*",
                "date_start",
                "date_finish",
                "location_at",
            ),
        )
        .drop("date_start")
        .drop("date_finish")
        .drop("location_at")
    )
    return df


def save_dataframe_to_postgres(df: DataFrame, table_name: str):
    pdf = df.toPandas()
    pdf.to_sql(table_name, engine, index=False, if_exists="append")


# read event
# Possible problems identified:
# * Events that *arrive* after processing the old data
# * Would have to


if __name__ == "__main__":
    events_folder = get_events_from_date("2019-06-01")
    # Store data on S3
    # get_events_from_date()
    df = read_events_into_dataframe(events_folder)
    df = fix_dataframe_dates(df)

    df_vehicle = (
        df.where(df.on == "vehicle")
        .withColumn("data_id", df.data.id)
        .withColumn("location_at", df.data.location_at)
        .withColumn("location_lat", df.data.location.lat)
        .withColumn("location_lng", df.data.location.lng)
        .drop(df.data)
    )
    df_operating_period = (
        df.where(df.on == "operating_period")
        .withColumn("data_id", df.data.id)
        .withColumn("date_start", df.data.date_start)
        .withColumn("date_finish", df.data.date_finish)
        .drop(df.data)
    )
    save_dataframe_to_postgres(df_vehicle, "vehicle")
    save_dataframe_to_postgres(df_operating_period, "operating_period")
    # Displays the content of the DataFrame to stdout
    # df = df.withColumn("Lengths/m", geodesic_udf(F.array("B", "A"), F.array("D", "C")))
