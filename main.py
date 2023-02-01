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
import boto3
import awswrangler as wr
from datetime import datetime
from geopy.distance import geodesic
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

raw_data_bucket = "de-tech-assessment-2022"
raw_data_prefix = "data"

spark = SparkSession.builder.appName("Loka Application").getOrCreate()


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


@F.udf(returnType=FloatType())
def geodesic_udf(a, b):
    return geodesic(a, b).km


def get_events_from_date(event_date: str = ""):
    event_list = get_s3_paths(event_date)
    events_folder = download_only_new_files(event_list)

    for ff in os.listdir(events_folder):
        pass
        # df = df.withColumn(
        # "Lengths/m", geodesic_udf(F.array("B", "A"), F.array("D", "C"))
        # )
        # df.show()


# read event
# Possible problems identified:
# * Events that *arrive* after processing the old data
# * Would have to


if __name__ == "__main__":
    get_events_from_date("2019-06-01")
    get_events_from_date()
    df = spark.read.json("/tmp/loka-data")
    df.show()
    # Displays the content of the DataFrame to stdout
    # df = df.withColumn("Lengths/m", geodesic_udf(F.array("B", "A"), F.array("D", "C")))
