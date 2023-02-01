Version of Python used: 3.10.9

How to run locally:
* `docker compose up` to start a local instance of PostgreSQL
* `pip install -r requirements.txt` to install all python libraries needed (a local environment using venv or pyenv-virtualenv is highly recommended)
* `python main.py event_date` to start processing

Sketch of how this could run in a cloud environment:
* Having the main script as an Airflow DAG or an AWS Lambda
* Having the "Copy" step as an S3 action, so as to always make sure that any files that arrive on the raw bucket are copied to some place safe and won't be lost
* If needed, use the classes modeled in the `classes.py` file to check if event data is following the expected format

Situations that are expected but not treated:
* Reprocessing a day: data being saved to the PostgreSQL DW is not considering a reprocessing event. To support that, a auxiliary table with dates that were processed could be created, and if an `event_date` is present in the aux. table, first all entries from this date would be deleted from `vehicle` table and only them, readded.
* To answer the question, the `geopy.geodesic` function could be used to calculate the traveled distance of each vehicle.
* Since no `delete` event was present on sample data, it wasn't possible to add and test a filter to the operating_period dataframe.
