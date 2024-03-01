# Apache Airflow Pipeline Script for ETL Toll Data

This repository contains an Apache Airflow pipeline script `airflow_ETL_toll_data.py` designed for ETL (Extract, Transform, Load) processes on a toll data example. The pipeline script demonstrates how to automate and manage different tasks using Apache Airflow.

## Usage

Store the pipeline script file in your `home/username` folder. After that, you need to change all mentions of "project" to your "username" in the script.

You can then use the below command to add the pipeline script into the list of DAGs in your Airflow:
```
sudo cp airflow_ETL_toll_data.py airflow/dags
```

To verify if you have successfully added the pipeline to your list of DAGs, use below command:
```
airflow dags list
```