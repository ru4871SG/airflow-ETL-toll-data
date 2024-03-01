# Libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# DAG arguments
default_args = {
    'owner': 'Ruddy Gunawan',
    'start_date': days_ago(0),
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow ETL Toll Data Example',
    schedule_interval=timedelta(days=1),
)

# Before you run Task 1, make sure you have the tolldata.tgz file in the 'project' folder.
# Use wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz

# Task 1 - unzip_data
task1 = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzf /home/project/finalassignment/tolldata.tgz -C /home/project/airflow/dags',
    dag=dag,
)

# Task 2 - extract_data_from_csv
## This task should extract the fields Rowid, Timestamp, Anonymized Vehicle number, and Vehicle type from 'vehicle-data.csv',
## and save them into a file named 'csv_data.csv'.
task2 = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1-4 /home/project/airflow/dags/vehicle-data.csv > /home/project/airflow/dags/csv_data.csv',
    dag=dag,
)

# Task 3 - extract_data_from_tsv
## This task should extract the fields Number of axles, Tollplaza id, and Tollplaza code from 'tollplaza-data.tsv', 
## and save it into a file named 'tsv_data.csv'
task3 = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5-7 /home/project/airflow/dags/tollplaza-data.tsv > /home/project/airflow/dags/tsv_data.csv',
    dag=dag,
)

# Task 4 - extract_data_from_fixed_width
## This task should extract the fields Type of Payment code, and Vehicle Code from the fixed width file 'payment-data.txt',
## and save it into a file named 'fixed_width_data.csv'
task4 = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="cat /home/project/airflow/dags/payment-data.txt | tr -s '[:space:]' | cut -d ' ' -f11,12 > /home/project/airflow/dags/fixed_width_data.csv",
    dag=dag,
)

# Task 5 - consolidate_data
## This task should create a single csv file named 'extracted_data.csv' by combining data from the following files:
## 'csv_data.csv', 'tsv_data.csv', 'fixed_width_data.csv'
task5 = BashOperator(
    task_id='consolidate_data',
    bash_command='paste /home/project/airflow/dags/csv_data.csv /home/project/airflow/dags/tsv_data.csv /home/project/airflow/dags/fixed_width_data.csv > /home/project/airflow/dags/extracted_data.csv',
    dag=dag,
)

# Task 6 - transform_data
## This task should transform the vehicle_type field in 'extracted_data.csv' into capital letters and save it into 
## a file named 'transformed_data.csv' in the staging directory.
task6 = BashOperator(
    task_id='transform_data',
    bash_command='tr "[a-z]" "[A-Z]" < /home/project/airflow/dags/extracted_data.csv > /home/project/airflow/dags/transformed_data.csv',
    dag=dag,
)

# Task Pipeline
task1 >> task2 >> task3 >> task4 >> task5 >> task6
