#user imports
from utils.utils import _local_to_s3

#airflow imports 
from airflow.models import DAG
#airflow variables at Admin>Variable
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

#Config 
BUCKET_NAME = Variable.get("BUCKET")
LOCAL_PATH = Variable.get("LOCAL_PATH")

#DAG definition
default_args = {'owner': 'airflow','start_date': days_ago(1)}

dag = DAG(
    #how the dag will be named
    dag_id = 'user_behavior', 
    default_args= default_args, 
    schedule_interval=None,
    max_active_runs = 1,
    )

with dag:
    extract_user_purchase_data = PostgresOperator(
        dag=dag,
        task_id="extract_user_purchase_data",
        #its in dag folder
        sql="sql/unload_user_purchase.sql",
        postgres_conn_id="postgres_local",
        #/tmp/user_purchase.csv
        params={"user_purchase": LOCAL_PATH,"begin_date":"01/01/2023","end_date":"04/30/2023"},
        #depends_on_past=True,
        #wait_for_downstream=True,
    )
    user_purchase_to_stage_data_lake = PythonOperator(
        dag=dag,
        task_id="user_purchase_to_stage_data_lake",
        python_callable=_local_to_s3,
        op_kwargs={
            "file_name": "/opt/airflow/plugins/user_purchase.csv",
            "key": "stage/user_purchase/{{ ds }}/user_purchase.csv",
            "bucket_name": BUCKET_NAME,
            "remove_local": "true",
        },
    )
 #   to_raw_data_lake= PythonOperator(
        #dag: (Required)The DAG object to which the task belongs.
#        dag=dag,
        #task_id: (Required)A unique identifier for the task.
 #       task_id="to_raw_data_lake",
        #python_callable: (Required)A Python function will be executed when the task is run.
#        python_callable=_local_to_s3,
        #op_kwargs: A dictionary of keyword arguments that will be
        #passed to the python_callable function when the operator calls it.
 #       op_kwargs={
            #/opt/airflow/ <- basic base file path given in docker-compose.yaml
 #           "file_name": "/opt/airflow/plugins/data/some_data.csv",
            # {{ ds }} = is airflow template reference to the
            # DAG run's logical date YYYY-MM-DD
 #           "key": "raw/some_data/{{ ds }}/some_data.csv",
#            "bucket_name": BUCKET_NAME,
#        },
 #   )

 
    extract_user_purchase_data>>user_purchase_to_stage_data_lake
    #to_raw_data_lake
    # task1>>task2
    # task2>>task3