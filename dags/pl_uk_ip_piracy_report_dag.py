from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from common.functions.gen_utils import read_config, read_query
from scripts.pl_uk_ip_piracy_report_traffic.pl_uk_ip_piracy_report_traffic import main as tmp_name
from scripts.pl_uk_ip_piracy_report_detection.pl_uk_ip_piracy_report_detection import read_pl_fixtures, read_traffic, model_one
import pandas as pd

# Define DAG object
args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 4, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id='pl_uk_ip_piracy_report_dag', default_args=args,
          description='DAG to detect IPs infringing during pl matches',
          schedule_interval=timedelta(days=1))


def fixtures_check_func(**context):
    """
    first task that just read a csv file of matchs time from bigquery and send a value to branch tasks,
     which show if there is a match today or not

      Args:
      storage_client: storage object
      bucket: string for google storage bucket name
      SQL_to_load_league_fixtures_file: string of query which loaded from storage.
      bq_client: big query object
      dates_df: DataFrame of league_fixtures_file
      val: int (1 or 2) that show if there was match or not(1 if there was)
      task_instance: object to perform Xcom
      execution_date: string date of today

     Returns:

    """
    pl_fixtures = read_pl_fixtures()

    # execution_date shows the execution date for running dag.
    execution_date = datetime(2021, 4, 12)
    print('Airflow is running the ML algorithm')

    # check if there is a match on the execution date.
    if (execution_date == pl_fixtures['date']).any():
        cnd = 1
    else:
        cnd = -1

    # send the condition (cnd) parameter to Xcom
    task_instance = context['task_instance']
    task_instance.xcom_push(key="Match Day", value=cnd)


def selector_func(**kwargs):
    """
    The selector function is raise a flag to stop the dag operation if there is no match on the current execution date.
    Args:
      cnd: A dict value received from upper (previous) task in the tree.
    Returns:
      True or False
    """

    # pull the condition parameter from the Xcom.
    ti = kwargs['ti']
    cnd = ti.xcom_pull(task_ids='fixtures_check', key='Match Day')
    if cnd == 1:
        return True
    else:
        return False


def ml_model_one_func(*context):
    """
     This function runs the traffic query on the dag running date. Then the query results will be analysed by the ML
     Model 1. The ML model 1 identify potential infringing IPs and save them as a csv file in google storage or a table
      in google bigquery.
    Args:
    Returns:
     """
    # Create a file name ... Format of csv file name, which demonstrate todays date
    ip_traffic = read_traffic()

    pl_fixtures = read_pl_fixtures()

    match_date = datetime(2021, 4, 12)
    blob_list = model_one(ip_traffic, pl_fixtures,  match_date)

    # send the blob list to Xcom to piracy report operator
    task_instance = context['task_instance']
    task_instance.xcom_push(key="Blob List", value=blob_list)


# Define airflow operators
fixtures_check_op = PythonOperator(
    task_id='fixtures_check',
    provide_context=True,
    xcom_push=True,
    python_callable=fixtures_check_func,
    dag=dag)

shrt_cr_op = ShortCircuitOperator(
    task_id='skip_downstream',
    provide_context=False,
    python_callable=selector_func,
    dag=dag)

ml_model_one_op = PythonOperator(
    task_id='ml_model_one',
    provide_context=True,
    python_callable=ml_model_one_func,
    dag=dag)

piracy_report_op = PythonOperator(
    task_id='piracy_report',
    provide_context=True,
    python_callable=tmp_name,
    dag=dag)

# Define sequence of running task
fixtures_check_op >> shrt_cr_op >> ml_model_one_op >> piracy_report_op