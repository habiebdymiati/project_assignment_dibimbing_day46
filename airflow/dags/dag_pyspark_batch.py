from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

# Definisi Default Arguments untuk DAG Airflow (Sesuai parameter file Anda)
default_args = {
    'owner': 'dibimbing_student',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# Fungsi Validasi Pra-Eksekusi (Pre-flight Check) di Sisi Airflow
def check_source_files_exist():
    orders_path = "/opt/airflow/data/orders.csv"
    order_items_path = "/opt/airflow/data/order_items.csv"
    
    if not os.path.exists(orders_path):
        raise FileNotFoundError(f"❌ Berkas sumber Orders tidak ditemukan: {orders_path}")
    if not os.path.exists(order_items_path):
        raise FileNotFoundError(f"❌ Berkas sumber Order Items tidak ditemukan: {order_items_path}")
        
    print("✅ Pre-flight check OK: Semua berkas input CSV tersedia.")
    return True

# Definisi DAG Airflow
with DAG(
    dag_id='dag_pyspark_batch',
    default_args=default_args,
    description='DAG tugas batch processing segmented step-by-step menggunakan Spark Cluster',
    schedule_interval=None,
    catchup=False,
    tags=["pyspark", "production", "etl"]
) as dag:

    # Task 1: Validasi Awal ketersediaan berkas sebelum dikirim ke Spark Cluster
    task_validate_input = PythonOperator(
        task_id='validate_input_sources',
        python_callable=check_source_files_exist,
        doc_md="Memastikan berkas CSV input sudah ada di direktori data bersama."
    )

    # Task 2: Mengirim seluruh rangkaian logika transformasi tersegmentasi ke Spark Master
    task_run_spark_etl = SparkSubmitOperator(
        task_id='run_pyspark_batch_job',
        conn_id='spark_default',  # Mengarah ke host kluster -> spark://spark:7077
        application='/opt/airflow/data/spark_batch_etl.py',
        application_args=[
            "--orders-csv", "/opt/airflow/data/orders.csv",
            "--items-csv", "/opt/airflow/data/order_items.csv",
            "--staging-dir", "/opt/airflow/data/staging",
            "--output-parquet", "/opt/airflow/data/output/fact_transactions_parquet"
        ],
        total_executor_cores=2,
        executor_cores=1,
        executor_memory="512m",
        driver_memory="512m",
        name="pyspark_batch_production_job",
        verbose=True,
        doc_md="Mengirimkan skrip urutan pemrosesan data batch ke kluster Apache Spark luar."
    )

    # Alur kerja (Task Dependencies)
    task_validate_input >> task_run_spark_etl
