from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

# Helper function untuk mendapatkan/membuat Spark Session
def get_spark_session():
    return SparkSession.builder \
        .appName("BatchProcessingPySpark") \
        .master("local[*]") \
        .config("spark.executor.memory", "512m") \
        .getOrCreate()

# 1. Task: Membaca dataset awal dan menyimpannya ke temporary staging
def load_data():
    spark = get_spark_session()
    orders_path = "/opt/airflow/data/orders.csv"
    order_items_path = "/opt/airflow/data/order_items.csv"
    
    print("Membaca data orders.csv dan order_items.csv...")
    df_orders = spark.read.csv(orders_path, header=True, inferSchema=True)
    df_items = spark.read.csv(order_items_path, header=True, inferSchema=True)
    
    # Simpan sementara ke staging parquet
    df_orders.write.mode("overwrite").parquet("/opt/airflow/data/staging/orders_raw")
    df_items.write.mode("overwrite").parquet("/opt/airflow/data/staging/items_raw")
    spark.stop()

# 2. Task: Pembersihan Data
def clean_data():
    spark = get_spark_session()
    df_orders = spark.read.parquet("/opt/airflow/data/staging/orders_raw")
    df_items = spark.read.parquet("/opt/airflow/data/staging/items_raw")
    
    print("Melakukan pembersihan data...")
    df_orders_clean = df_orders.filter(df_orders["order_id"].isNotNull())
    df_items_clean = df_items.filter(df_items["order_id"].isNotNull())
    df_items_clean = df_items_clean.fillna({"quantity": 0, "price": 0.0})
    
    df_orders_clean.write.mode("overwrite").parquet("/opt/airflow/data/staging/orders_clean")
    df_items_clean.write.mode("overwrite").parquet("/opt/airflow/data/staging/items_clean")
    spark.stop()

# 3. Task: Join dataset
def join_data():
    spark = get_spark_session()
    df_orders_clean = spark.read.parquet("/opt/airflow/data/staging/orders_clean")
    df_items_clean = spark.read.parquet("/opt/airflow/data/staging/items_clean")
    
    print("Melakukan proses join tabel...")
    df_joined = df_orders_clean.join(df_items_clean, on="order_id", how="inner")
    
    df_joined.write.mode("overwrite").parquet("/opt/airflow/data/staging/data_joined")
    spark.stop()

# 4. Task: Standarisasi Data
def standardize_data():
    spark = get_spark_session()
    df_joined = spark.read.parquet("/opt/airflow/data/staging/data_joined")
    
    print("Melakukan standarisasi tipe data, pengisian UNKNOWN, dan penambahan catatan...")
    is_status_empty = (F.col("order_status").isNull() | (F.trim(F.col("order_status")) == ""))
    is_payment_empty = (F.col("payment_method").isNull() | (F.trim(F.col("payment_method")) == ""))
    is_any_empty = is_status_empty | is_payment_empty

    df_transformed = df_joined \
        .withColumn("quantity", F.col("quantity").cast(IntegerType())) \
        .withColumn("price", F.col("price").cast(DoubleType())) \
        .withColumnRenamed("price", "unit_price") \
        .withColumn(
            "internal_notes",
            F.when(
                is_any_empty,
                F.concat_ws(" | ", F.coalesce(F.col("internal_notes"), F.lit("")), F.lit("Missing Status/Method detected"))
            ).otherwise(F.col("internal_notes"))
        ) \
        .withColumn(
            "order_status",
            F.when(is_status_empty, F.lit("UNKNOWN")).otherwise(F.col("order_status"))
        ) \
        .withColumn(
            "payment_method",
            F.when(is_payment_empty, F.lit("UNKNOWN")).otherwise(F.col("payment_method"))
        )
        
    df_transformed.write.mode("overwrite").parquet("/opt/airflow/data/staging/data_standardized")
    spark.stop()

# 5. Task: Membuat Kolom Turunan value_per_line
def generate_value_per_line():
    spark = get_spark_session()
    df_transformed = spark.read.parquet("/opt/airflow/data/staging/data_standardized")
    
    print("Membuat kolom turunan value_per_line...")
    df_final = df_transformed.withColumn("value_per_line", F.col("quantity") * F.col("unit_price"))
    
    df_final.write.mode("overwrite").parquet("/opt/airflow/data/staging/data_final")
    spark.stop()

# 6. Task: Menyimpan ke Parquet Akhir & Validasi
def export_and_validate():
    spark = get_spark_session()
    output_path = "/opt/airflow/data/output/fact_transactions_parquet"
    df_final = spark.read.parquet("/opt/airflow/data/staging/data_final")
    
    print(f"Menyimpan hasil akhir ke format Parquet di: {output_path}")
    df_final.write.mode("overwrite").parquet("/opt/airflow/data/output/fact_transactions_parquet")

    # Validasi
    # FIX THE READER
    df_check = spark.read.option("header", "true").csv(output_path)
    
    print(f"PROSES BERHASIL! Total baris yang disimpan di output akhir: {df_check.count()}")
    spark.stop()

# Definisi Default Arguments untuk DAG Airflow
default_args = {
    'owner': 'dibimbing_student',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# Definisi DAG Airflow
with DAG(
    'pyspark_batch_processing_dag',
    default_args=default_args,
    description='DAG tugas batch processing segmented step-by-step menggunakan Spark',
    schedule_interval=None,
    catchup=False,
) as dag:

    task_load = PythonOperator(
        task_id='load_raw_data',
        python_callable=load_data,
    )

    task_clean = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
    )

    task_join = PythonOperator(
        task_id='join_data',
        python_callable=join_data,
    )

    task_standardize = PythonOperator(
        task_id='standardize_data',
        python_callable=standardize_data,
    )

    task_value_per_line = PythonOperator(
        task_id='generate_value_per_line_column',
        python_callable=generate_value_per_line,
    )

    task_export_validate = PythonOperator(
        task_id='export_and_validate_output',
        python_callable=export_and_validate,
    )

    # Alur kerja (Task Dependencies)
    task_load >> task_clean >> task_join >> task_standardize >> task_value_per_line >> task_export_validate