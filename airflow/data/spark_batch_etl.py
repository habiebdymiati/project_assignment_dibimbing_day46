import argparse
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

# Helper function untuk mendapatkan/membuat Spark Session di Docker Cluster
def get_spark_session(app_name="BatchProcessingPySpark"):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .config("spark.hadoop.dfs.permissions.enabled", "false")
    )
    
    master_url = os.environ.get("SPARK_MASTER_URL", "")
    if master_url:
        builder = builder.master(master_url)
        
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def run_pipeline(orders_path, order_items_path, output_path):
    spark = None
    try:
        # Inisialisasi Spark Session sekali di awal pipeline (Best Practice)
        spark = get_spark_session()
        
        # =====================================================================
        # 1. Task: Membaca Dataset (orders.csv & order_items.csv)
        # =====================================================================
        print("➡️ Menjalankan Task 1: Membaca data...")
        df_orders = spark.read.csv(orders_path, header=True, inferSchema=True)
        df_items = spark.read.csv(order_items_path, header=True, inferSchema=True)
        
        # =====================================================================
        # 2. Task: Join Kedua Tabel menggunakan 'order_id'
        # =====================================================================
        print("➡️ Menjalankan Task 2: Melakukan join tabel...")
        df_joined = df_orders.join(df_items, on="order_id", how="inner")
        
        # =====================================================================
        # 3. Task: Transformasi Data (Pembersihan, Standarisasi, & Kolom Turunan)
        # =====================================================================
        print("➡️ Menjalankan Task 3: Transformasi Data...")
        
        # Penanda filter data kosong/tidak valid
        is_status_empty = (F.col("order_status").isNull() | (F.trim(F.col("order_status")) == ""))
        is_payment_empty = (F.col("payment_method").isNull() | (F.trim(F.col("payment_method")) == ""))
        
        df_transformed = df_joined \
            .filter(F.col("order_id").isNotNull()) \
            .fillna({"quantity": 0, "price": 0.0}) \
            .withColumn("quantity", F.col("quantity").cast(IntegerType())) \
            .withColumn("price", F.col("price").cast(DoubleType())) \
            .withColumnRenamed("price", "unit_price") \
            .withColumn("order_status", F.when(is_status_empty, F.lit("UNKNOWN")).otherwise(F.col("order_status"))) \
            .withColumn("payment_method", F.when(is_payment_empty, F.lit("UNKNOWN")).otherwise(F.col("payment_method"))) \
            .withColumn("value_per_line", F.col("quantity") * F.col("unit_price"))

        # =====================================================================
        # 4. Task: Menyimpan ke Parquet Akhir & Validasi
        # =====================================================================
        print(f"➡️ Menjalankan Task 4: Menyimpan hasil ke Parquet di: {output_path}")
        df_transformed.write.mode("overwrite").parquet(output_path)

        # Validasi pembacaan kembali menggunakan Spark
        df_check = spark.read.parquet(output_path)
        print(f"✅ PROSES BERHASIL! Total baris di file output Parquet: {df_check.count()}")
        
        # Menampilkan sampel data hasil akhir untuk memastikan
        df_check.show(5, truncate=False)
        
        sys.exit(0)  # Sinyal sukses untuk Airflow Task

    except Exception as e:
        print(f"❌ Terjadi kesalahan fatal pada pipeline Spark: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)  # Sinyal gagal untuk Airflow Task
        
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Production Spark Batch Process")
    parser.add_argument("--orders-csv", required=True, help="Path ke berkas orders.csv")
    parser.add_argument("--items-csv", required=True, help="Path ke berkas order_items.csv")
    parser.add_argument("--output-parquet", required=True, help="Path hasil output akhir parquet")
    parser.add_argument("--staging-dir", required=False) 
    
    args = parser.parse_args()
    run_pipeline(args.orders_csv, args.items_csv, args.output_parquet)
