import argparse
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

# Helper function dinamis untuk mendapatkan/membuat Spark Session di Docker Cluster
def get_spark_session(app_name="BatchProcessingPySpark"):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Wajib: umask 000 agar file Parquet bisa ditulis lintas UID kontainer Docker
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .config("spark.hadoop.dfs.permissions.enabled", "false")
    )
    
    # Otomatis menggunakan URL master kluster jika tersedia dari Airflow Connection
    master_url = os.environ.get("SPARK_MASTER_URL", "")
    if master_url:
        builder = builder.master(master_url)
        
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def run_pipeline(orders_path, order_items_path, staging_dir, output_path):
    try:
        # Define internal staging paths exactly like the original processes
        staging_orders_raw = os.path.join(staging_dir, "orders_raw")
        staging_items_raw = os.path.join(staging_dir, "items_raw")
        staging_orders_clean = os.path.join(staging_dir, "orders_clean")
        staging_items_clean = os.path.join(staging_dir, "items_clean")
        staging_data_joined = os.path.join(staging_dir, "data_joined")
        staging_data_standardized = os.path.join(staging_dir, "data_standardized")
        staging_data_final = os.path.join(staging_dir, "data_final")

        # =====================================================================
        # 1. Task: Membaca dataset awal dan menyimpannya ke temporary staging
        # =====================================================================
        spark = get_spark_session()
        print("Membaca data orders.csv dan order_items.csv...")
        df_orders = spark.read.csv(orders_path, header=True, inferSchema=True)
        df_items = spark.read.csv(order_items_path, header=True, inferSchema=True)
        
        # Simpan sementara ke staging parquet
        df_orders.write.mode("overwrite").parquet(staging_orders_raw)
        df_items.write.mode("overwrite").parquet(staging_items_raw)
        spark.stop()

        # =====================================================================
        # 2. Task: Pembersihan Data
        # =====================================================================
        spark = get_spark_session()
        df_orders = spark.read.parquet(staging_orders_raw)
        df_items = spark.read.parquet(staging_items_raw)
        
        print("Melakukan pembersihan data...")
        df_orders_clean = df_orders.filter(df_orders["order_id"].isNotNull())
        df_items_clean = df_items.filter(df_items["order_id"].isNotNull())
        df_items_clean = df_items_clean.fillna({"quantity": 0, "price": 0.0})
        
        df_orders_clean.write.mode("overwrite").parquet(staging_orders_clean)
        df_items_clean.write.mode("overwrite").parquet(staging_items_clean)
        spark.stop()

        # =====================================================================
        # 3. Task: Join dataset
        # =====================================================================
        spark = get_spark_session()
        df_orders_clean = spark.read.parquet(staging_orders_clean)
        df_items_clean = spark.read.parquet(staging_items_clean)
        
        print("Melakukan proses join tabel...")
        df_joined = df_orders_clean.join(df_items_clean, on="order_id", how="inner")
        
        df_joined.write.mode("overwrite").parquet(staging_data_joined)
        spark.stop()

        # =====================================================================
        # 4. Task: Standarisasi Data
        # =====================================================================
        spark = get_spark_session()
        df_joined = spark.read.parquet(staging_data_joined)
        
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
            
        df_transformed.write.mode("overwrite").parquet(staging_data_standardized)
        spark.stop()

        # =====================================================================
        # 5. Task: Membuat Kolom Turunan value_per_line
        # =====================================================================
        spark = get_spark_session()
        df_transformed = spark.read.parquet(staging_data_standardized)
        
        print("Membuat kolom turunan value_per_line...")
        df_final = df_transformed.withColumn("value_per_line", F.col("quantity") * F.col("unit_price"))
        
        df_final.write.mode("overwrite").parquet(staging_data_final)
        spark.stop()

        # =====================================================================
        # 6. Task: Menyimpan ke Parquet Akhir & Validasi
        # =====================================================================
        spark = get_spark_session()
        df_final = spark.read.parquet(staging_data_final)
        
        print(f"Menyimpan hasil akhir ke format Parquet di: {output_path}")
        df_final.write.mode("overwrite").parquet(output_path)

        # Validasi (FIXED: Membaca format Parquet hasil keluaran yang valid)
        df_check = spark.read.parquet(output_path)
        
        print(f"PROSES BERHASIL! Total baris yang disimpan di output akhir: {df_check.count()}")
        spark.stop()
        
        sys.exit(0)  # Sinyal sukses untuk Airflow Task

    except Exception as e:
        print(f"❌ Terjadi kesalahan fatal pada pipeline Spark: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)  # Sinyal gagal untuk Airflow Task

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Production Spark Decoupled Batch Process")
    parser.add_argument("--orders-csv", required=True, help="Path ke berkas orders.csv")
    parser.add_argument("--items-csv", required=True, help="Path ke berkas order_items.csv")
    parser.add_argument("--staging-dir", required=True, help="Direktori utama target penampungan folder staging")
    parser.add_argument("--output-parquet", required=True, help="Path hasil output akhir parquet")
    
    args = parser.parse_args()
    run_pipeline(args.orders_csv, args.items_csv, args.staging_dir, args.output_parquet)
