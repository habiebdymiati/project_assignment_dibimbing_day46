# Assignment Data Engineer Bacth 15 Day 46

Jalankan airflow, database dan spark
## airflow
`docker compose --profile airflow up -d`

## postgres dan sql
`docker compose --profile postgres up -d`
`docker compose --profile db up -d` 


## spark
`docker compose --profile spark up -d`

## Tambahkan connection di airflow dengan 
connection_id = spark_default
host = spark://spark
port = 7007

## Penjelasan file python airflow pyspark_batch_processing_dag 
setelah airflow, database dan spark dijalankan,
akses airflow melalui http://localhost:8080/home
dag pyspark_batch_processing_dag untuk melakukan proses
membaca dataset orders.csv dan order_items.csv di dalam folder
/airflow/data, data mentah akan disimpan di airflow/data/output/staging lalu kedua tabel akan dijoin orders_raw dan items_raw
lalu data difilter dari data anomali untuk dan disimpan di orders_clean dan items_clean,
setelahnya data akan di join disimpan di data_joined dan selanjutnya di proses untuk distandarisasi,
status_payment dan payment_method yang kosong akan diisi UNKNOWN dan diberi flags Missing Status/Method detected di kolom internal_notes,
setelahnya, dibuat kolom turunan value_per_line atau gmv, hasil kali dari kolom quantity dan unit_price
data final akan disimpan di data_final dalam bentuk parquet dan disimpan dengan metode overwrite.

## Penjelasan file python airflow dag_pyspark_batch
konsepnya adalah membagi tugas untuk pemrosesan data di file python spark_bacth_etl.py, dan orkestrasinya di file dag_pyspark_bacth 
