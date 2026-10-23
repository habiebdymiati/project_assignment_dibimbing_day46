# Gunakan versi Airflow 2.10.5 untuk kompatibilitas
FROM apache/airflow:2.10.5-python3.11

USER root

# Install OpenJDK 17 dengan repositori bersih
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Kembalikan ke user airflow sebelum instalasi pip
USER airflow

# PENTING: Gunakan symlink default-java bawaan OS agar aman di semua arsitektur (AMD64 & ARM64)
ENV JAVA_HOME: /usr/lib/jvm/java-17-openjdk-amd64



# Install dependensi PySpark pendukung
RUN pip install --no-cache-dir \
    "pyspark==3.5.4" \
    "apache-airflow-providers-apache-hive==9.0.0" \
    "apache-airflow-providers-apache-spark==5.0.0"
