#!/usr/bin/env bash
set -e

cd /home/chi/Project_DE/Real_Time_Pipeline_Glamira

docker container stop test-spark || true
docker container rm -f test-spark || true

docker run --rm -it \
  --name test-spark \
  --network streaming-network \
  -v "$PWD":/spark \
  -v spark_lib:/home/spark/.ivy2 \
  -v spark_data:/data \
  -e HOME=/home/spark \
  -e HADOOP_CONF_DIR=/spark/conf/ \
  -e KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9094}" \
  -e KAFKA_SASL_JAAS_CONFIG="${KAFKA_SASL_JAAS_CONFIG:-}" \
  -e KAFKA_PASSWORD="${KAFKA_PASSWORD:-}" \
  -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}" \
  -e POSTGRES_HOST="${POSTGRES_HOST:-postgres}" \
  -e POSTGRES_PORT="${POSTGRES_PORT:-5432}" \
  unigap/spark:3.5 bash -lc '
    set -e
    export HOME=/home/spark
    mkdir -p /home/spark/.ivy2/cache /tmp

    cd /spark/src
    zip -r /tmp/src_code.zip ./*

    source /home/spark/miniconda3/etc/profile.d/conda.sh || true
    conda env create --file /spark/spark-image/environment.yml || true
    conda activate pyspark_conda_env || conda activate base

    spark-submit \
      --master local[*] \
      --deploy-mode client \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
      --conf "spark.ivy.home=/home/spark/.ivy2" \
      --conf "spark.driver.extraJavaOptions=-Duser.home=/home/spark -Djava.io.tmpdir=/tmp" \
      --conf "spark.executor.extraJavaOptions=-Duser.home=/home/spark -Djava.io.tmpdir=/tmp" \
      --conf "spark.sql.shuffle.partitions=1" \
      --conf "spark.default.parallelism=1" \
      --py-files /tmp/src_code.zip \
      /spark/src/main.py
  '
