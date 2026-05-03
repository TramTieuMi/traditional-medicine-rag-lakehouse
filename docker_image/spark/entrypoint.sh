#!/bin/bash
set -e

if [ "$SPARK_MODE" = "master" ]; then
    /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
elif [ "$SPARK_MODE" = "worker" ]; then
    /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker "$SPARK_MASTER_URL"
else
    echo "SPARK_MODE must be 'master' or 'worker'"
    exit 1
fi