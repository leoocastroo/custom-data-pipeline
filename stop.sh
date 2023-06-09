cd docker

docker-compose -f docker-compose-debezium.yaml -f docker-compose-spark.yaml -f docker-compose-airflow.yaml down