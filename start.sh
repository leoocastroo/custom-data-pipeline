cd docker

docker-compose -f docker-compose-debezium.yaml -f docker-compose-spark.yaml up -d

sleep 8

curl --header "Content-Type:application/json" \
  --request POST \
 --data '{"name": "inventory-connector","config": {"connector.class": "io.debezium.connector.postgresql.PostgresConnector","database.hostname": "postgres","database.port": "5432","database.user": "postgres","database.password": "postgres","database.dbname" : "postgres","database.server.name": "dbserver1","table.include.list": "inventory.customers"}}' \
 http://localhost:8083/connectors

docker logs `docker ps -aqf "name=docker_jupyterlab"`