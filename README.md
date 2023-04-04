# Custom data pipeline

A data pipeline to process data from various sources (In development)

We use three layers (raw, cleaned and curated) to ingest data into a data lake.

## Motivations

Project was based on an interest in Data Engineering field. It provided a good way to develop skills and experience in a range of tools (spark, airflow, docker, delta).

## Architeture

![Architeture.png](images/Architeture.png)

## Setup

This project is to be used in your own machine, so you just need to run the follow commanda

```commandline
docker/docker-compose up
```

## Tools

Access the tools in your browser

Airflow

```commandline
localhost:8080
```

Spark

```commandline
localhost:8081
```

Jupyter (To use jupyter you must get the token provide in his container)

```commandline
localhost:8888
```

## Screenshots

![airflow.png](images/airflow.png)

![spark.png](images/spark.png)

![jupyter.png](images/jupyter.png)