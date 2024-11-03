# Realtime Data Streaming

## References

* Start from this video: [Realtime Data Streaming |  End To End Data Engineering Project](https://www.youtube.com/watch?v=GqAcTrqKcrY&t=3s&ab_channel=CodeWithYu)
* [Tutorial on the TaskFlow API — Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/2.3.1/tutorial_taskflow_api.html)

## Architecture

![1730554725756](image/README/1730554725756.png)

* Data Source: [Random User Generator | Home](https://randomuser.me/)
* Apache Airflow
* Apache Zookeeper
* Apache Kafka
  * Broker
  * Schema registry
  * Control center
* Apache Spark
  * Master
  * Worker
* Cassandra

| App UI               | URL                                                           |
| -------------------- | ------------------------------------------------------------- |
| Airflow              | [http://localhost:8080/home](http://localhost:8080/home)         |
| Kafka Control Center | [http://localhost:9021/clusters](http://localhost:9021/clusters) |
| Spark Master         | [http://localhost:9090/](http://localhost:9090/)                 |

## Tasks

**Steps to do**

1. [ ] Get data from the API
2. [ ] Build infrastructure using docker compose
3. [ ] Stream data into Kafka
4. [ ] Process by Spark
5. [ ] Stream data into Cassandra

### Get data from API

* [ ] Using request to get data

### Build infrastructure

* [ ] Zookeeper
* [ ] Kafka
* [ ] Airflow
* [ ] Spark
* [ ] cassandra

### Stream data into Kafka

* [ ] Add Kafka setup to docker-compose
* [ ] Implement Kafka send message code
* [ ] Add Airflow setup to docker-compose

### Apache Spark processing

* [ ] Setup Spark (master and 1 worker)
* [ ] Setup Cassandra

### Stream data into Cassandra

* [ ] Add Spark stream

## Run the project locally

#### Environments

```
py -3.11 -m venv .venv

.\.venv\Scripts\activate

python -m pip install -r requirements.txt
```

#### API account

[https://randomuser.me/api]()

### Run streaming

```
spark-submit --master spark://localhost:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 user_spark_stream.py
```


```
# PowerShell (draft)

spark-submit --master spark://localhost:7077 --packages com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1 user_spark_stream.py

spark-submit --master spark://localhost:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 user_spark_stream.py


spark-submit --master spark://localhost:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.1 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 user_spark_stream.py


spark-submit --master spark://localhost:7077 `
    --packages com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1 `
    user_spark_stream.py


spark-submit --master spark://localhost:7077 `
    --jars /opt/bitnami/spark/jars/spark-cassandra-connector_2.13-3.4.1.jar,/opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.13-3.4.1.jar `
  user_spark_stream.py


```
