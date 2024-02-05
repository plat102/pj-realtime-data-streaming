# Introduction

Learm from this video:
https://www.youtube.com/watch?v=GqAcTrqKcrY&list=PL_Ct8Cox2p8UlTfHyJc3RDGuGktPNs9Q3&index=5

# Architecture

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

# Guide

## Steps to do

1. [ ] Get data from the API
2. [ ] Build infrastructure using docker compose
3. [ ] Stream data into Kafka
4. [ ] Process by Spark
5. [ ] Stream data into Cassdra

## Things to improve

* Packages management
  * [python - PIP Constraints Files - Stack Overflow](https://stackoverflow.com/questions/34645821/pip-constraints-files)
* 

# How to run it

## Setup

### Environments

`python3.9 -m venv .venv `

`source .venv/bin/activate `

` pip install -r requirements.txt`

* `apache-airflow`: [How to Install Apache Airflow on Mac | by Egemen Eroglu | Medium](https://erogluegemen.medium.com/how-to-install-apache-airflow-on-mac-df9bd5cf1ff8)
  * `pip install 'apache-airflow==2.8.0' \ --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.0/constraints-3.8.txt"`
    * [apache/airflow: Apache Airflow - A platform to programmatically author, schedule, and monitor workflows (github.com)](https://github.com/apache/airflow)
  * `pip install "apache-airflow==2.6.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.6.1/constraints-3.11.txt"`
  * [python 3.x - Pip install on Mac OS gets error: command &#39;/usr/bin/clang&#39; failed with exit code 1 - Stack Overflow](https://stackoverflow.com/questions/64881510/pip-install-on-mac-os-gets-error-command-usr-bin-clang-failed-with-exit-code)

`pip install kafka-python`

### API account

[https://randomuser.me/api]()

# Checklists

## Get data from API

* [ ] Using request to get data

## Build infrastructure

* [ ] Zookeeper
* [ ] Kafka
* [ ] Spark
* [ ] cassandra

## Stream data into Kafka


## Apache Spark processing

## Stream data into Cassandra
