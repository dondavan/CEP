# Data correlation for kafka event
This directory contains different implementation of event correlator
* ksql: Legacy ksqldb stream process.
* TBA: New event correlator

## ksql:
Ksqldb is a stream process platform, ksqldb connects to kafka topic and executes user sql query. \
This implementation implemented a **local** ksqldb connects to an **cloud** kafka broker. \
It uses docker image to compose service, described in `./ksql/docker-compose.yaml`, contains:
* ksqldb-server
* ksqldb-cli

### Usage:
Compose and start ksqldb:
> Go to ksql directory: `cd ksql` \
> Compose and start ksqldb-server: `sh run_ksqldb.sh` \
Connect to ksql cli:
> Execute in shell: `docker exec -it ksqldb-cli ksql http://ksqldb-server:8088`\

## TBA: