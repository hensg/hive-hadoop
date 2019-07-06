#!/bin/bash
set -x

# copy local files to namenode container
docker cp data.dat namenode:/tmp/data.dat
docker cp hive-script.sql namenode:/tmp/hive-script.sql

# put container local files to HDFS
docker exec -it namenode hdfs dfs -put -f file:///tmp/data.dat /tmp/data.dat
docker exec -it namenode hdfs dfs -put -f file:///tmp/hive-script.sql /tmp/hive-script.sql

# exec hive-load with hive to create table and load data in
docker run --rm --name hive-cli --net hadoop-net -v hive-volume:/mnt/hive hadoop hive -f hdfs://namenode:/tmp/hive-script.sql
