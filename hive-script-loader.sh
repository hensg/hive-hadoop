#!/bin/bash
set -x

# copy local files to namenode container
docker cp data.dat namenode:/tmp/
docker cp hive-script.sql namenode:/tmp/
docker cp hive-udf/target/hive-udf-1.0-SNAPSHOT.jar namenode:/tmp/hive-udf-1.0-SNAPSHOT.jar

# put container local files to HDFS
docker exec -it namenode hdfs dfs -put -f file:///tmp/data.dat /tmp/search_results.dat
docker exec -it namenode hdfs dfs -put -f file:///tmp/hive-script.sql /tmp/hive-script.sql

docker exec -it namenode hdfs dfs -mkdir /udfs
docker exec -it namenode hdfs dfs -put -f file:///tmp/hive-udf-1.0-SNAPSHOT.jar /udfs/hive-udf-1.0-SNAPSHOT.jar

# exec hive-load with hive to create table and load data in
docker run --rm --name hive-cli --net hadoop-net -v hive-volume:/mnt/hive myhadoop-2.6-hive-1.1.0 hive -f hdfs://namenode:8020/tmp/hive-script.sql
