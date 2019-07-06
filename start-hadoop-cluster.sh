#!/bin/bash

echo 'Creating network hadoop-net'
docker network ls|grep hadoop-net > /dev/null || docker network create hadoop-net

echo 'Starting namenode container...'
docker run --rm --name namenode --net hadoop-net -v hadoop-volume:/mnt/hadoop -p 8020:8020 -p 50070:50070 -p 50090:50090 -p 50100:50100 -d hadoop hdfs namenode

echo 'Starting datanode container...'
docker run --rm --name datanode --net hadoop-net -v hadoop-volume:/mnt/hadoop -p 50075:50075 -p 50010:50010 -p 50020:50020 -d hadoop hdfs datanode

echo 'Starting resourcemanager container...'
docker run --rm --name resourcemanager --net hadoop-net -p 50030:50030 -p 8088:8088 -p 8021:8021 -p 8050:8050 -p 8025:8025 -p 8030:8030 -p 8032:8032 -p 8141:8141 -d hadoop yarn resourcemanager

echo 'Starting nodemanager container...'
docker run --rm --name nodemanager --net hadoop-net -p 8042:8042 -p 10200:10200 -d hadoop yarn nodemanager

echo 'Starting yarn proxyserver container...'
docker run --rm --name proxyserver --net hadoop-net -p 8089:8089 -d hadoop yarn proxyserver

echo 'Starting mapred history server container...'
docker run --rm --name mapredhistory --net hadoop-net -p 10020:10020 -p 19888:19888 -p 10033:10033 -p 51111:51111 -d hadoop mapred historyserver

