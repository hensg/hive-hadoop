#!/bin/bash

echo 'Stoping datanode...'
docker stop datanode

echo 'Stoping namenode...'
docker stop namenode

echo 'Stoping nodemanager...'
docker stop nodemanager

echo 'Stoping resourcemanager...'
docker stop resourcemanager

echo 'Stoping proxyserver...'
docker stop proxyserver

echo 'Stoping mapredhistory...'
docker stop mapredhistory
