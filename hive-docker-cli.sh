#!/bin/bash
docker run --rm --name hive-cli --net hadoop-net -v hive-volume:/mnt/hive -it hadoop hive
