#!/bin/bash
docker run --rm --name hive-cli --net hadoop-net -v hive-volume:/mnt/hive -it myhadoop-2.6-hive-1.1.0 hive
