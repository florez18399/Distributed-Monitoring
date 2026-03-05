# Limpiar datos corruptos en namenode

docker exec -it namenode hdfs dfsadmin -safemode leave
docker exec -it namenode hdfs fsck / -delete
docker exec -it namenode hdfs dfs -rm -r -skipTrash /checkpoints/trazas_v3