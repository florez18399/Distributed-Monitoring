#!/bin/bash

echo "Iniciando servicio de sincronización Hive..."

while true; do
    echo "$(date): Sincronizando particiones..."
    
    # Usar Trino para sincronizar (ajusta la URL de Trino)
    curl -X POST \
    # Usar Trino para sincronizar y capturar respuesta explícita
    RESPONSE=$(curl -s -X POST \
      http://trino:8080/v1/statement \
      -H "Content-Type: text/plain" \
      -H "X-Trino-User: hive" \
      -d "CALL hive.system.sync_partition_metadata('default', 'trazas_logs_v5', 'FULL')"
    # Alternativa usando HIVE CLI directamente
    # docker exec hive-metastore hive -e "MSCK REPAIR TABLE kafka_traces.transaction_logs"
      -d "CALL hive.system.sync_partition_metadata('default', 'trazas_logs_v5', 'FULL')")
    
    echo "$(date): Sincronización completada"
    echo "$(date): Respuesta Trino: $RESPONSE"
    sleep 60 
done