#!/bin/bash
set -e

if [ -f .env ]; then
  export $(cat .env | xargs)
else
  echo "No se encontró archivo .env"
  exit 1
fi

echo "🧹 Iniciando destrucción de zona $ZONE_ID..."

# 0. Eliminar registro del Main Gateway
echo "🗑️ Eliminando registro en el Gateway Principal..."
MAIN_CONF_FILE="../main-gateway/confs/${ZONE_ID}.conf"
if [ -f "$MAIN_CONF_FILE" ]; then
    rm "$MAIN_CONF_FILE"
    docker exec main-api-gateway nginx -s reload 2>/dev/null || true
fi

# 1. Borrar API Gateway de Zona
echo "🗑️ Eliminando API Gateway de Zona..."
docker compose -f zone-gateway/docker-compose.yml -p "${ZONE_ID}-gateway" down --remove-orphans || true
rm -f zone-gateway/nginx.conf

# 2. Borrar Spark Structured Streaming
echo "🗑️ Eliminando Spark Streaming..."
docker compose -f spark-streaming-app/docker-compose.yml -v -p "${ZONE_ID}-spark" down --remove-orphans || true

# Limpiar HDFS (Solo Checkpoints para permitir reinicio limpio)
echo "🐘 Limpiando checkpoints en HDFS para la zona $ZONE_ID..."
docker exec namenode hdfs dfs -rm -r -f "/checkpoints/trazas_v5/$ZONE_ID" || true
# Ya no borramos /data/trazas_v5/zona=$ZONE_ID para preservar el histórico de datos

# 3. Borrar Apps de la malla
echo "🗑️ Eliminando aplicaciones de la malla..."
for app_dir in ./server-mesh/apps/*; do
    if [ -d "$app_dir" ]; then
        export APP_NAME=$(basename "$app_dir")
        echo " -> App: $APP_NAME"
        docker compose -f server-mesh/docker-compose.yml \
            -p "${ZONE_ID}-${APP_NAME}" \
            down -v --remove-orphans || true
    fi
done

# 4. Borrar Infra Kafka y Redis
echo "Eliminando infraestructura Kafka y Redis..."
docker compose -p "${ZONE_ID}-infra" -f streaming-kafka/docker-compose.yml down -v --remove-orphans || true
docker compose -p "${ZONE_ID}-redis" -f redis-streaming/docker-compose.yml down -v --remove-orphans || true

# 5. Borrar DataNode
echo "Eliminando DataNode $ZONE_ID..."
docker compose -f datanode/docker-compose.yml \
    -p "${ZONE_ID}-hdfs" \
    down --remove-orphans || true

# 6. Remover entradas de topología
CSV_FILE="../hadoop-config/topology-mapping.csv"
echo "Eliminando entradas de topología HDFS para $ZONE_ID..."

sed -i "/${ZONE_ID}-datanode/d" "$CSV_FILE"
sed -i "/${ZONE_ID}-spark-processor/d" "$CSV_FILE"

# 7. Remover red
echo "Eliminando red: $ZONE_NETWORK_NAME..."
docker network rm "$ZONE_NETWORK_NAME" 2>/dev/null || true

# 8. Actualizando NameNode
echo "Forzando al NameNode a actualizar la topología..."
docker exec namenode hdfs dfsadmin -refreshNodes

echo "Zona $ZONE_ID eliminada por completo."
