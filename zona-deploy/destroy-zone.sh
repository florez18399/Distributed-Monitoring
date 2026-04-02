#!/bin/bash
set -e

if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
else
  echo "No se encontro archivo .env"
  exit 1
fi

# --- Argumento: zone_id ---
if [ -z "$1" ]; then
  echo "Uso: ./destroy-zone.sh <zone_id>"
  echo "Ejemplo: ./destroy-zone.sh zone1"
  exit 1
fi

export ZONE_ID=$1
Z_NUM=$(echo $ZONE_ID | tr -dc '0-9')
export ZONE_NETWORK_NAME="zone_${Z_NUM}_net"

# Buscar manifiesto desplegado
DEPLOYED_MANIFEST=".deployed/${ZONE_ID}.json"
if [ ! -f "$DEPLOYED_MANIFEST" ]; then
  echo "No se encontro manifiesto desplegado para $ZONE_ID en $DEPLOYED_MANIFEST"
  echo "Intentando limpieza manual..."
  DEPLOYED_MANIFEST=""
fi

echo "Iniciando destruccion de zona $ZONE_ID (Red: $ZONE_NETWORK_NAME)..."

# 0. Eliminar registro del Main Gateway
echo "Eliminando registro en el Gateway Principal..."
MAIN_CONF_FILE="../main-gateway/confs/${ZONE_ID}.conf"
if [ -f "$MAIN_CONF_FILE" ]; then
    rm "$MAIN_CONF_FILE"
    docker exec main-api-gateway nginx -s reload 2>/dev/null || true
fi

# 1. Borrar API Gateway de Zona
echo "Eliminando API Gateway de Zona..."
docker compose -f zone-gateway/docker-compose.yml -p "${ZONE_ID}-gateway" down --remove-orphans || true
rm -f zone-gateway/nginx.conf

# 2. Borrar Spark Structured Streaming
echo "Eliminando Spark Streaming..."
docker compose -f spark-streaming-app/docker-compose.yml -v -p "${ZONE_ID}-spark" down --remove-orphans || true

# Limpiar checkpoints en HDFS
echo "Limpiando checkpoints en HDFS para la zona $ZONE_ID..."
docker exec namenode hdfs dfs -rm -r -f "/checkpoints/trazas_v5/$ZONE_ID" || true

# 3. Borrar Apps de la malla (desde manifiesto o fallback)
echo "Eliminando aplicaciones de la malla..."
if [ -n "$DEPLOYED_MANIFEST" ] && [ -f "$DEPLOYED_MANIFEST" ]; then
    APP_COUNT=$(jq '.apps | length' "$DEPLOYED_MANIFEST")
    for i in $(seq 0 $((APP_COUNT - 1))); do
        APP_NAME=$(jq -r ".apps[$i].name" "$DEPLOYED_MANIFEST")
        echo " -> App: $APP_NAME"
        docker compose -f server-mesh/docker-compose.yml \
            -p "${ZONE_ID}-${APP_NAME}" \
            down -v --remove-orphans || true
    done
else
    # Fallback: buscar containers con prefijo de zona
    echo " -> Buscando containers con prefijo ${ZONE_ID}-..."
    for container in $(docker ps -a --filter "name=${ZONE_ID}-" --format "{{.Names}}" | grep -E "\-core$" | sed "s/${ZONE_ID}-//;s/-core$//"); do
        echo " -> App (detectada): $container"
        export APP_NAME=$container
        docker compose -f server-mesh/docker-compose.yml \
            -p "${ZONE_ID}-${container}" \
            down -v --remove-orphans || true
    done
fi

# 4. Borrar Kafka
echo "Eliminando infraestructura Kafka..."
docker compose -p "${ZONE_ID}-infra" -f streaming-kafka/docker-compose.yml down -v --remove-orphans || true

# 5. Borrar DataNode
echo "Eliminando DataNode $ZONE_ID..."
docker compose -f datanode/docker-compose.yml \
    -p "${ZONE_ID}-hdfs" \
    down --remove-orphans || true

# 6. Remover entradas de topologia
CSV_FILE="../hadoop-config/topology-mapping.csv"
echo "Eliminando entradas de topologia HDFS para $ZONE_ID..."
sed -i "/${ZONE_ID}-datanode/d" "$CSV_FILE"
sed -i "/${ZONE_ID}-spark-processor/d" "$CSV_FILE"

# 7. Remover red
echo "Eliminando red: $ZONE_NETWORK_NAME..."
docker network rm "$ZONE_NETWORK_NAME" 2>/dev/null || true

# 8. Actualizando NameNode
echo "Forzando al NameNode a actualizar la topologia..."
docker exec namenode hdfs dfsadmin -refreshNodes 2>/dev/null || true

# 9. Limpiar manifiesto desplegado
if [ -f "$DEPLOYED_MANIFEST" ]; then
    rm "$DEPLOYED_MANIFEST"
fi

echo "Zona $ZONE_ID eliminada por completo."
