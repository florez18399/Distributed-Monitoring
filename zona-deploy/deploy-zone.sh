#!/bin/bash
set -e

if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
else
  echo "No se encontro archivo .env"
  exit 1
fi

# --- Argumentos: zone_id y manifiesto ---
if [ -z "$1" ] || [ -z "$2" ]; then
  echo "Uso: ./deploy-zone.sh <zone_id> <manifest.json>"
  echo "Ejemplo: ./deploy-zone.sh zone1 zones/zone1.json"
  exit 1
fi

export ZONE_ID=$1
MANIFEST=$2

if [ ! -f "$MANIFEST" ]; then
  echo "Manifiesto no encontrado: $MANIFEST"
  exit 1
fi

Z_NUM=$(echo $ZONE_ID | tr -dc '0-9')
export ZONE_NETWORK_NAME="zone_${Z_NUM}_net"

echo "Desplegando zona $ZONE_ID (Red: $ZONE_NETWORK_NAME) con manifiesto: $MANIFEST"

# 1. Crear Red Base
docker network create $ZONE_NETWORK_NAME 2>/dev/null || true

# 2. Registrar DataNode en topologia HDFS
CSV_FILE="../hadoop-config/topology-mapping.csv"
echo "Registrando zona en topologia HDFS..."
if ! grep -q "${ZONE_ID}-datanode" "$CSV_FILE"; then
  echo "${ZONE_ID}-datanode,/rack/${ZONE_ID}" >> "$CSV_FILE"
fi
if ! grep -q "${ZONE_ID}-spark-processor" "$CSV_FILE"; then
  echo "${ZONE_ID}-spark-processor,/rack/${ZONE_ID}" >> "$CSV_FILE"
fi

echo "Levantando DataNode para $ZONE_ID..."
docker compose -f datanode/docker-compose.yml \
    -p "${ZONE_ID}-hdfs" \
    up -d

# 3. Asegurar Redis Global y desplegar Kafka
export KAFKA_PORT_EXT=$((9090 + Z_NUM * 2))
echo "Desplegando infraestructura para $ZONE_ID (Kafka: $KAFKA_PORT_EXT)"

echo "Asegurando Redis Global..."
(cd .. && docker compose -f redis-streaming/docker-compose.yml up -d)

echo "Levantando Kafka..."
docker compose -p "${ZONE_ID}-infra" -f streaming-kafka/docker-compose.yml up -d

# 4. Renderizar envoy.yaml desde template
echo "Renderizando configuracion de Envoy..."
export ZONE_GATEWAY_HOST="${ZONE_ID}-api-gateway"
envsubst '${ZONE_GATEWAY_HOST}' < server-mesh/commons/envoy.yaml.template > server-mesh/commons/envoy.yaml

# 5. Configurar Kafka broker host para Fluent Bit
export KAFKA_BROKER_HOST="${ZONE_ID}-broker"

# 6. Preparar nginx.conf de zona
ZONE_NGINX_CONF="zone-gateway/nginx.conf"
cat <<EOF > "$ZONE_NGINX_CONF"
events { worker_connections 1024; }
http {
    server {
        listen 80;
EOF

# 7. Desplegar apps desde el manifiesto
CURRENT_PORT=$((18000 + Z_NUM * 100))
APP_COUNT=$(jq '.apps | length' "$MANIFEST")

echo "Desplegando $APP_COUNT aplicaciones..."

for i in $(seq 0 $((APP_COUNT - 1))); do
    export APP_NAME=$(jq -r ".apps[$i].name" "$MANIFEST")
    export SCENARIO=$(jq -r ".apps[$i].scenario" "$MANIFEST")
    export DOWNSTREAM_CALLS=$(jq -c ".apps[$i].downstream_calls" "$MANIFEST")
    export HOST_PORT=$CURRENT_PORT

    echo " -> App: $APP_NAME (Port: $HOST_PORT, Scenario: $SCENARIO)"
    echo "    Downstream: $DOWNSTREAM_CALLS"

    docker compose -f server-mesh/docker-compose.yml \
        -p "${ZONE_ID}-${APP_NAME}" \
        up -d --build

    # Agregar ruta al nginx de zona
    cat <<EOF >> "$ZONE_NGINX_CONF"
        location /${APP_NAME} {
            proxy_pass http://${ZONE_ID}-${APP_NAME}-envoy:8080;
            proxy_http_version 1.1;
        }
EOF
    CURRENT_PORT=$((CURRENT_PORT + 1))
done

# 8. Agregar ruta cross-zone al nginx
cat <<EOF >> "$ZONE_NGINX_CONF"
        location /_external/ {
            proxy_pass http://main-api-gateway:81/;
            proxy_http_version 1.1;
        }
    }
}
EOF

# 9. Levantar API Gateway de Zona
echo "Levantando API Gateway de Zona..."
docker compose -f zone-gateway/docker-compose.yml -p "${ZONE_ID}-gateway" up -d

# 10. Registrar zona en el Main Gateway
echo "Registrando zona $ZONE_ID en el Gateway Principal..."
MAIN_CONF_DIR="../main-gateway/confs"
mkdir -p "$MAIN_CONF_DIR"

cat <<EOF > "${MAIN_CONF_DIR}/${ZONE_ID}.conf"
location /${ZONE_ID}/ {
    proxy_pass http://${ZONE_ID}-api-gateway:80/;
}
EOF

docker exec main-api-gateway nginx -s reload 2>/dev/null || echo "Main Gateway no detectado, asegurate de levantarlo."

# 11. Levantar Spark Structured Streaming
echo "Levantando Spark Streaming..."
docker compose -f spark-streaming-app/docker-compose.yml -p "${ZONE_ID}-spark" up -d --build

# 12. Guardar manifiesto desplegado para destroy
mkdir -p .deployed
cp "$MANIFEST" ".deployed/${ZONE_ID}.json"

echo "Zona $ZONE_ID desplegada correctamente."
