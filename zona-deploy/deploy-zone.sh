#!/bin/bash
set -e 

if [ -f .env ]; then
  export $(cat .env | xargs)
else
  echo "❌ No se encontró archivo .env"
  exit 1
fi

# 1. Crear Red Base
docker network create $ZONE_NETWORK_NAME 2>/dev/null || true

# 2. Agregando Datanode
CSV_FILE="../hadoop-config/topology-mapping.csv"
echo "Registrando zona en topología HDFS..."
if ! grep -q "${ZONE_ID}-datanode" "$CSV_FILE"; then
  echo "${ZONE_ID}-datanode,/rack/${ZONE_ID}" >> "$CSV_FILE"
fi

if ! grep -q "${ZONE_ID}-spark-processor" "$CSV_FILE"; then
  echo "${ZONE_ID}-spark-processor,/rack/${ZONE_ID}" >> "$CSV_FILE"
fi

echo "🐘 Levantando DataNode para $ZONE_ID..."
docker compose -f datanode/docker-compose.yml \
    -p "${ZONE_ID}-hdfs" \
    up -d


# 3. Desplegando KAFKA

Z_NUM=$(echo $ZONE_ID | tr -dc '0-9')
export KAFKA_PORT_EXT=$((9090 + Z_NUM * 2))
echo "Desplegando Kafka para $ZONE_ID en puerto host: $KAFKA_PORT_EXT"

echo "Levantando Infraestructura (Kafka)..."
docker compose -p "${ZONE_ID}-infra" -f streaming-kafka/docker-compose.yml up -d

# 4. Creando malla de aplicaciones
echo "🚀 Desplegando Aplicaciones y configurando Gateway..."
CURRENT_PORT=$INITIAL_PORT

# Configurar host de Kafka para los sidecars (Fluent Bit)
export KAFKA_BROKER_HOST="${ZONE_ID}-broker"

# Preparar archivo de configuración de Nginx de zona
ZONE_NGINX_CONF="zone-gateway/nginx.conf"
cat <<EOF > "$ZONE_NGINX_CONF"
events { worker_connections 1024; }
http {
    server {
        listen 80;
EOF

for app_dir in ./server-mesh/apps/*; do
    if [ -d "$app_dir" ]; then
        export APP_NAME=$(basename "$app_dir")
        export HOST_PORT=$CURRENT_PORT
        
        echo " -> App: $APP_NAME (Port: $HOST_PORT)"
        
        docker compose -f server-mesh/docker-compose.yml \
            -p "${ZONE_ID}-${APP_NAME}" \
            up -d --build

        # Agregar ruta al Nginx de zona (apunta al sidecar Envoy)
        cat <<EOF >> "$ZONE_NGINX_CONF"
        location /${APP_NAME} {
            proxy_pass http://${ZONE_ID}-${APP_NAME}-envoy:8080;
            proxy_http_version 1.1;
        }
EOF
        CURRENT_PORT=$((CURRENT_PORT + 1))
    fi
done

# Cerrar configuración de Nginx de zona
cat <<EOF >> "$ZONE_NGINX_CONF"
    }
}
EOF

echo "🌐 Levantando API Gateway de Zona..."
docker compose -f zone-gateway/docker-compose.yml -p "${ZONE_ID}-gateway" up -d

# 5. Registrar zona en el Main Gateway
echo "📝 Registrando zona $ZONE_ID en el Gateway Principal..."
MAIN_CONF_DIR="../main-gateway/confs"
mkdir -p "$MAIN_CONF_DIR"

cat <<EOF > "${MAIN_CONF_DIR}/${ZONE_ID}.conf"
location /${ZONE_ID}/ {
    proxy_pass http://${ZONE_ID}-api-gateway:80/;
}
EOF

# Recargar Nginx Principal si está corriendo
docker exec main-api-gateway nginx -s reload 2>/dev/null || echo "⚠️ Main Gateway no detectado, asegúrate de levantarlo en la raíz."

# 6. Creando aplicación Spark Structured Streaming
echo "Levantando Spark Streaming..."
docker compose -f spark-streaming-app/docker-compose.yml -p "${ZONE_ID}-spark" up -d --build

echo "✅ Zona $ZONE_ID desplegada correctamente."