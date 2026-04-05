#!/bin/bash
# ==============================================================================
# recover.sh - Recuperación del sistema tras apagado abrupto del host
#
# Reinicia servicios, valida HDFS (datanodes, bloques corruptos, safe mode),
# limpia checkpoints corruptos, valida Trino y sincroniza metadata de Hive.
#
# Uso: ./recover.sh
# ==============================================================================
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log()  { echo -e "${CYAN}[$(date '+%H:%M:%S')]${NC} $1"; }
ok()   { echo -e "${GREEN}[OK]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; }

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HDFS_MAX_WAIT=60
TRINO_MAX_WAIT=60

# ==============================================================================
# 1. Reiniciar todos los contenedores detenidos
# ==============================================================================
log "Fase 1: Reiniciando contenedores detenidos..."

STOPPED=$(docker ps -a --filter "status=exited" --filter "status=created" --format "{{.Names}}" 2>/dev/null || true)
if [ -n "$STOPPED" ]; then
    STOPPED_COUNT=$(echo "$STOPPED" | wc -l)
    warn "Encontrados $STOPPED_COUNT contenedores detenidos"
    echo "$STOPPED" | while read -r name; do
        echo "  Reiniciando: $name"
    done
    docker start $STOPPED
    ok "Contenedores reiniciados"
else
    ok "Todos los contenedores ya estaban corriendo"
fi

# ==============================================================================
# 2. Esperar a que el NameNode esté listo
# ==============================================================================
log "Fase 2: Esperando a que el NameNode esté listo..."

elapsed=0
while [ $elapsed -lt $HDFS_MAX_WAIT ]; do
    if docker exec namenode hdfs dfsadmin -report >/dev/null 2>&1; then
        ok "NameNode respondiendo"
        break
    fi
    sleep 2
    elapsed=$((elapsed + 2))
done

if [ $elapsed -ge $HDFS_MAX_WAIT ]; then
    fail "NameNode no respondio en ${HDFS_MAX_WAIT}s. Abortando."
    exit 1
fi

# ==============================================================================
# 3. Salir de Safe Mode
# ==============================================================================
log "Fase 3: Verificando Safe Mode..."

SAFE_MODE=$(docker exec namenode hdfs dfsadmin -safemode get 2>&1 | grep -o "ON\|OFF")
if [ "$SAFE_MODE" = "ON" ]; then
    warn "HDFS en Safe Mode, desactivando..."
    docker exec namenode hdfs dfsadmin -safemode leave >/dev/null 2>&1
    ok "Safe Mode desactivado"
else
    ok "Safe Mode ya estaba desactivado"
fi

# ==============================================================================
# 4. Validar DataNodes - detectar zonas dinámicamente
# ==============================================================================
log "Fase 4: Validando DataNodes..."

# Detectar todas las zonas por contenedores que matcheen *-datanode
ALL_DATANODES=$(docker ps -a --format "{{.Names}}" | grep -E "^zone[0-9]+-datanode$" | sort)
TOTAL_DATANODES=$(echo "$ALL_DATANODES" | wc -l)

LIVE_COUNT=$(docker exec namenode hdfs dfsadmin -report 2>&1 | grep "Live datanodes" | grep -oP '\d+')

log "DataNodes esperados: $TOTAL_DATANODES, registrados en NameNode: $LIVE_COUNT"

if [ "$LIVE_COUNT" -lt "$TOTAL_DATANODES" ]; then
    warn "Faltan $((TOTAL_DATANODES - LIVE_COUNT)) DataNodes. Reiniciando datanodes no registrados..."

    # Obtener datanodes registrados
    REGISTERED=$(docker exec namenode hdfs dfsadmin -report 2>&1 | grep "Hostname:" | awk '{print $2}')

    for dn in $ALL_DATANODES; do
        if ! echo "$REGISTERED" | grep -q "^${dn}$"; then
            warn "  $dn no registrado, reiniciando..."
            docker restart "$dn" >/dev/null 2>&1
        fi
    done

    # Esperar a que se registren
    log "Esperando registro de DataNodes..."
    elapsed=0
    while [ $elapsed -lt $HDFS_MAX_WAIT ]; do
        LIVE_COUNT=$(docker exec namenode hdfs dfsadmin -report 2>&1 | grep "Live datanodes" | grep -oP '\d+')
        if [ "$LIVE_COUNT" -ge "$TOTAL_DATANODES" ]; then
            break
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done

    LIVE_COUNT=$(docker exec namenode hdfs dfsadmin -report 2>&1 | grep "Live datanodes" | grep -oP '\d+')
    if [ "$LIVE_COUNT" -ge "$TOTAL_DATANODES" ]; then
        ok "Todos los DataNodes registrados: $LIVE_COUNT/$TOTAL_DATANODES"
    else
        warn "Solo $LIVE_COUNT/$TOTAL_DATANODES DataNodes registrados tras espera"
    fi
else
    ok "Todos los DataNodes registrados: $LIVE_COUNT/$TOTAL_DATANODES"
fi

# ==============================================================================
# 5. Detectar y limpiar bloques corruptos
# ==============================================================================
log "Fase 5: Buscando bloques corruptos en HDFS..."

CORRUPT_OUTPUT=$(docker exec namenode hdfs fsck / -list-corruptfileblocks 2>&1)
CORRUPT_COUNT=$(echo "$CORRUPT_OUTPUT" | grep -oP '\d+ CORRUPT' | grep -oP '^\d+' || echo "0")

if [ "$CORRUPT_COUNT" -gt 0 ]; then
    warn "Encontrados $CORRUPT_COUNT archivos corruptos"

    # Separar archivos de datos vs checkpoints
    CORRUPT_DATA_FILES=$(echo "$CORRUPT_OUTPUT" | grep -P '^\S+\s+/data/' | awk '{print $2}' || true)
    CORRUPT_CHECKPOINT_FILES=$(echo "$CORRUPT_OUTPUT" | grep -P '^\S+\s+/checkpoints/' | awk '{print $2}' || true)
    CORRUPT_OTHER_FILES=$(echo "$CORRUPT_OUTPUT" | grep -P '^\S+\s+/' | grep -v '/data/' | grep -v '/checkpoints/' | awk '{print $2}' || true)

    # Eliminar checkpoints corruptos (Spark los regenera)
    if [ -n "$CORRUPT_CHECKPOINT_FILES" ]; then
        CP_COUNT=$(echo "$CORRUPT_CHECKPOINT_FILES" | wc -l)
        warn "  Eliminando $CP_COUNT checkpoints corruptos (Spark los recreara)..."
        echo "$CORRUPT_CHECKPOINT_FILES" | while read -r f; do
            docker exec namenode hdfs dfs -rm -f "$f" 2>/dev/null && echo "    Eliminado: $f" || true
        done
    fi

    # Eliminar datos corruptos (irrecuperables sin replicación)
    if [ -n "$CORRUPT_DATA_FILES" ]; then
        DATA_COUNT=$(echo "$CORRUPT_DATA_FILES" | wc -l)
        warn "  Eliminando $DATA_COUNT archivos de datos corruptos (bloques irrecuperables)..."
        echo "$CORRUPT_DATA_FILES" | while read -r f; do
            docker exec namenode hdfs dfs -rm -f "$f" 2>/dev/null && echo "    Eliminado: $f" || true
        done
    fi

    # Otros archivos corruptos
    if [ -n "$CORRUPT_OTHER_FILES" ]; then
        OTHER_COUNT=$(echo "$CORRUPT_OTHER_FILES" | wc -l)
        warn "  Eliminando $OTHER_COUNT otros archivos corruptos..."
        echo "$CORRUPT_OTHER_FILES" | while read -r f; do
            docker exec namenode hdfs dfs -rm -f "$f" 2>/dev/null && echo "    Eliminado: $f" || true
        done
    fi

    # Verificar que no queden corruptos
    REMAINING=$(docker exec namenode hdfs fsck / -list-corruptfileblocks 2>&1 | grep -oP '\d+ CORRUPT' | grep -oP '^\d+' || echo "0")
    if [ "$REMAINING" -eq 0 ]; then
        ok "Todos los bloques corruptos eliminados"
    else
        warn "Aun quedan $REMAINING archivos corruptos. Revision manual recomendada."
    fi
else
    ok "No se encontraron bloques corruptos"
fi

# ==============================================================================
# 6. Reiniciar procesadores Spark (para que recreen checkpoints)
# ==============================================================================
log "Fase 6: Reiniciando Spark processors..."

SPARK_CONTAINERS=$(docker ps -a --format "{{.Names}}" | grep -E "^zone[0-9]+-spark-processor$" | sort || true)
if [ -n "$SPARK_CONTAINERS" ]; then
    docker restart $SPARK_CONTAINERS >/dev/null 2>&1
    ok "Spark processors reiniciados: $(echo "$SPARK_CONTAINERS" | tr '\n' ' ')"
else
    warn "No se encontraron Spark processors"
fi

# ==============================================================================
# 7. Reiniciar y validar Trino
# ==============================================================================
log "Fase 7: Reiniciando Trino..."

docker restart trino trino-worker-1 trino-worker-2 >/dev/null 2>&1

elapsed=0
while [ $elapsed -lt $TRINO_MAX_WAIT ]; do
    if docker exec trino trino --execute "SELECT 1" >/dev/null 2>&1; then
        ok "Trino respondiendo"
        break
    fi
    sleep 3
    elapsed=$((elapsed + 3))
done

if [ $elapsed -ge $TRINO_MAX_WAIT ]; then
    fail "Trino no respondio en ${TRINO_MAX_WAIT}s"
else
    # Sincronizar particiones de Hive
    log "Sincronizando metadata de particiones Hive..."
    docker exec trino trino --execute "CALL system.sync_partition_metadata('default', 'trazas_logs_v5', 'FULL')" >/dev/null 2>&1 && \
        ok "Metadata de particiones sincronizada" || \
        warn "No se pudo sincronizar metadata (tabla puede no existir aun)"

    # Query de validación
    RECORD_COUNT=$(docker exec trino trino --execute "SELECT count(*) FROM hive.default.trazas_logs_v5" 2>&1 | tail -1 | tr -d '"')
    if [[ "$RECORD_COUNT" =~ ^[0-9]+$ ]]; then
        ok "Trino validado: $RECORD_COUNT registros en trazas_logs_v5"
    else
        warn "Query de validación falló. Verificar manualmente."
    fi
fi

# ==============================================================================
# 8. Recargar Nginx (main gateway)
# ==============================================================================
log "Fase 8: Recargando Main Gateway..."
docker exec main-api-gateway nginx -s reload >/dev/null 2>&1 && \
    ok "Nginx recargado" || \
    warn "No se pudo recargar Nginx"

# ==============================================================================
# Resumen final
# ==============================================================================
echo ""
echo "============================================"
log "Recuperacion completada"
echo "============================================"

# Resumen de estado
TOTAL_RUNNING=$(docker ps --format "{{.Names}}" | wc -l)
TOTAL_STOPPED=$(docker ps -a --filter "status=exited" --format "{{.Names}}" | wc -l)
LIVE_DN=$(docker exec namenode hdfs dfsadmin -report 2>&1 | grep "Live datanodes" | grep -oP '\d+')

echo -e "  Contenedores corriendo: ${GREEN}${TOTAL_RUNNING}${NC}"
echo -e "  Contenedores detenidos: $([ "$TOTAL_STOPPED" -eq 0 ] && echo "${GREEN}0${NC}" || echo "${RED}${TOTAL_STOPPED}${NC}")"
echo -e "  DataNodes vivos:        ${GREEN}${LIVE_DN}/${TOTAL_DATANODES}${NC}"
echo -e "  Registros en Trino:     ${GREEN}${RECORD_COUNT:-N/A}${NC}"
echo "============================================"
