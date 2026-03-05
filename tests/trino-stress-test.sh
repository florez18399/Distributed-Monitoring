#!/bin/bash

# --- Configuración ---
TRINO_CONTAINER="trino"
COLOR_RESET="\033[0m"
COLOR_INFO="\033[34m"
COLOR_SUCCESS="\033[32m"
COLOR_WARNING="\033[33m"
COLOR_ERROR="\033[31m"

echo -e "${COLOR_INFO}--- Iniciando Pruebas de Estrés en Trino (TPCH) ---${COLOR_RESET}"

# 1. Verificar si el catálogo TPCH existe
echo -e "${COLOR_INFO}[1/3] Verificando catálogo TPCH...${COLOR_RESET}"
TPCH_EXISTS=$(docker exec $TRINO_CONTAINER trino --execute "SHOW CATALOGS" | grep -w "tpch" || true)

if [ -z "$TPCH_EXISTS" ]; then
    echo -e "${COLOR_WARNING}Advertencia: El catálogo 'tpch' no está configurado.${COLOR_RESET}"
    echo "Agregando tpch.properties automáticamente..."
    
    # Intentar crearlo en el contenedor (volátil) si no está
    docker exec $TRINO_CONTAINER bash -c "echo 'connector.name=tpch' > /etc/trino/catalog/tpch.properties"
    docker restart $TRINO_CONTAINER
    echo "Reiniciando Trino para cargar catálogo... esperando 30s."
    sleep 30
else
    echo -e "${COLOR_SUCCESS}Catálogo TPCH detectado correctamente.${COLOR_RESET}"
fi

# 2. Prueba de Carga Moderada (SF1 - ~1.5M registros)
echo -e "${COLOR_INFO}[2/3] Ejecutando consulta de carga moderada (TPCH SF1)...${COLOR_RESET}"
echo "Calculando agregaciones sobre lineitem (sf1)..."

docker exec $TRINO_CONTAINER trino --execute "
    SELECT 
        l.returnflag, 
        l.linestatus, 
        count(*) AS count_order,
        sum(l.quantity) AS sum_qty, 
        avg(l.extendedprice) AS avg_price 
    FROM tpch.sf1.lineitem l 
    GROUP BY 1, 2 
    ORDER BY 1, 2"

if [ $? -eq 0 ]; then
    echo -e "${COLOR_SUCCESS}Consulta SF1 completada con éxito.${COLOR_RESET}"
else
    echo -e "${COLOR_ERROR}Error en consulta SF1.${COLOR_RESET}"
fi

# 3. Prueba de Estrés Pesado (SF10 - ~60M registros)
echo -e "${COLOR_INFO}[3/3] Ejecutando consulta de estrés pesado (TPCH SF10)...${COLOR_RESET}"
echo "Realizando JOIN complejo de 3 tablas (60M registros en memoria)..."
echo -e "${COLOR_WARNING}Nota: Se espera que esta consulta pueda exceder los límites de memoria si están ajustados.${COLOR_RESET}"

docker exec $TRINO_CONTAINER trino --execute "
    SELECT 
        l.orderkey, 
        sum(l.extendedprice * (1 - l.discount)) AS revenue, 
        o.orderdate, 
        o.shippriority 
    FROM tpch.sf10.customer c, 
         tpch.sf10.orders o, 
         tpch.sf10.lineitem l 
    WHERE c.mktsegment = 'BUILDING' 
      AND c.custkey = o.custkey 
      AND l.orderkey = o.orderkey 
      AND o.orderdate < DATE '1995-03-15' 
      AND l.shipdate > DATE '1995-03-15' 
    GROUP BY 1, 3, 4 
    ORDER BY 2 DESC, 3 
    LIMIT 10"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${COLOR_SUCCESS}Consulta SF10 completada (Trino tiene suficiente RAM).${COLOR_RESET}"
else
    echo -e "${COLOR_WARNING}La consulta SF10 falló o excedió los límites (Comportamiento esperado en estrés).${COLOR_RESET}"
fi

echo -e "${COLOR_INFO}--- Pruebas Finalizadas ---${COLOR_RESET}"
