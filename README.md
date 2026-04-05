# Pipeline de Analisis de Trazas Distribuido con Big Data

Este proyecto implementa un pipeline de datos de extremo a extremo para la recoleccion, procesamiento, almacenamiento y consulta en tiempo real de trazas de solicitudes en un sistema distribuido multi-zona. La arquitectura esta completamente contenerizada utilizando Docker y Docker Compose, demostrando la integracion de tecnologias lideres en el ecosistema de Big Data.

## Arquitectura General

El sistema se organiza en **zonas de disponibilidad** independientes, cada una con su propia malla de servicios, pipeline de datos y almacenamiento local. Una capa de infraestructura compartida (HDFS, Trino, Redis Global) unifica la consulta y el monitoreo.

Ver diagramas detallados en [`Arch_Diagram.md`](Arch_Diagram.md).

### Flujo de Datos

1. **Generacion de Trafico** (`tests/mock-client`): Un generador composable con 3 modos (personas, temporal, anomalias) envia requests al sistema a traves del main gateway.
2. **Enrutamiento** (`main-gateway` + `zone-gateway`): nginx enruta por zona y por app hacia los proxies Envoy.
3. **Patron Sidecar** (`server-mesh`): Cada app corre junto a un proxy Envoy (ingress/egress) y un colector Fluent Bit. Envoy intercepta el trafico, inyecta headers de trazabilidad (`X-CONSUMER-ID`, `x-resource-id`) via filtros Lua, y genera access logs en JSON.
4. **Recoleccion de Logs** (`Fluent Bit`): Consume los access logs de Envoy y los envia al broker Kafka de la zona.
5. **Broker de Mensajeria** (`Kafka KRaft`): Un broker por zona recibe las trazas en el topico `envoy-logs`. Desacopla productores de consumidores.
6. **Procesamiento en Streaming** (`Spark Structured Streaming`): Se suscribe al topico Kafka, procesa en micro-lotes y escribe en dos destinos:
   - **HDFS**: Archivos Parquet particionados por `zona/year/month/day/hour`
   - **Redis Global**: Streams en tiempo real (`trazas:stream:zona-N`)
7. **Almacenamiento Distribuido** (`Hadoop HDFS`): NameNode centralizado + un DataNode por zona, con topologia rack-aware.
8. **Consulta Federada** (`Trino` + `Hive Metastore`): Motor SQL distribuido sobre los datos en HDFS. Un sincronizador de particiones mantiene los metadatos actualizados.
9. **Monitoreo** (`Grafana`): Dashboards en tiempo real conectados a Redis Streams y Trino.

## Aplicaciones y Escenarios

Las aplicaciones se definen de forma declarativa en **manifiestos JSON** (`zona-deploy/zones/zone*.json`). Cada manifiesto especifica las apps de la zona, su escenario de comportamiento y sus llamadas downstream:

```json
{
  "apps": [
    {
      "name": "catalog-api",
      "scenario": "NORMAL",
      "downstream_calls": [
        { "target": "orders-api", "path": "/orders-api/checkout", "probability": 0.4 }
      ]
    }
  ]
}
```

### Escenarios de Comportamiento

Cada app se despliega con un modo `SCENARIO` que simula condiciones operativas distintas:

| Escenario | Comportamiento |
|-----------|---------------|
| `NORMAL` | Latencia estandar (100-300ms), tasa de error baja (5%) |
| `DEGRADED` | Alta latencia (+3s por solicitud) |
| `CHAOS` | 50% de probabilidad de error 500, latencia minima |
| `BURSTY` | Picos intermitentes de latencia (10% chance de +5s) |

### Llamadas Inter-Servicio

Las apps realizan llamadas downstream a traves del proxy Envoy egress (puerto 9001), que inyecta automaticamente el header `X-CONSUMER-ID` con el nombre del servicio origen. Esto permite trazar cadenas de llamadas completas (ej. `catalog-api -> orders-api -> payments-api -> auth-api`).

## Generador de Trafico Composable

El componente `tests/mock-client` implementa un generador de trafico con 3 modos independientes que pueden activarse en cualquier combinacion:

### Modo 1: Personas (State Machine)

Simula usuarios con comportamiento realista mediante maquinas de estados:

```
IDLE -> BROWSING -> SELECTING -> CHECKOUT -> VALIDATING -> DONE
```

| Tipo | Distribucion | Patron |
|------|-------------|--------|
| `casual` | 50% | Navega mucho, rara vez compra |
| `power` | 20% | Va directo al checkout |
| `window` | 30% | Navega bastante, casi nunca compra |

Cada persona elige una zona al azar, genera un `X-CONSUMER-ID` unico (`user-casual-a1b2c3d4`) y transiciona entre estados con probabilidades configurables.

### Modo 2: Temporal

Reloj simulado acelerado que modula el volumen de trafico siguiendo una curva diaria:

- **Noche** (00-05h): 10% del trafico
- **Manana** (05-12h): Rampa de 10% a 100%
- **Pico** (12-13h): 100% del trafico
- **Tarde** (13-20h): 80% declinando a 30%
- **Noche** (20-24h): 30% a 10%

Soporta eventos overlay como `flash_sale` (spike 10x) y `maintenance` (trafico ~0).

### Modo 3: Anomalias

Perfiles de estres dirigidos que corren de forma independiente:

| Perfil | Descripcion |
|--------|------------|
| `cascade_failure` | Inunda un app para provocar fallas en cadena |
| `zone_brownout` | Rampa gradual de requests a una zona |
| `scanner_attack` | Probes rapidos a rutas inexistentes (404s) |
| `ddos` | Rafaga masiva a un endpoint especifico |

### Configuracion

```bash
# Iniciar con personas y patron temporal
cd tests/mock-client
MODES=personas,temporal docker compose up -d --build

# Con anomalias configuradas
ANOMALY_PROFILES='[{"type":"cascade_failure","target_zone":"zone1","target_app":"catalog-api","concurrency":50,"duration_seconds":120}]' \
MODES=personas,temporal,anomaly docker compose up -d --build

# Detener
docker compose down
```

Variables de entorno disponibles:

| Variable | Default | Descripcion |
|----------|---------|-------------|
| `MODES` | `personas,temporal` | Modos activos (comma-separated) |
| `PERSONA_POOL_SIZE` | `10` | Pool base de personas concurrentes |
| `PERSONA_MIX` | `casual:0.5,power:0.2,window:0.3` | Distribucion de tipos |
| `REQUEST_INTERVAL` | `0.5` | Intervalo base entre requests (s) |
| `TIME_ACCELERATION` | `300` | Segundos reales por hora simulada |
| `TEMPORAL_EVENTS` | `[]` | JSON de eventos overlay |
| `ANOMALY_PROFILES` | `[]` | JSON de perfiles de anomalia |

## Estructura del Proyecto

```
DockerSidecars/
├── hadoop-cluster/          # HDFS NameNode
│   └── hadoop-image/        # Imagen base Hadoop 3.2.1
├── hadoop-config/           # topology-mapping.csv, scripts
├── hive-config/             # Configuracion de Hive Metastore
├── main-gateway/            # nginx principal (puerto 81)
│   └── confs/               # Configs de zona generadas dinamicamente
├── monitoring/              # Grafana OSS + datasources
│   └── grafana/
├── redis-streaming/         # Redis Global (Streams tiempo real)
├── compactor/               # Compactador de small files HDFS (cron cada 6h)
│   ├── compactor.py         # Spark job: consolida parquets por particion
│   ├── Dockerfile           # PySpark + cron
│   └── docker-compose.yml
├── sql/                     # Scripts SQL de creacion de tablas
├── tracer_api/              # Flask API para consulta de trazas
│   └── app/
├── trino-sql/               # Trino Coordinator + Workers + Hive
│   ├── trino-config/
│   └── synchronizer/        # Sincronizador de particiones
├── tests/
│   └── mock-client/         # Generador de trafico composable
├── zona-deploy/             # Orquestacion de zonas
│   ├── zones/               # Manifiestos JSON (zone1.json, ...)
│   ├── server-mesh/         # Patron sidecar (App + Envoy + FluentBit)
│   │   ├── apps/generic-app/  # App Node.js generica
│   │   └── commons/           # envoy.yaml.template, fluent-bit.conf
│   ├── streaming-kafka/     # Kafka KRaft por zona
│   ├── spark-streaming-app/ # Spark Structured Streaming
│   ├── redis-streaming/     # Redis por zona
│   ├── datanode/            # DataNode HDFS por zona
│   ├── zone-gateway/        # nginx por zona (generado)
│   ├── deploy-zone.sh       # Despliegue declarativo por manifiesto
│   └── destroy-zone.sh      # Destruccion limpia con cleanup
├── recover.sh               # Recuperacion automatica post-crash del host
├── queries.txt              # Queries Grafana optimizadas con partition pruning
└── Arch_Diagram.md          # Diagramas de arquitectura (Mermaid)
```

## Prerrequisitos

- Docker y Docker Compose
- `jq` (para parseo de manifiestos JSON)
- `envsubst` (para renderizado de templates)
- Recursos: se recomiendan +8GB de RAM asignados a Docker

## Como Empezar

### Paso 1: Levantar Infraestructura Base

```bash
# HDFS NameNode
cd hadoop-cluster && docker compose up -d && cd ..

# Main Gateway
cd main-gateway && docker compose up -d && cd ..

# Redis Global
cd redis-streaming && docker compose up -d && cd ..

# Trino + Hive Metastore
cd trino-sql && docker compose up -d && cd ..

# Monitoring (Grafana)
cd monitoring && docker compose up -d && cd ..
```

### Paso 2: Desplegar Zonas con Manifiestos

Cada zona se despliega de forma declarativa pasando un manifiesto JSON:

```bash
cd zona-deploy/

# Zona 1: catalog(NORMAL) -> orders(DEGRADED) -> payments(CHAOS) -> auth(BURSTY)
./deploy-zone.sh zone1 zones/zone1.json

# Zona 2: escenarios distintos
./deploy-zone.sh zone2 zones/zone2.json

# Zona 3
./deploy-zone.sh zone3 zones/zone3.json
```

El script `deploy-zone.sh` automaticamente:
1. Crea la red de zona (`zone_N_net`)
2. Registra el DataNode en la topologia HDFS
3. Despliega Kafka, Redis y DataNode
4. Renderiza la configuracion de Envoy desde el template
5. Despliega las apps definidas en el manifiesto con sus sidecars
6. Genera y levanta el zone gateway (nginx)
7. Registra la zona en el main gateway
8. Inicia Spark Structured Streaming

### Paso 3: Iniciar Generador de Trafico

```bash
cd tests/mock-client
docker compose up -d --build
```

### Paso 4: Consultar Datos

```bash
# Via Trino (datos historicos en HDFS)
docker exec -it trino trino --execute \
  "SELECT id_consumer, count(*) FROM hive.default.trazas_logs_v5 GROUP BY id_consumer ORDER BY 2 DESC LIMIT 10"

# Via Grafana (tiempo real)
# Acceder a http://localhost:3000 (admin/admin)
```

## Detener el Entorno

```bash
# Detener trafico
cd tests/mock-client && docker compose down

# Destruir una zona
cd zona-deploy/
./destroy-zone.sh zone1

# El script limpia: containers, redes, checkpoints HDFS, topologia
```

## Compactador HDFS

Spark Structured Streaming genera un archivo parquet por micro-batch (cada 2 minutos), lo que produce miles de small files (~6KB cada uno). El compactador consolida estos archivos en uno solo por particion hora, reduciendo el overhead de I/O en Trino.

```bash
# El compactador corre automaticamente cada 6 horas via cron
cd compactor && docker compose up -d

# Ejecucion manual
docker exec hdfs-compactor-cron spark-submit --master 'local[*]' /app/compactor.py --recent 6

# Opciones disponibles
spark-submit compactor.py                    # Compactar todas las particiones
spark-submit compactor.py --date 2026-04-02  # Compactar un dia especifico
spark-submit compactor.py --yesterday        # Compactar el dia anterior
spark-submit compactor.py --recent 6         # Compactar las ultimas 6 horas (excluyendo la actual)
```

El compactor tambien realiza backfill de campos derivados (`consumer_type`, `source_zone`, `source_service`) para trazas legacy que no los tengan.

## Recuperacion Post-Crash

El script `recover.sh` automatiza la recuperacion del sistema tras un apagado abrupto del host:

```bash
./recover.sh
```

Fases de recuperacion:
1. Reinicia todos los contenedores detenidos
2. Espera a que el NameNode responda
3. Desactiva Safe Mode de HDFS
4. Valida y re-registra DataNodes (deteccion dinamica de zonas)
5. Detecta y elimina bloques corruptos (checkpoints y datos)
6. Reinicia Spark processors (para recrear checkpoints)
7. Reinicia y valida Trino (sincroniza metadata Hive, ejecuta query de validacion)
8. Recarga nginx del main gateway

## Optimizaciones de Rendimiento

### Trino
- `optimize_hash_generation=true`: precomputo de hashes para distribución y agregaciones
- Metadata cache de Hive Metastore (TTL 10m, refresh 5m)
- Parquet column name matching habilitado

### Queries Grafana
Las consultas en `queries.txt` incluyen un filtro de particion que habilita partition pruning en Trino:
```sql
WHERE year * 10000 + month * 100 + day
      BETWEEN ${__from:date:YYYY} * 10000 + ${__from:date:M} * 100 + ${__from:date:D}
          AND ${__to:date:YYYY} * 10000 + ${__to:date:M} * 100 + ${__to:date:D}
  AND $__timeFilter(event_timestamp)
```
Esto permite a Trino descartar particiones fuera del rango temporal sin abrir archivos.

### Spark Streaming
Los streams HDFS y Redis se monitorean de forma independiente: si el stream de Redis falla, solo se reinicia Redis sin afectar la escritura a HDFS, evitando la generacion excesiva de small files.

## Redes

| Red | Tipo | Proposito |
|-----|------|-----------|
| `hdfs-backbone` | Bridge, persistente | Conecta NameNode, DataNodes, Spark, Trino, Redis Global |
| `main_gateway_net` | Bridge, persistente | Conecta main gateway, zone gateways, traffic generator |
| `zone_N_net` | Bridge, por zona | Conecta apps/envoy, Kafka, Spark, DataNode de cada zona |
| `{zone}-{app}_internal_net` | Bridge, por app | Aislamiento interno: App + Envoy + FluentBit (pod) |
