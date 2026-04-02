# Diagramas de Arquitectura

## Diagrama General del Sistema

```mermaid
graph TB
    subgraph CLIENT["CLIENTES"]
        TG["traffic_generator.py<br/>3 modos composables:<br/>Personas · Temporal · Anomalias"]
    end

    subgraph GATEWAY_MAIN["MAIN GATEWAY · nginx:81"]
        MG["main-api-gateway<br/>confs/zone1.conf<br/>confs/zone2.conf<br/>confs/zone3.conf"]
    end

    subgraph ZONE["ZONA-N (replicado x3)"]
        subgraph ZGW["Zone Gateway · nginx:80"]
            ZG["zone-gateway<br/>/catalog-api → envoy<br/>/orders-api → envoy<br/>/payments-api → envoy<br/>/auth-api → envoy"]
        end

        subgraph POD1["Pod catalog-api"]
            APP1["Node.js Express<br/>generic-app<br/>SCENARIO: variable"]
            ENV1["Envoy v1.35.3<br/>Lua: X-CONSUMER-ID<br/>ingress:8080 · egress:9001"]
            FB1["Fluent Bit 2.2<br/>tail access.log<br/>→ kafka: envoy-logs"]
            APP1 <-->|"pod_net :8080"| ENV1
            ENV1 -->|"shared-logs vol"| FB1
        end

        subgraph POD2["Pod orders-api"]
            APP2["Node.js Express"]
            ENV2["Envoy v1.35.3"]
            FB2["Fluent Bit 2.2"]
            APP2 <-->|"pod_net"| ENV2
            ENV2 -->|"shared-logs"| FB2
        end

        subgraph POD3["Pod payments-api"]
            APP3["Node.js Express"]
            ENV3["Envoy v1.35.3"]
            FB3["Fluent Bit 2.2"]
            APP3 <-->|"pod_net"| ENV3
            ENV3 -->|"shared-logs"| FB3
        end

        subgraph POD4["Pod auth-api"]
            APP4["Node.js Express"]
            ENV4["Envoy v1.35.3"]
            FB4["Fluent Bit 2.2"]
            APP4 <-->|"pod_net"| ENV4
            ENV4 -->|"shared-logs"| FB4
        end

        subgraph KAFKA_Z["Kafka KRaft"]
            KBR["broker:29092<br/>topic: envoy-logs<br/>retencion 2h / 500MB"]
        end

        subgraph SPARK_Z["Spark Structured Streaming"]
            SPK["stream-writer.py<br/>local[*] mode<br/>startingOffsets=latest"]
        end

        subgraph REDIS_Z["Redis local"]
            RLZ["redis-realtime<br/>512MB LRU"]
        end

        DN["DataNode<br/>hadoop-base:3.2.1<br/>rack: /rack/zona-N"]
    end

    subgraph HDFS_CORE["HDFS CORE"]
        NN["NameNode<br/>hadoop-base:3.2.1<br/>:9000 / :9870<br/>topology.sh + mapping.csv"]
    end

    subgraph TRINO_STACK["TRINO STACK"]
        subgraph META["Hive Metastore"]
            PG["PostgreSQL 14<br/>metastore-db"]
            HM["Apache Hive 4.1.0<br/>thrift:9083"]
            PG <-->|"JDBC"| HM
        end

        TC["Trino Coordinator 448<br/>:8080 · 4GB query mem<br/>catalog: hive, tpch"]
        TW1["Trino Worker 1<br/>1.2GB/node"]
        TW2["Trino Worker 2<br/>1.2GB/node"]
        SYN["synchronizer.sh<br/>sync_partition_metadata<br/>cada 60s"]

        TC <-->|"discovery"| TW1
        TC <-->|"discovery"| TW2
        HM <-->|"thrift:9083"| TC
        SYN -->|"POST /v1/statement"| TC
    end

    subgraph REDIS_GLOBAL["REDIS GLOBAL"]
        RG["global-redis:6379<br/>Redis Streams<br/>retencion 15 min<br/>trazas:stream:zona-N"]
    end

    subgraph API["TRACER API"]
        FA["Flask 3 + Gunicorn<br/>4 workers · :5000<br/>GET /api/v1/traces<br/>POST /api/v1/query"]
    end

    subgraph MONITORING["MONITORING"]
        GF["Grafana OSS<br/>:3000<br/>plugins: trino + redis"]
    end

    %% Flujo principal
    TG -->|"HTTP<br/>X-CONSUMER-ID<br/>user-casual-xxxx"| MG
    MG -->|"proxy /zoneN/"| ZG

    ZG --> ENV1
    ZG --> ENV2
    ZG --> ENV3
    ZG --> ENV4

    %% Egress: apps llaman downstream via envoy
    ENV1 -.->|"egress:9001<br/>downstream calls"| ZG
    ENV2 -.->|"egress:9001"| ZG
    ENV3 -.->|"egress:9001"| ZG

    FB1 -->|"kafka producer"| KBR
    FB2 -->|"kafka producer"| KBR
    FB3 -->|"kafka producer"| KBR
    FB4 -->|"kafka producer"| KBR

    KBR -->|"subscribe envoy-logs"| SPK

    SPK -->|"foreachBatch 2min<br/>Parquet particionado<br/>zona/year/month/day/hour"| NN
    SPK -->|"foreachPartition 5s<br/>xadd + minid trim"| RG

    NN <-->|"bloque replicado<br/>hdfs-backbone"| DN

    NN -->|"hdfs://namenode:9000"| TC
    HM -->|"partition detect 60s"| PG

    TC -->|"SELECT trazas_logs_v5"| FA
    RG -->|"redis-datasource"| GF
    TC -->|"trino-datasource"| GF
    FA -->|"REST JSON"| GF

    %% Cross-zone
    ZG -.->|"/_external/ → main-gateway"| MG
```

---

## Diagrama: Aplicacion con Sidecar (Pod)

Detalle del patron sidecar implementado para cada aplicacion dentro de una zona.

```mermaid
graph LR
    subgraph EXTERNAL["zone_N_net"]
        ZGW["Zone Gateway<br/>nginx:80"]
        KAFKA["Kafka Broker<br/>:29092"]
    end

    subgraph POD["Pod: {zone}-{app}"]
        subgraph APP_CONTAINER["Contenedor App"]
            APP["Node.js Express<br/>generic-app<br/>:8080"]
            APP_ENV["ENV:<br/>APP_NAME · ZONE_ID<br/>SCENARIO · DOWNSTREAM_CALLS<br/>EGRESS_HOST=envoy<br/>EGRESS_PORT=9001"]
        end

        subgraph ENVOY_CONTAINER["Contenedor Envoy v1.35.3"]
            direction TB
            INGRESS["Listener Ingress<br/>:8080<br/>Lua: x-resource-id"]
            EGRESS["Listener Egress<br/>:9001<br/>Lua: X-CONSUMER-ID"]
            ADMIN["Admin :9901"]
            ACCESS_LOG["access.log<br/>JSON format"]
        end

        subgraph FB_CONTAINER["Contenedor Fluent Bit 2.2"]
            FB["tail access.log<br/>parser json<br/>kafka output"]
        end

        %% Flujo ingress
        INGRESS -->|"route → app:8080"| APP

        %% Flujo egress
        APP -->|"downstream call<br/>envoy:9001"| EGRESS

        %% Logs
        INGRESS --> ACCESS_LOG
        ACCESS_LOG -->|"shared-logs<br/>volume"| FB
    end

    %% Redes
    ZGW -->|"zone_net<br/>:8080"| INGRESS
    EGRESS -->|"zone_net<br/>→ zone-gateway:80"| ZGW
    FB -->|"zone_net<br/>broker:29092"| KAFKA

    %% Red interna
    style POD fill:#1a1a2e,color:#eee
    style APP_CONTAINER fill:#16213e,color:#eee
    style ENVOY_CONTAINER fill:#0f3460,color:#eee
    style FB_CONTAINER fill:#533483,color:#eee
```

### Campos del Access Log (JSON)

| Campo | Valor Envoy | Ejemplo |
|-------|------------|---------|
| `id_recurso` | `%REQ(x-resource-id)%` | `catalog-api-1` |
| `id_transaccion` | `%REQ(X-REQUEST-ID)%` | `uuid-generado` |
| `id_consumer` | `%REQ(X-CONSUMER-ID)%` | `user-casual-a1b2c3d4` |
| `ip_transaccion` | `%UPSTREAM_HOST%` | `172.18.0.5:8080` |
| `ip_consumer` | `%DOWNSTREAM_REMOTE_ADDRESS%` | `172.18.0.2:54321` |
| `endpoint` | `%REQ(:PATH)%` | `/catalog-api/products` |
| `status_response` | `%RESPONSE_CODE%` | `200` |
| `time_transaction` | `%DURATION%` | `142` |

---

## Diagrama: Zona Completa

Una zona de disponibilidad con sus 4 apps, infraestructura de datos y conectividad.

```mermaid
graph TB
    MG["Main Gateway<br/>nginx:81<br/>(main_gateway_net)"]

    subgraph ZONE["Zona N · zone_N_net"]
        subgraph ZGW["Zone Gateway · nginx:80"]
            ZG["Rutas generadas:<br/>/catalog-api → envoy<br/>/orders-api → envoy<br/>/payments-api → envoy<br/>/auth-api → envoy<br/>/_external/ → main-gateway"]
        end

        subgraph APPS["Malla de Servicios (4 pods)"]
            subgraph P1["catalog-api"]
                A1["App"] --- E1["Envoy"] --- F1["FluentBit"]
            end
            subgraph P2["orders-api"]
                A2["App"] --- E2["Envoy"] --- F2["FluentBit"]
            end
            subgraph P3["payments-api"]
                A3["App"] --- E3["Envoy"] --- F3["FluentBit"]
            end
            subgraph P4["auth-api"]
                A4["App"] --- E4["Envoy"] --- F4["FluentBit"]
            end
        end

        subgraph DATA_PLANE["Pipeline de Datos"]
            KAFKA["Kafka KRaft<br/>broker:29092<br/>topic: envoy-logs"]
            SPARK["Spark Streaming<br/>stream-writer.py"]
        end

        subgraph LOCAL_INFRA["Infra Local"]
            REDIS_L["Redis Local<br/>512MB LRU"]
            DN["DataNode HDFS<br/>rack: /rack/zona-N"]
        end
    end

    subgraph SHARED["Infraestructura Compartida · hdfs-backbone"]
        NN["NameNode HDFS<br/>:9000 / :9870"]
        RG["Redis Global<br/>:6379 Streams"]
    end

    %% Flujo de requests
    MG -->|"/zoneN/"| ZG
    ZG --> E1 & E2 & E3 & E4

    %% Cadena de llamadas downstream
    E1 -.->|"egress → orders-api<br/>prob: 0.4"| ZG
    E2 -.->|"egress → payments-api<br/>prob: 0.5"| ZG
    E3 -.->|"egress → auth-api<br/>prob: 0.8"| ZG

    %% Pipeline de datos
    F1 & F2 & F3 & F4 --> KAFKA
    KAFKA --> SPARK
    SPARK -->|"Parquet<br/>2min batch"| NN
    SPARK -->|"xadd<br/>5s batch"| RG

    %% HDFS replicacion
    NN <-->|"bloques"| DN

    %% Cross-zone
    ZG -.->|"/_external/"| MG

    style ZONE fill:#1a1a2e,color:#eee
    style APPS fill:#16213e,color:#eee
    style DATA_PLANE fill:#0f3460,color:#eee
    style LOCAL_INFRA fill:#533483,color:#eee
```

### Despliegue de una Zona

```bash
cd zona-deploy/
./deploy-zone.sh zone1 zones/zone1.json
```

Componentes desplegados por `deploy-zone.sh`:

| Paso | Proyecto Docker | Contenedores |
|------|----------------|-------------|
| 1 | `zone1-hdfs` | `zone1-datanode` |
| 2 | `zone1-infra` | `zone1-broker`, `zone1-kafka-consumer` |
| 3 | `zone1-catalog-api` | `zone1-catalog-api-core`, `-envoy`, `-fluentd` |
| 4 | `zone1-orders-api` | `zone1-orders-api-core`, `-envoy`, `-fluentd` |
| 5 | `zone1-payments-api` | `zone1-payments-api-core`, `-envoy`, `-fluentd` |
| 6 | `zone1-auth-api` | `zone1-auth-api-core`, `-envoy`, `-fluentd` |
| 7 | `zone1-gateway` | `zone1-api-gateway` |
| 8 | `zone1-spark` | `zone1-spark-processor` |

**Total por zona: ~17 contenedores**

---

## Diagrama: Trino Stack

Motor de consultas SQL distribuido sobre los datos almacenados en HDFS.

```mermaid
graph TB
    subgraph HDFS["HDFS"]
        NN["NameNode<br/>hdfs://namenode:9000"]
        PARQUET["/user/hive/warehouse/<br/>trazas_logs_v5/<br/>zona=zone1/year=2026/<br/>month=04/day=01/hour=12/<br/>*.parquet"]
        NN --- PARQUET
    end

    subgraph TRINO["TRINO CLUSTER"]
        subgraph COORDINATOR["Coordinator :8080"]
            TC["Trino 448<br/>4GB query memory<br/>catalogs: hive, tpch"]
        end

        subgraph WORKERS["Workers"]
            TW1["Worker 1<br/>1.2GB/node"]
            TW2["Worker 2<br/>1.2GB/node"]
        end

        TC <-->|"discovery.uri"| TW1
        TC <-->|"discovery.uri"| TW2
    end

    subgraph HIVE["HIVE METASTORE"]
        HM["Apache Hive 4.1.0<br/>thrift:9083<br/>init-hive.sh"]
        PG["PostgreSQL 14<br/>metastore-db<br/>puerto 5432"]
        HM <-->|"JDBC"| PG
    end

    subgraph SYNC["SINCRONIZADOR"]
        SYN["synchronizer.sh<br/>loop cada 60s<br/>CALL system.sync_partition_metadata<br/>schema='default',<br/>table='trazas_logs_v5',<br/>mode='FULL'"]
    end

    subgraph CONSUMERS["CONSUMIDORES"]
        FA["Tracer API<br/>Flask :5000"]
        GF["Grafana<br/>trino-datasource"]
    end

    %% Conexiones
    NN -->|"hive.properties<br/>hive.metastore.uri"| TC
    HM -->|"thrift:9083<br/>schema + particiones"| TC

    SYN -->|"POST /v1/statement<br/>cada 60s"| TC

    TC --> FA
    TC --> GF

    style SYNC fill:#ff9900,color:#000
    style COORDINATOR fill:#0f3460,color:#eee
    style WORKERS fill:#16213e,color:#eee
```

### Consultas de Ejemplo

```sql
-- Trazas por consumidor (personas del traffic generator)
SELECT id_consumer, count(*) as total, avg(time_transaction) as avg_latency
FROM hive.default.trazas_logs_v5
WHERE zona = 'zone1'
GROUP BY id_consumer
ORDER BY total DESC;

-- Cadena de llamadas: ver downstream
SELECT id_transaccion, id_recurso, id_consumer, endpoint, status_response
FROM hive.default.trazas_logs_v5
WHERE id_transaccion = '<uuid>'
ORDER BY date_transaction;

-- Errores por escenario/zona
SELECT zona, status_response, count(*) as total
FROM hive.default.trazas_logs_v5
GROUP BY zona, status_response;
```

---

## Diagrama: Sistema de Almacenamiento

Flujo de datos desde la generacion hasta el almacenamiento persistente y en tiempo real.

```mermaid
graph LR
    subgraph SOURCES["Fuentes de Datos (por zona)"]
        FB1["FluentBit<br/>zona 1"]
        FB2["FluentBit<br/>zona 2"]
        FB3["FluentBit<br/>zona 3"]
    end

    subgraph KAFKA["Kafka KRaft (por zona)"]
        K1["zone1-broker<br/>:29092"]
        K2["zone2-broker<br/>:29092"]
        K3["zone3-broker<br/>:29092"]
    end

    subgraph SPARK["Spark Streaming (por zona)"]
        S1["zone1-spark-processor"]
        S2["zone2-spark-processor"]
        S3["zone3-spark-processor"]
    end

    subgraph HDFS["HDFS · hdfs-backbone"]
        NN["NameNode<br/>:9000 / :9870"]
        DN1["DataNode zona1<br/>/rack/zone1"]
        DN2["DataNode zona2<br/>/rack/zone2"]
        DN3["DataNode zona3<br/>/rack/zone3"]

        NN <-->|"replicacion"| DN1
        NN <-->|"replicacion"| DN2
        NN <-->|"replicacion"| DN3

        PARQUET["Parquet Files<br/>/user/hive/warehouse/trazas_logs_v5/<br/>zona=zoneN/year/month/day/hour/"]
        NN --- PARQUET
    end

    subgraph REDIS["Redis Global · :6379"]
        RS["Redis Streams<br/>trazas:stream:zone1<br/>trazas:stream:zone2<br/>trazas:stream:zone3<br/>retencion: 15 min"]
    end

    subgraph QUERY["Capa de Consulta"]
        TRINO["Trino<br/>SQL historico"]
        GRAFANA["Grafana<br/>tiempo real"]
    end

    %% Flujo
    FB1 --> K1
    FB2 --> K2
    FB3 --> K3

    K1 --> S1
    K2 --> S2
    K3 --> S3

    S1 -->|"batch 2min<br/>Parquet"| NN
    S2 -->|"batch 2min"| NN
    S3 -->|"batch 2min"| NN

    S1 -->|"batch 5s<br/>xadd"| RS
    S2 -->|"batch 5s"| RS
    S3 -->|"batch 5s"| RS

    PARQUET -->|"hive catalog"| TRINO
    RS -->|"redis-datasource"| GRAFANA
    TRINO -->|"trino-datasource"| GRAFANA

    style HDFS fill:#1a1a2e,color:#eee
    style REDIS fill:#cc3300,color:#fff
    style SPARK fill:#cc6600,color:#fff
```

### Particionamiento en HDFS

```
/user/hive/warehouse/trazas_logs_v5/
├── zona=zone1/
│   └── year=2026/
│       └── month=04/
│           └── day=01/
│               ├── hour=08/
│               │   ├── part-00000.snappy.parquet
│               │   └── part-00001.snappy.parquet
│               └── hour=09/
│                   └── part-00000.snappy.parquet
├── zona=zone2/
│   └── ...
└── zona=zone3/
    └── ...
```

### Topologia Rack-Aware

```
/rack/zone1  →  zone1-datanode, zone1-spark-processor
/rack/zone2  →  zone2-datanode, zone2-spark-processor
/rack/zone3  →  zone3-datanode, zone3-spark-processor
```

Configurada en `hadoop-config/topology-mapping.csv` y aplicada via `topology.sh` en el NameNode.
