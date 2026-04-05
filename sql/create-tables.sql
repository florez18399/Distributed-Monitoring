-- ============================================================
-- Tabla principal de trazas - Sistema Distribuido por Zonas
-- ============================================================
-- Ejecutar en Trino contra el catálogo Hive:
--   trino --execute "$(cat create-tables.sql)"
--
-- Prerequisitos:
--   - Hive Metastore corriendo y accesible
--   - HDFS con namenode accesible en hdfs://namenode:9000
--   - Directorio /data/trazas_v5 existente en HDFS
-- ============================================================

CREATE TABLE IF NOT EXISTS hive.default.trazas_logs_v5 (
   date_transaction varchar,
   endpoint varchar,
   id_consumer varchar,
   id_recurso varchar,
   id_transaccion varchar,
   ip_consumer varchar,
   ip_transaccion varchar,
   req_body_size bigint,
   resp_body_size bigint,
   status_response integer,
   time_transaction bigint,
   tipo_operacion varchar,
   query_params varchar,
   event_timestamp timestamp(3),
   consumer_type varchar,
   source_zone varchar,
   source_service varchar,
   zona varchar,
   year integer,
   month integer,
   day integer,
   hour integer
)
WITH (
   external_location = 'hdfs://namenode:9000/data/trazas_v5',
   format = 'PARQUET',
   partitioned_by = ARRAY['zona','year','month','day','hour']
);

-- Sincronizar particiones existentes en HDFS
CALL system.sync_partition_metadata('default', 'trazas_logs_v5', 'FULL');
