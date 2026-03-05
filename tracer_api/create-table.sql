CREATE TABLE hive.default.trazas_logs (
    -- Columnas de datos (las que ya tenías)
    date_transaction VARCHAR,
    endpoint VARCHAR,
    id_consumer VARCHAR,
    id_recurso VARCHAR,
    id_transaccion VARCHAR,
    ip_consumer VARCHAR,
    ip_transaccion VARCHAR,
    req_body_size BIGINT,
    resp_body_size BIGINT,
    status_response INTEGER,
    time_transaction BIGINT,
    tipo_operacion VARCHAR,
    event_timestamp TIMESTAMP(3),

    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER,
    zona VARCHAR
)
WITH (
      format = 'PARQUET',
      external_location = 'hdfs://namenode:9000/data/trazas_parquet',
      partitioned_by = ARRAY['year', 'month', 'day', 'hour', 'zona']
);


-- Conectar a Hive/Trino y ejecutar:
CREATE DATABASE IF NOT EXISTS system_distributed_traces;

CREATE EXTERNAL TABLE IF NOT EXISTS system_distributed_traces.transaction_logs (
    date_transaction VARCHAR,
    endpoint VARCHAR,
    id_consumer VARCHAR,
    id_recurso VARCHAR,
    id_transaccion VARCHAR,
    ip_consumer VARCHAR,
    ip_transaccion VARCHAR,
    req_body_size BIGINT,
    resp_body_size BIGINT,
    status_response INTEGER,
    time_transaction BIGINT,
    tipo_operacion VARCHAR,
    event_timestamp TIMESTAMP(3),

    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER,
    zona VARCHAR
)
PARTITIONED BY (year INT, month INT, day INT, hour INT, zona STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/trazas_parquet'
TBLPROPERTIES (
    'EXTERNAL'='TRUE',
    'skip.header.line.count'='1',
    'parquet.compression'='SNAPPY'
);