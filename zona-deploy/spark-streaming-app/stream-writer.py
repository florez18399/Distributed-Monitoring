import os
import redis
import json
import time
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, hour, lit,
    from_json, to_timestamp, split, when, regexp_extract
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, TimestampType
)

# --- Configuración (ajustada para aislamiento) ---
ZONA_ID = os.environ.get("ZONA_ID", "zona_desconocida")
HDFS_PATH = "hdfs://namenode:9000"
OUTPUT_PATH_V3 = "hdfs://namenode:9000/data/trazas_v5"
CHECKPOINT_PATH_V3 = f"hdfs://namenode:9000/checkpoints/trazas_v5/{ZONA_ID}"
KAFKA_BOOTSTRAP_SERVERS = "broker:29092" 
KAFKA_TOPIC = "envoy-logs"

# Configuración Redis (Real-time) - Ahora Global
REDIS_HOST = "global-redis"  
REDIS_PORT = 6379
REDIS_STREAM_KEY = f"trazas:stream:{ZONA_ID}"
RETENTION_SECONDS = 15 * 60  # 15 minutos

# --- Esquema del Log INTERNO ---
log_schema = StructType([
    StructField("date_transaction", StringType(), True),
    StructField("endpoint", StringType(), True),
    StructField("id_consumer", StringType(), True),
    StructField("id_recurso", StringType(), True),
    StructField("id_transaccion", StringType(), True),
    StructField("ip_consumer", StringType(), True),
    StructField("ip_transaccion", StringType(), True),
    StructField("req_body_size", LongType(), True),
    StructField("resp_body_size", LongType(), True),
    StructField("status_response", IntegerType(), True),
    StructField("time_transaction", LongType(), True),
    StructField("tipo_operacion", StringType(), True),
])

wrapper_schema = StructType([
    StructField("@timestamp", StringType(), True), 
    StructField("log", StringType(), True) 
])

def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def cleanup_hdfs_on_startup(spark, path, checkpoint_path):
    """Limpia carpetas temporales y resuelve conflictos de Lease/FileNotFound."""
    try:
        sc = spark.sparkContext
        Path = spark._jvm.org.apache.hadoop.fs.Path
        FileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem
        conf = sc._jsc.hadoopConfiguration()
        fs = FileSystem.get(conf)

        # 1. Limpiar carpeta _temporary
        temp_path = Path(f"{path}/_temporary")
        if fs.exists(temp_path):
            print(f"[*] Limpiando archivos temporales en HDFS: {temp_path}")
            fs.delete(temp_path, True)
    except Exception as e:
        print(f"[!] Error durante la limpieza inicial: {e}")

def create_resilient_kafka_stream(spark):
    """Crea el stream de Kafka configurado para leer desde el offset más reciente al reiniciar."""
    df_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    return df_kafka

def process_batch_hdfs(batch_df, batch_id):
    row_count = batch_df.count()
    if row_count > 0:
        print(f"--- [HDFS] Batch {batch_id} | Rows: {row_count} ---")
        # Escritura directa a path para evitar conflictos de formato de tabla Hive
        batch_df.coalesce(1).write \
            .format("parquet") \
            .mode("append") \
            .partitionBy("zona", "year", "month", "day", "hour") \
            .save(OUTPUT_PATH_V3)
        print(f"   -> [HDFS] Datos guardados en path HDFS: {OUTPUT_PATH_V3}")

def process_batch_redis(batch_df, batch_id):
    row_count = batch_df.count()
    if row_count > 0:
        print(f"--- [REDIS] Batch {batch_id} | Rows: {row_count} ---")
        def send_to_redis(rows):
            try:
                r = get_redis_client()
                pipeline = r.pipeline()
                min_id_ms = int((time.time() - RETENTION_SECONDS) * 1000)
                
                for row in rows:
                    data = row.asDict()
                    if data['event_timestamp']:
                        data['event_timestamp'] = data['event_timestamp'].isoformat()
                    # Usamos el minid para recortar el stream automáticamente
                    pipeline.xadd(REDIS_STREAM_KEY, {"payload": json.dumps(data)}, minid=min_id_ms, approximate=True)
                
                pipeline.execute()
            except Exception as e:
                print(f"Error enviando a Redis: {e}")

        batch_df.foreachPartition(send_to_redis)
        print(f"   -> [REDIS] Datos enviados a Stream (Retention: 15m)")

def main():
    print(f"Iniciando sesión de Spark en [ZONA: {ZONA_ID}]")

    spark = (
        SparkSession.builder.appName(f"StreamingTrazas-{ZONA_ID}")
        .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    while True:
        try:
            print(f"[*] [ZONA: {ZONA_ID}] Preparando ejecución del stream...")
            
            # Limpieza de seguridad
            cleanup_hdfs_on_startup(spark, OUTPUT_PATH_V3, CHECKPOINT_PATH_V3)

            # 1. LEER STREAM
            df_kafka = create_resilient_kafka_stream(spark)

            # 2. PROCESAR
            df_logs_str = df_kafka.selectExpr("CAST(value AS STRING) as value_str")
            df_outer_parsed = df_logs_str.withColumn("data", from_json(col("value_str"), wrapper_schema))
            df_parsed = df_outer_parsed.withColumn("log_data", from_json(col("data.log"), log_schema))

            df_processed = (
                df_parsed.select("log_data.*") 
                .withColumn("full_endpoint", col("endpoint"))
                .withColumn("endpoint", split(col("full_endpoint"), "\\?").getItem(0))
                .withColumn("query_params", split(col("full_endpoint"), "\\?").getItem(1))
                .withColumn("event_timestamp", to_timestamp(col("date_transaction"), "yyyy-MM-dd'T'HH:mm:ssZ"))
                .withColumn("year", year(col("event_timestamp")))
                .withColumn("month", month(col("event_timestamp")))
                .withColumn("day", dayofmonth(col("event_timestamp")))
                .withColumn("hour", hour(col("event_timestamp")))
                .withColumn("zona", lit(ZONA_ID))
                .drop("full_endpoint")
                # Consumer classification: zone1/service-1 = internal, external/xxx = external, service-1 (legacy) = internal, - = external
                .withColumn("consumer_type",
                    when(col("id_consumer").isNull() | (col("id_consumer") == "-"), lit("external"))
                    .when(col("id_consumer").startswith("external/"), lit("external"))
                    .otherwise(lit("internal"))
                )
                .withColumn("source_zone",
                    when(col("id_consumer").isNull() | (col("id_consumer") == "-"), lit("unknown"))
                    .when(col("id_consumer").contains("/"),
                          split(col("id_consumer"), "/").getItem(0))
                    .otherwise(lit("unknown"))
                )
                .withColumn("source_service",
                    when(col("id_consumer").isNull() | (col("id_consumer") == "-"), lit("unknown"))
                    .when(col("id_consumer").contains("/"),
                          regexp_extract(col("id_consumer"), r"^[^/]+/(.+?)(?:-\d+)?$", 1))
                    .otherwise(regexp_extract(col("id_consumer"), r"^(.+?)(?:-\d+)?$", 1))
                )
            )

            # 3. LANZAR QUERIES
            print(f"[*] [ZONA: {ZONA_ID}] Lanzando queries de stream (HDFS + Redis)...")

            # Query 1: HDFS (Consolidación) - Cada 2 minutos
            hdfs_query = (
                df_processed.writeStream
                .foreachBatch(process_batch_hdfs)
                .option("checkpointLocation", f"{CHECKPOINT_PATH_V3}/hdfs")
                .trigger(processingTime="2 minutes")
                .start()
            )

            # Query 2: REDIS (Tiempo Real) - Cada 5 segundos
            redis_query = (
                df_processed.writeStream
                .foreachBatch(process_batch_redis)
                .option("checkpointLocation", f"{CHECKPOINT_PATH_V3}/redis")
                .trigger(processingTime="5 seconds")
                .start()
            )

            print(f"[*] [ZONA: {ZONA_ID}] Streams activos. Esperando datos...")

            # Monitorear streams independientemente: si Redis falla, solo reiniciar Redis
            while True:
                time.sleep(10)

                if not hdfs_query.isActive:
                    exc = hdfs_query.exception()
                    print(f"[!] HDFS stream terminó: {exc}")
                    # HDFS es crítico — reiniciar todo
                    if redis_query.isActive:
                        redis_query.stop()
                    break

                if not redis_query.isActive:
                    exc = redis_query.exception()
                    print(f"[!] Redis stream terminó: {exc}. Reiniciando solo Redis...")
                    time.sleep(5)
                    redis_query = (
                        df_processed.writeStream
                        .foreachBatch(process_batch_redis)
                        .option("checkpointLocation", f"{CHECKPOINT_PATH_V3}/redis")
                        .trigger(processingTime="5 seconds")
                        .start()
                    )
                    print(f"[*] Redis stream reiniciado")

        except Exception as e:
            print(f"[!] ERROR CRÍTICO EN RUNTIME: {e}")
            print("[*] Reintentando en 15 segundos...")
            time.sleep(15)

if __name__ == "__main__":
    main()
