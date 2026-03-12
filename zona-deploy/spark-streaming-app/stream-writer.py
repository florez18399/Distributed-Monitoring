import os
import redis
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, hour, lit, 
    from_json, to_timestamp  # ¡Ya no necesitamos regexp_extract!
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, TimestampType
)

# --- Configuración (ajustada para aislamiento) ---
ZONA_ID = os.environ.get("ZONA_ID", "zona_desconocida")
HDFS_PATH = "hdfs://namenode:9000"
TABLE_NAME = "trazas_logs_v5"
OUTPUT_PATH_V3 = "hdfs://namenode:9000/data/trazas_v5"
CHECKPOINT_PATH_V3 = f"hdfs://namenode:9000/checkpoints/trazas_v5/{ZONA_ID}"
KAFKA_BOOTSTRAP_SERVERS = "broker:29092" 
KAFKA_TOPIC = "envoy-logs"

# Configuración Redis (Real-time)
REDIS_HOST = "redis-realtime"  # Nombre del host en la red de zona
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

def check_and_reset_checkpoint_if_needed(spark, checkpoint_path):
    """Verifica si el checkpoint está corrupto y lo resetea si es necesario."""
    try:
        spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("checkpointLocation", checkpoint_path) \
            .load()
        print("Checkpoint existente es válido.")
        return False
    except Exception as e:
        print(f"Checkpoint corrupto o inválido detectado: {e}")
        print("Reseteando checkpoint...")
        try:
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
            path = spark._jvm.org.apache.hadoop.fs.Path(checkpoint_path)
            if fs.exists(path):
                fs.delete(path, True)
                print(f"Checkpoint eliminado exitosamente: {checkpoint_path}")
        except Exception as fs_e:
            print(f"No se pudo eliminar checkpoint: {fs_e}")
        return True 

def create_resilient_kafka_stream(spark, checkpoint_path):
    checkpoint_reset = check_and_reset_checkpoint_if_needed(spark, checkpoint_path)
    df_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest" if checkpoint_reset else "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    return df_kafka

def main():
    print(f"Iniciando sesión de Spark en [ZONA: {ZONA_ID}] con soporte Real-time Redis")

    spark = (
        SparkSession.builder.appName(f"StreamingTrazas-{ZONA_ID}")
        .enableHiveSupport()
        .config("spark.sql.hive.metastore.version", "3.1")
        .config("spark.sql.hive.metastore.jars", "/opt/spark/hive-3.1-libs/*")
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # 1. LEER STREAM DESDE KAFKA
    df_kafka = create_resilient_kafka_stream(spark, CHECKPOINT_PATH_V3)

    # 2. PROCESAR DATOS
    df_logs_str = df_kafka.selectExpr("CAST(value AS STRING) as value_str")
    df_outer_parsed = df_logs_str.withColumn("data", from_json(col("value_str"), wrapper_schema))
    df_parsed = df_outer_parsed.withColumn("log_data", from_json(col("data.log"), log_schema))

    df_final = (
        df_parsed.select("log_data.*") 
        .withColumn("event_timestamp", to_timestamp(col("date_transaction"), "yyyy-MM-dd'T'HH:mm:ssZ"))
        .withColumn("year", year(col("event_timestamp")))
        .withColumn("month", month(col("event_timestamp")))
        .withColumn("day", dayofmonth(col("event_timestamp")))
        .withColumn("hour", hour(col("event_timestamp")))
        .withColumn("zona", lit(ZONA_ID))
    )

    # Inicializar Tabla Hive si no existe
    try:
        spark.read.table(TABLE_NAME)
    except:
        print(f"Inicializando tabla {TABLE_NAME}...")
        spark.createDataFrame([], df_final.schema).write \
            .format("parquet").mode("ignore") \
            .partitionBy("zona", "year", "month", "day", "hour") \
            .option("path", OUTPUT_PATH_V3).saveAsTable(TABLE_NAME)

    def process_batch(batch_df, batch_id):
        row_count = batch_df.count()
        if row_count > 0:
            print(f"--- Batch {batch_id} | Rows: {row_count} ---")
            
            # A. Escritura en HDFS (Histórico)
            batch_df.write \
                .format("parquet") \
                .mode("append") \
                .partitionBy("zona", "year", "month", "day", "hour") \
                .saveAsTable(TABLE_NAME)
            
            # B. Escritura en Redis (Tiempo Real)
            def send_to_redis(rows):
                r = get_redis_client()
                pipeline = r.pipeline()
                min_id = int((time.time() - RETENTION_SECONDS) * 1000)
                
                for row in rows:
                    data = row.asDict()
                    # Convertir timestamp a string para JSON
                    if data['event_timestamp']:
                        data['event_timestamp'] = data['event_timestamp'].isoformat()
                    
                    # Agregar al stream y recortar (XADD con MINID para retención por tiempo)
                    # El ID de redis stream es <timestamp-ms>-<sequence>
                    pipeline.xadd(REDIS_STREAM_KEY, {"payload": json.dumps(data)}, minid=min_id, approximate=True)
                
                pipeline.execute()

            # Usamos foreachPartition para eficiencia en la conexión a Redis
            batch_df.foreachPartition(send_to_redis)
            print(f"   -> Datos enviados a Redis Stream: {REDIS_STREAM_KEY}")

    query = (
        df_final.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH_V3)
        .trigger(processingTime="10 seconds") # Más frecuente para real-time
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()