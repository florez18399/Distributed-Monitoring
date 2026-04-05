"""
HDFS Parquet Compactor
======================
Compacts small parquet files into fewer, larger files per partition.
Also backfills derived fields (consumer_type, source_zone, source_service)
for legacy traces that don't have them.

Usage:
  spark-submit compactor.py                    # Compact all partitions
  spark-submit compactor.py --date 2026-04-02  # Compact specific date
  spark-submit compactor.py --yesterday        # Compact yesterday's data
  spark-submit compactor.py --recent 6         # Compact last 6 hours (excluding current)
"""

import sys
import re
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, split, regexp_extract, input_file_name
)


HDFS_PATH = "hdfs://namenode:9000/data/trazas_v5"
COMPACT_TMP = "hdfs://namenode:9000/data/_compaction_tmp"

KNOWN_SERVICES = {"catalog-api", "orders-api", "payments-api", "auth-api"}


def parse_args():
    target_date = None
    recent_hours = None
    compact_all = True

    for i, arg in enumerate(sys.argv[1:]):
        if arg == "--date" and i + 1 < len(sys.argv) - 1:
            target_date = sys.argv[i + 2]
            compact_all = False
        elif arg == "--yesterday":
            target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            compact_all = False
        elif arg == "--recent" and i + 1 < len(sys.argv) - 1:
            recent_hours = int(sys.argv[i + 2])
            compact_all = False

    return compact_all, target_date, recent_hours


def add_consumer_fields(df):
    """Add consumer_type, source_zone, source_service if not present."""
    existing_cols = set(df.columns)

    if "consumer_type" not in existing_cols:
        df = df.withColumn("consumer_type",
            when(col("id_consumer").isNull() | (col("id_consumer") == "-"), lit("external"))
            .when(col("id_consumer").startswith("external/"), lit("external"))
            .otherwise(lit("internal"))
        )

    if "source_zone" not in existing_cols:
        df = df.withColumn("source_zone",
            when(col("id_consumer").isNull() | (col("id_consumer") == "-"), lit("unknown"))
            .when(col("id_consumer").contains("/"),
                  split(col("id_consumer"), "/").getItem(0))
            .otherwise(lit("unknown"))
        )

    if "source_service" not in existing_cols:
        df = df.withColumn("source_service",
            when(col("id_consumer").isNull() | (col("id_consumer") == "-"), lit("unknown"))
            .when(col("id_consumer").contains("/"),
                  regexp_extract(col("id_consumer"), r"^[^/]+/(.+?)(?:-\d+)?$", 1))
            .otherwise(regexp_extract(col("id_consumer"), r"^(.+?)(?:-\d+)?$", 1))
        )

    return df


def compact_partition(spark, path, partition_filter=None):
    """Read, compact, and rewrite a partition."""
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark.sparkContext._jsc.hadoopConfiguration()
    )
    Path = spark._jvm.org.apache.hadoop.fs.Path

    source = Path(path)
    if not fs.exists(source):
        print(f"[SKIP] Path does not exist: {path}")
        return 0

    try:
        df = spark.read.parquet(path)
    except Exception as e:
        print(f"[ERROR] Failed to read {path}: {e}")
        return 0

    if df.rdd.isEmpty():
        print(f"[SKIP] Empty partition: {path}")
        return 0

    row_count = df.count()

    # Count source files
    file_count = df.withColumn("_file", input_file_name()).select("_file").distinct().count()

    if file_count <= 1:
        print(f"[SKIP] Already compact ({file_count} file, {row_count} rows): {path}")
        return 0

    # Backfill consumer fields for legacy data
    df = add_consumer_fields(df)

    # Remove partition columns before writing (they're encoded in the path)
    write_cols = [c for c in df.columns if c not in ("zona", "year", "month", "day", "hour")]
    df_write = df.select(*write_cols)

    # Write to temp location
    tmp_path = COMPACT_TMP + "/" + path.split("/trazas_v5/")[-1]
    df_write.coalesce(1).write.format("parquet").mode("overwrite").save(tmp_path)

    # Swap: delete original partition, move compacted data
    fs.delete(source, True)

    # Move files from tmp to original location
    tmp_dir = Path(tmp_path)
    if fs.exists(tmp_dir):
        file_list = fs.listStatus(tmp_dir)
        for file_status in file_list:
            file_path = file_status.getPath()
            name = file_path.getName()
            if name.startswith("part-") or name.endswith(".parquet"):
                fs.mkdirs(source)
                fs.rename(file_path, Path(path + "/" + name))
        # Cleanup tmp
        fs.delete(tmp_dir, True)

    print(f"[COMPACTED] {path}: {file_count} files -> 1 file ({row_count} rows)")
    return file_count - 1


def discover_partitions(spark, base_path, date_filter=None, recent_hours=None):
    """List all leaf partition paths."""
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark.sparkContext._jsc.hadoopConfiguration()
    )
    Path = spark._jvm.org.apache.hadoop.fs.Path

    partitions = []

    def walk(path, depth=0):
        try:
            statuses = fs.listStatus(Path(path))
        except Exception:
            return

        has_parquet = False
        subdirs = []

        for s in statuses:
            name = s.getPath().getName()
            if name.startswith("_") or name.startswith("."):
                continue
            if s.isDirectory():
                subdirs.append(s.getPath().toString())
            elif name.endswith(".parquet"):
                has_parquet = True

        if has_parquet:
            partitions.append(path)
        for d in subdirs:
            walk(d, depth + 1)

    if recent_hours:
        # Compact the last N hours, excluding the current hour (still being written)
        now = datetime.utcnow()
        hours_to_compact = []
        for h in range(1, recent_hours + 1):
            dt = now - timedelta(hours=h)
            hours_to_compact.append(dt)

        try:
            zone_dirs = fs.listStatus(Path(base_path))
        except Exception:
            return []

        for zone_status in zone_dirs:
            zone_name = zone_status.getPath().getName()
            if not zone_name.startswith("zona="):
                continue
            for dt in hours_to_compact:
                hour_path = (f"{base_path}/{zone_name}/year={dt.year}"
                             f"/month={dt.month}/day={dt.day}/hour={dt.hour}")
                walk(hour_path)

    elif date_filter:
        # Parse date and build specific paths
        dt = datetime.strptime(date_filter, "%Y-%m-%d")
        # Scan all zones for this date
        try:
            zone_dirs = fs.listStatus(Path(base_path))
        except Exception:
            return []

        for zone_status in zone_dirs:
            zone_name = zone_status.getPath().getName()
            if not zone_name.startswith("zona="):
                continue
            date_path = f"{base_path}/{zone_name}/year={dt.year}/month={dt.month}/day={dt.day}"
            walk(date_path)
    else:
        walk(base_path)

    return partitions


def main():
    compact_all, target_date, recent_hours = parse_args()

    if recent_hours:
        mode = f"last {recent_hours} hours (excluding current)"
    elif compact_all:
        mode = "ALL partitions"
    else:
        mode = f"date={target_date}"
    print(f"=" * 60)
    print(f"HDFS Parquet Compactor")
    print(f"Mode: {mode}")
    print(f"Source: {HDFS_PATH}")
    print(f"=" * 60)

    spark = (
        SparkSession.builder
        .appName("ParquetCompactor")
        .config("spark.sql.sources.commitProtocolClass",
                "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    partitions = discover_partitions(spark, HDFS_PATH, target_date, recent_hours)
    print(f"\nFound {len(partitions)} partition(s) to evaluate\n")

    total_removed = 0
    compacted_count = 0

    for partition_path in sorted(partitions):
        removed = compact_partition(spark, partition_path)
        total_removed += removed
        if removed > 0:
            compacted_count += 1

    print(f"\n{'=' * 60}")
    print(f"COMPACTION COMPLETE")
    print(f"  Partitions compacted: {compacted_count}")
    print(f"  Files eliminated: {total_removed}")
    print(f"{'=' * 60}")

    spark.stop()


if __name__ == "__main__":
    main()
