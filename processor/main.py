import logging
import psycopg2

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "trades.raw"
SPARK_KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"

TRADE_SCHEMA = StructType([
    StructField("product_id", StringType()),
    StructField("trade_id", StringType()),
    StructField("price", StringType()),
    StructField("size", StringType()),
    StructField("time", StringType()),
    StructField("side", StringType()),
])

PG_CONN = {
    "host": "localhost",
    "port": 5433,
    "dbname": "crypto",
    "user": "crypto",
    "password": "crypto",
    "options": "-c timezone=UTC",
}
CHECKPOINT_DIR = "./.checkpoints/trades"

UPSERT_SQL = """
INSERT INTO trades (product_id, trade_id, price, size, time, side)
VALUES %s
ON CONFLICT (product_id, trade_id) DO NOTHING
"""

def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("crypto-stream-processor")
        .config("spark.jars.packages", SPARK_KAFKA_PKG)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

def upsert_batch(batch_df, batch_id: int) -> None:
    rows = [
        (r.product_id, r.trade_id, r.price, r.size, r.time, r.side)
        for r in batch_df.collect()
    ]
    if not rows:
        return
    with psycopg2.connect(**PG_CONN) as conn, conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows)
    logger.info("batch %s: upserted %d row(s)", batch_id, len(rows))

def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw.select(from_json(col("value").cast("string"), TRADE_SCHEMA).alias("t"))
        .select("t.*")
        .where(col("trade_id").isNotNull())
        .select(
            col("product_id"),
            col("trade_id"),
            col("price").cast(DecimalType(38, 18)).alias("price"),
            col("size").cast(DecimalType(38, 18)).alias("size"),
            to_timestamp(col("time")).alias("time"),
            col("side"),
        )
    )
    
    deduped = parsed.withWatermark("time", "2 hours").dropDuplicatesWithinWatermark(
        ["product_id", "trade_id"]
    )
    parsed.printSchema()

    query = (
        deduped.writeStream.foreachBatch(upsert_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()