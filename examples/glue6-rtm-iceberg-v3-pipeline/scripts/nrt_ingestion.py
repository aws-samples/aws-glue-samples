# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date
from awsglue.utils import getResolvedOptions
import time

args = getResolvedOptions(sys.argv, ['BOOTSTRAP_SERVERS', 'DATABASE_NAME', 'CHECKPOINT_PATH'])
BOOTSTRAP = args['BOOTSTRAP_SERVERS']
DB_NAME = args['DATABASE_NAME']
CHECKPOINT_PATH = args['CHECKPOINT_PATH']

spark = SparkSession.builder.getOrCreate()
print(f"Spark version: {spark.version}")
print(f"Bootstrap: {BOOTSTRAP}")
print(f"Database: {DB_NAME}")

TABLE = f"glue_catalog.{DB_NAME}.trade_risk_vectors"

# Create Iceberg v3 table with Variant + shredding
spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{DB_NAME}")
spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
spark.sql(f"""
CREATE TABLE {TABLE} (
    trade_id STRING, book_id STRING, desk STRING,
    asset_class STRING DEFAULT 'UNKNOWN',
    execution_time STRING, pricing_vector VARIANT,
    var_contribution DOUBLE DEFAULT 0.0,
    risk_weight DOUBLE DEFAULT 1.0,
    trade_date DATE, region STRING DEFAULT 'EMEA'
) USING iceberg
TBLPROPERTIES ('format-version'='3', 'write.delete.mode'='merge-on-read',
               'write.update.mode'='merge-on-read',
               'write.parquet.shred-variants'='true')
PARTITIONED BY (trade_date, asset_class)
""")
print("✅ Iceberg v3 table created (Variant + shredding + DEFAULT values)")

# Read from Kafka with IAM auth
raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP) \
    .option("subscribe", "trade-risk-vectors") \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "AWS_MSK_IAM") \
    .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;") \
    .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler") \
    .load()

trades = raw.select(col("value").cast("string").alias("json_str")).selectExpr(
    "get_json_object(json_str, '$.trade_id') AS trade_id",
    "get_json_object(json_str, '$.book_id') AS book_id",
    "get_json_object(json_str, '$.desk') AS desk",
    "get_json_object(json_str, '$.asset_class') AS asset_class",
    "get_json_object(json_str, '$.execution_time') AS execution_time",
    "PARSE_JSON(get_json_object(json_str, '$.pricing_vector')) AS pricing_vector",
    "CAST(get_json_object(json_str, '$.var_contribution') AS DOUBLE) AS var_contribution",
    "CAST(get_json_object(json_str, '$.risk_weight') AS DOUBLE) AS risk_weight",
    "CURRENT_DATE() AS trade_date",
    "get_json_object(json_str, '$.region') AS region"
)

query = trades.writeStream.format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/nrt_ingestion/") \
    .trigger(processingTime="10 seconds") \
    .toTable(TABLE)

print("✅ NRT streaming started — running for 5 minutes")
time.sleep(300)
query.stop()

count = spark.sql(f"SELECT count(*) FROM {TABLE}").collect()[0][0]
print(f"✅ NRT complete — {count} rows in Iceberg v3 table")

# Show sample with variant_get
spark.sql(f"""
SELECT trade_id, asset_class,
  variant_get(pricing_vector, '$.model', 'STRING') as model,
  variant_get(pricing_vector, '$.notional', 'DOUBLE') as notional
FROM {TABLE} LIMIT 5
""").show(truncate=False)

spark.stop()
