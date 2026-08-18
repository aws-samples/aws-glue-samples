# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import sys
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['BUCKET', 'DATABASE'])
BUCKET = args['BUCKET']
DATABASE = args['DATABASE']

spark = SparkSession.builder \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{BUCKET}/warehouse/") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

print(f"Spark: {spark.version}")
print(f"Bucket: {BUCKET}")
print(f"Database: {DATABASE}")

TABLE = f"glue_catalog.{DATABASE}.vehicle_telemetry"

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS glue_catalog.{DATABASE}")
spark.sql(f"DROP TABLE IF EXISTS {TABLE}")

# Create table with all four new Iceberg v3 column types
spark.sql(f"""
    CREATE TABLE {TABLE} (
        event_id STRING,
        vehicle_id STRING,
        vehicle_type STRING DEFAULT 'UNKNOWN',
        event_time TIMESTAMP_NTZ(9),
        location GEOMETRY(4326),
        service_area GEOGRAPHY(4326),
        sensor_payload VARIANT,
        speed_kmh DOUBLE DEFAULT 0.0,
        region STRING DEFAULT 'EMEA'
    ) USING ICEBERG
    TBLPROPERTIES (
        'format-version' = '3',
        'write.delete.mode' = 'merge-on-read'
    )
    PARTITIONED BY (days(event_time), vehicle_type)
""")
print("✅ Table created with GEOMETRY + TIMESTAMP_NTZ(9) + VARIANT + DEFAULT values")

# Insert van telemetry — POINT(-0.1278, 51.5074) = Central London
spark.sql(f"""
    INSERT INTO {TABLE} VALUES (
        'EVT-001', 'VAN-042', 'VAN',
        CAST('2026-07-28 09:15:30.123456789' AS TIMESTAMP_NTZ(9)),
        ST_SetSrid(ST_GeomFromWKB(X'0101000000E17A14AE47E1C0BF1F85EB51B84E4940'), 4326),
        ST_SetSrid(ST_GeogFromWKB(X'0101000000E17A14AE47E1C0BF1F85EB51B84E4940'), 4326),
        PARSE_JSON('{{"fuel_pct": 0.72, "cargo_kg": 450, "door_open": false, "engine": {{"rpm": 2100, "temp_c": 88.5}}, "route": {{"stops_remaining": 4, "eta_minutes": 35}}}}'),
        35.2, 'EMEA'
    )
""")

# Insert delivery robot telemetry — POINT(4, 1)
spark.sql(f"""
    INSERT INTO {TABLE} VALUES (
        'EVT-002', 'ROB-117', 'ROBOT',
        CAST('2026-07-28 09:15:30.123456790' AS TIMESTAMP_NTZ(9)),
        ST_SetSrid(ST_GeomFromWKB(X'01010000000000000000001040000000000000F03F'), 4326),
        ST_SetSrid(ST_GeogFromWKB(X'01010000000000000000001040000000000000F03F'), 4326),
        PARSE_JSON('{{"battery_pct": 0.62, "obstacle_distance_m": 2.8, "payload_g": 1200, "navigation_mode": "autonomous", "cameras": {{"front": "active", "rear": "recording"}}, "path_clear": true}}'),
        48.0, 'EMEA'
    )
""")

# Insert bike telemetry — POINT(3, 1)
spark.sql(f"""
    INSERT INTO {TABLE} VALUES (
        'EVT-003', 'BKE-203', 'BIKE',
        CAST('2026-07-28 09:15:30.999999999' AS TIMESTAMP_NTZ(9)),
        ST_SetSrid(ST_GeomFromWKB(X'01010000000000000000000840000000000000F03F'), 4326),
        ST_SetSrid(ST_GeogFromWKB(X'01010000000000000000000840000000000000F03F'), 4326),
        PARSE_JSON('{{"cadence_rpm": 72, "heart_rate_bpm": 145, "assist_level": "high", "battery_pct": 0.55, "route_type": "cycle_lane", "deliveries": {{"completed": 6, "remaining": 4, "bag_pct_full": 55}}}}'),
        18.5, 'EMEA'
    )
""")

# Insert with defaults omitted — test DEFAULT values — POINT(1, 1)
spark.sql(f"""
    INSERT INTO {TABLE}
        (event_id, vehicle_id, event_time, location, service_area, sensor_payload)
    VALUES (
        'EVT-004', 'UNK-999',
        CAST('2026-07-28 10:00:00.000000000' AS TIMESTAMP_NTZ(9)),
        ST_SetSrid(ST_GeomFromWKB(X'0101000000000000000000F03F000000000000F03F'), 4326),
        ST_SetSrid(ST_GeogFromWKB(X'0101000000000000000000F03F000000000000F03F'), 4326),
        PARSE_JSON('{{"status": "initializing"}}')
    )
""")

count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {TABLE}").collect()[0]['cnt']
print(f"✅ Inserted {count} telemetry events (van, delivery robot, bike, unknown)")
spark.stop()
