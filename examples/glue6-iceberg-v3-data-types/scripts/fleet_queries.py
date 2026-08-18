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
print(f"Spark version: {spark.version}")
print(f"Database: {DATABASE}")

TABLE = f"glue_catalog.{DATABASE}.vehicle_telemetry"

# Geofence polygon: POLYGON((0 0, 5 0, 5 2, 0 2, 0 0)) encoded as WKB
POLY = "01030000000100000005000000000000000000000000000000000000000000000000001440000000000000000000000000000014400000000000000040000000000000000000000000000000400000000000000000000000000000000000000000"

print("=" * 60)
print("GEOFENCE DETECTION (ST_Intersects)")
print("=" * 60)
print("Polygon (0,0)-(5,0)-(5,2)-(0,2) covers ROBOT(4,1), BIKE(3,1), UNKNOWN(1,1)")
print("VAN at (-0.1278, 51.5074) is outside the polygon")
spark.sql(f"""
    SELECT event_id, vehicle_id, vehicle_type, speed_kmh
    FROM {TABLE}
    WHERE ST_Intersects(location, ST_SetSrid(ST_GeomFromWKB(X'{POLY}'), 4326))
    ORDER BY event_id
""").show(truncate=False)

print("=" * 60)
print("NANOSECOND EVENT SEQUENCING")
print("=" * 60)
print("EVT-001 and EVT-002 are 1 nanosecond apart")
spark.sql(f"""
    SELECT event_id, vehicle_id, CAST(event_time AS STRING) AS precise_time
    FROM {TABLE}
    WHERE event_id IN ('EVT-001', 'EVT-002', 'EVT-003')
    ORDER BY event_time ASC
""").show(truncate=False)

print("=" * 60)
print("VARIANT QUERIES (energy level per vehicle type)")
print("=" * 60)
print("Different JSON schema per vehicle -- queried with variant_get")
spark.sql(f"""
    SELECT vehicle_id, vehicle_type,
        CASE vehicle_type
            WHEN 'VAN' THEN variant_get(sensor_payload, '$.fuel_pct', 'DOUBLE')
            WHEN 'ROBOT' THEN variant_get(sensor_payload, '$.battery_pct', 'DOUBLE')
            WHEN 'BIKE' THEN variant_get(sensor_payload, '$.battery_pct', 'DOUBLE')
            ELSE NULL
        END AS energy_level,
        variant_get(sensor_payload, '$.engine.temp_c', 'DOUBLE') AS engine_temp,
        variant_get(sensor_payload, '$.cameras.front', 'STRING') AS front_cam,
        variant_get(sensor_payload, '$.deliveries.completed', 'INT') AS deliveries_done
    FROM {TABLE}
    WHERE vehicle_type != 'UNKNOWN'
    ORDER BY vehicle_id
""").show(truncate=False)

print("=" * 60)
print("DEFAULT VALUES (EVT-004 with omitted columns)")
print("=" * 60)
print("vehicle_type, speed_kmh, region were omitted -- defaults applied")
spark.sql(f"""
    SELECT event_id, vehicle_type, speed_kmh, region
    FROM {TABLE}
    WHERE event_id = 'EVT-004'
""").show(truncate=False)

print("=" * 60)
print("COMBINED QUERY (geospatial + nanosecond + variant)")
print("=" * 60)
print("Vehicles inside the geofence, ordered by nanosecond timestamp, with energy from variant")
spark.sql(f"""
    SELECT vehicle_id, vehicle_type,
        CAST(event_time AS STRING) AS precise_time,
        CASE vehicle_type
            WHEN 'VAN' THEN variant_get(sensor_payload, '$.fuel_pct', 'DOUBLE')
            WHEN 'ROBOT' THEN variant_get(sensor_payload, '$.battery_pct', 'DOUBLE')
            WHEN 'BIKE' THEN variant_get(sensor_payload, '$.battery_pct', 'DOUBLE')
            ELSE NULL
        END AS energy_level,
        speed_kmh
    FROM {TABLE}
    WHERE ST_Intersects(location, ST_SetSrid(ST_GeomFromWKB(X'{POLY}'), 4326))
    ORDER BY event_time ASC
""").show(truncate=False)

print("=" * 60)
print("ALL QUERIES COMPLETED SUCCESSFULLY")
print("=" * 60)
spark.stop()
