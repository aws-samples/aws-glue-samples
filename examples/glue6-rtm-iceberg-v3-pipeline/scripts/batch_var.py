# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import arrow_udf, col, count, sum, max, current_date, lit
from pyspark.sql.types import DoubleType
import pyarrow as pa
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['DATABASE_NAME'])
DB_NAME = args['DATABASE_NAME']

spark = SparkSession.builder.getOrCreate()
print(f"Spark version: {spark.version}")
print(f"Database: {DB_NAME}")

TRADE_TABLE = f"glue_catalog.{DB_NAME}.trade_risk_vectors"
SUMMARY_TABLE = f"glue_catalog.{DB_NAME}.daily_risk_summary"

# Create summary table
spark.sql(f"DROP TABLE IF EXISTS {SUMMARY_TABLE}")
spark.sql(f"""
CREATE TABLE {SUMMARY_TABLE} (
    report_date DATE, desk STRING, asset_class STRING, jurisdiction STRING,
    trade_count BIGINT, total_exposure DOUBLE, total_var_99 DOUBLE,
    max_single_trade_var DOUBLE, report_metadata VARIANT
) USING iceberg
TBLPROPERTIES ('format-version'='3') PARTITIONED BY (report_date)
""")
print("✅ Summary table created")

# Read from Iceberg and extract risk metrics from Variant
enriched = spark.sql(f"""
    SELECT
        trade_id, book_id, desk, asset_class, region,
        CASE asset_class
            WHEN 'EQUITY' THEN variant_get(pricing_vector, '$.notional', 'DOUBLE')
                             * variant_get(pricing_vector, '$.greeks.delta', 'DOUBLE')
            WHEN 'FX' THEN variant_get(pricing_vector, '$.notional_usd', 'DOUBLE') * 0.01
            WHEN 'RATES' THEN variant_get(pricing_vector, '$.dv01', 'DOUBLE')
            ELSE var_contribution * 1000000
        END AS risk_exposure,
        variant_get(pricing_vector, '$.model', 'STRING') AS pricing_model,
        CASE asset_class
            WHEN 'EQUITY' THEN variant_get(pricing_vector, '$.scenarios[0].breakdown.by_sector.financials', 'DOUBLE')
            WHEN 'FX' THEN variant_get(pricing_vector, '$.vol_surface.skew.short_term.1W', 'DOUBLE')
            WHEN 'RATES' THEN variant_get(pricing_vector, '$.model_params.calibration.fit_error', 'DOUBLE')
        END AS deep_nested_metric,
        CASE region
            WHEN 'EMEA' THEN 'UK' WHEN 'AMER' THEN 'US'
            WHEN 'APAC' THEN 'JP' ELSE 'OTHER'
        END AS jurisdiction,
        risk_weight
    FROM {TRADE_TABLE}
    WHERE trade_date = current_date()
""")

row_count = enriched.count()
print(f"Processing {row_count} trades for VaR calculation")

if row_count == 0:
    print("⚠️ No trades for today — trying without date filter")
    enriched = spark.sql(f"""
        SELECT
            trade_id, book_id, desk, asset_class, region,
            CASE asset_class
                WHEN 'EQUITY' THEN variant_get(pricing_vector, '$.notional', 'DOUBLE')
                                 * variant_get(pricing_vector, '$.greeks.delta', 'DOUBLE')
                WHEN 'FX' THEN variant_get(pricing_vector, '$.notional_usd', 'DOUBLE') * 0.01
                WHEN 'RATES' THEN variant_get(pricing_vector, '$.dv01', 'DOUBLE')
                ELSE var_contribution * 1000000
            END AS risk_exposure,
            variant_get(pricing_vector, '$.model', 'STRING') AS pricing_model,
            CASE asset_class
                WHEN 'EQUITY' THEN variant_get(pricing_vector, '$.scenarios[0].breakdown.by_sector.financials', 'DOUBLE')
                WHEN 'FX' THEN variant_get(pricing_vector, '$.vol_surface.skew.short_term.1W', 'DOUBLE')
                WHEN 'RATES' THEN variant_get(pricing_vector, '$.model_params.calibration.fit_error', 'DOUBLE')
            END AS deep_nested_metric,
            CASE region
                WHEN 'EMEA' THEN 'UK' WHEN 'AMER' THEN 'US'
                WHEN 'APAC' THEN 'JP' ELSE 'OTHER'
            END AS jurisdiction,
            risk_weight
        FROM {TRADE_TABLE}
    """)
    row_count = enriched.count()
    print(f"  Found {row_count} trades (all dates)")
    if row_count == 0:
        print("❌ No data at all. Exiting.")
        spark.stop()
        sys.exit(1)

# Arrow-native UDF for VaR (Spark 4 @arrow_udf — processes entire batch as PyArrow arrays)
@arrow_udf(returnType=DoubleType())
def calculate_historical_var(exposure: pa.Array, weight: pa.Array) -> pa.Array:
    import numpy as np
    exp = exposure.to_numpy()
    wt = weight.to_numpy()

    # Simulate 1000 daily P&L scenarios per trade using historical volatility
    np.random.seed(42)
    scenarios = np.random.normal(0, 0.015, size=(len(exp), 1000))
    pnl = np.abs(exp).reshape(-1, 1) * scenarios * wt.reshape(-1, 1)

    # VaR = 99th percentile loss across scenarios
    var_99 = np.percentile(pnl, 99, axis=1)
    return pa.array(var_99)

enriched_with_var = enriched.withColumn(
    "parametric_var_99",
    calculate_historical_var(col("risk_exposure"), col("risk_weight"))
)

print("\n=== Sample VaR Results ===")
enriched_with_var.select("trade_id", "asset_class", "pricing_model", "risk_exposure", "deep_nested_metric", "parametric_var_99").show(5, truncate=False)
print("✅ @arrow_udf computed Historical VaR (1000 scenarios, 99th percentile)")

# Aggregate and write summary
summary = enriched_with_var.groupBy("desk", "asset_class", "jurisdiction").agg(
    count("*").alias("trade_count"),
    sum("risk_exposure").alias("total_exposure"),
    sum("parametric_var_99").alias("total_var_99"),
    max("parametric_var_99").alias("max_single_trade_var")
)

summary.selectExpr(
    "current_date() AS report_date",
    "desk", "asset_class", "jurisdiction",
    "trade_count", "total_exposure", "total_var_99", "max_single_trade_var",
    "PARSE_JSON('{\"model\":\"ParametricVaR\",\"confidence\":0.99,\"engine\":\"Glue6\"}') AS report_metadata"
).writeTo(SUMMARY_TABLE).append()

print("\n=== Daily Risk Summary ===")
spark.sql(f"""
SELECT desk, asset_class, jurisdiction, trade_count,
       ROUND(total_var_99, 2) AS var_99,
       variant_get(report_metadata, '$.engine', 'STRING') AS engine
FROM {SUMMARY_TABLE}
ORDER BY total_var_99 DESC
""").show(truncate=False)
print("✅ Batch VaR complete — summary written to daily_risk_summary")

# === Deletion Vectors Demo ===
print("\n=== Deletion Vectors (trade amendment + cancellation) ===")

# Get a trade to amend and one to cancel
# Note: trade_id is sourced from our own Iceberg table (trusted producer).
# In production, parameterize inputs from untrusted sources.
first_trade = spark.sql(f"SELECT trade_id FROM {TRADE_TABLE} ORDER BY trade_id LIMIT 1").collect()[0][0]
last_trade = spark.sql(f"SELECT trade_id FROM {TRADE_TABLE} ORDER BY trade_id DESC LIMIT 1").collect()[0][0]
print(f"  Amending: {first_trade}")
print(f"  Cancelling: {last_trade}")

# Trade amendment (UPDATE) — writes deletion vector, not data file rewrite
spark.sql(f"""
    UPDATE {TRADE_TABLE}
    SET risk_weight = 2.5, var_contribution = 0.025
    WHERE trade_id = '{first_trade}'
""")

# Trade cancellation (DELETE) — writes deletion vector
spark.sql(f"""
    DELETE FROM {TRADE_TABLE}
    WHERE trade_id = '{last_trade}'
""")

# Verify deletion vectors exist and show file paths
dv_files = spark.sql(f"SELECT file_path, content, record_count FROM {TRADE_TABLE}.delete_files")
dv_files.show(truncate=False)

# Verify reads post deletions
post_dv_count = spark.sql(f"SELECT COUNT(*) FROM {TRADE_TABLE}").collect()[0][0]
print(f"  ✅ Table readable after DVs: {post_dv_count} rows (was {row_count})")

spark.stop()
