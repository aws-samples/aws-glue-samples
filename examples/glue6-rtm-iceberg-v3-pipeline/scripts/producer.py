# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import sys
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
import json, random, time
from datetime import datetime

args = getResolvedOptions(sys.argv, ['BOOTSTRAP_SERVERS'])
BOOTSTRAP = args['BOOTSTRAP_SERVERS']

spark = SparkSession.builder.getOrCreate()
print(f"Spark version: {spark.version}")
print(f"Bootstrap: {BOOTSTRAP}")

TOPIC = "trade-risk-vectors"
REGIONS = ['EMEA', 'AMER', 'APAC']

# Produce continuously for 3.5 minutes (batches of 20 every 5 seconds)
DURATION_SECONDS = 210
BATCH_SIZE = 20
start_time = time.time()
total_produced = 0
batch_num = 0

print(f"✅ Starting continuous producer for {DURATION_SECONDS}s...")

while time.time() - start_time < DURATION_SECONDS:
    batch_num += 1
    trades = []
    for i in range(BATCH_SIZE):
        r = random.random()
        if r < 0.4:
            asset_class = "EQUITY"
            delta = round(random.uniform(0.2, 0.9), 4)
            notional = random.choice([1000000, 5000000, 10000000, 25000000, 55000000])
            pv = json.dumps({"greeks":{"delta":delta,"gamma":round(random.uniform(0.01,0.1),4),"vega":round(random.uniform(5,30),2),"theta":round(-random.uniform(0.3,2),2)},"spot_price":round(random.uniform(50,500),2),"notional":notional,"scenarios":[{"shift":"-10pct","pnl":-int(notional*delta*0.1),"breakdown":{"by_sector":{"financials":-int(notional*delta*0.04),"tech":-int(notional*delta*0.03),"energy":-int(notional*delta*0.03)},"by_region":{"US":-int(notional*delta*0.06),"EU":-int(notional*delta*0.04)}}}],"model":"BlackScholes","model_params":{"vol_type":"implied","surface":{"30d":round(random.uniform(0.15,0.35),4),"60d":round(random.uniform(0.14,0.30),4),"90d":round(random.uniform(0.13,0.28),4)}}})
            desk = random.choice(["Equity Derivatives", "Equity Flow"])
            book_id = f"BOOK-EQ-{random.randint(1,5):02d}"
        elif r < 0.7:
            asset_class = "FX"
            notional = random.choice([10000000, 25000000, 50000000, 60000000])
            pv = json.dumps({"spot_rate":round(random.uniform(0.8,1.5),4),"forward_points":{"1M":round(random.uniform(-20,-5),1),"3M":round(random.uniform(-50,-15),1),"6M":round(random.uniform(-100,-30),1)},"vol_surface":{"25D_RR":round(random.uniform(-1,1),2),"ATM":round(random.uniform(5,15),2),"skew":{"short_term":{"1W":round(random.uniform(-0.5,0.5),3),"1M":round(random.uniform(-0.8,0.8),3)},"long_term":{"3M":round(random.uniform(-1.0,1.0),3),"1Y":round(random.uniform(-1.5,1.5),3)}}},"notional_usd":notional,"pair":random.choice(["EURUSD","GBPUSD","USDJPY"]),"model":"SABR","hedge_portfolio":{"delta_hedge":{"notional":int(notional*0.6),"instrument":"spot"},"vega_hedge":{"notional":int(notional*0.2),"instrument":"1M_ATM_straddle","strike":round(random.uniform(1.05,1.15),4)}}})
            desk = random.choice(["G10 FX Options", "EM FX Spot"])
            book_id = f"BOOK-FX-{random.randint(1,5):02d}"
        else:
            asset_class = "RATES"
            dv01 = random.choice([100000, 300000, 500000, 700000, 1000000])
            notional = dv01 * 150
            pv = json.dumps({"curve_sensitivities":{"1Y":{"pv01":int(dv01*0.05),"convexity":round(random.uniform(0.0001,0.001),5)},"2Y":{"pv01":int(dv01*0.1),"convexity":round(random.uniform(0.0002,0.002),5)},"5Y":{"pv01":int(dv01*0.2),"convexity":round(random.uniform(0.0005,0.005),5)},"10Y":{"pv01":int(dv01*0.4),"convexity":round(random.uniform(0.001,0.01),5)},"30Y":{"pv01":int(dv01*0.25),"convexity":round(random.uniform(0.002,0.02),5)}},"dv01":dv01,"convexity":round(random.uniform(0.0005,0.003),4),"notional":notional,"swap_rate":round(random.uniform(0.02,0.05),4),"float_index":random.choice(["SOFR","EURIBOR"]),"model":"HullWhite","model_params":{"mean_reversion":round(random.uniform(0.01,0.1),4),"volatility":round(random.uniform(0.005,0.02),5),"calibration":{"method":"swaption","instruments":["2Y5Y","5Y10Y","10Y30Y"],"fit_error":round(random.uniform(0.0001,0.001),5)}}})
            desk = random.choice(["Rates Trading", "Credit Trading"])
            book_id = f"BOOK-IR-{random.randint(1,9):02d}"

        trade = {"trade_id": f"TRD-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{batch_num:03d}-{i:03d}",
                 "book_id": book_id, "desk": desk, "asset_class": asset_class,
                 "execution_time": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'),
                 "pricing_vector": pv,
                 "var_contribution": round(random.uniform(0.01, 0.15), 4),
                 "risk_weight": round(random.uniform(1.0, 3.0), 2),
                 "region": random.choice(REGIONS)}
        trades.append((trade["trade_id"], json.dumps(trade)))

    df = spark.createDataFrame(trades, ["key", "value"])
    df.write.format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP) \
        .option("topic", TOPIC) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "AWS_MSK_IAM") \
        .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;") \
        .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler") \
        .save()

    total_produced += BATCH_SIZE
    print(f"  Batch {batch_num}: {BATCH_SIZE} trades sent (total: {total_produced})")
    time.sleep(5)

print(f"\n✅ Producer finished: {total_produced} trades in {DURATION_SECONDS}s")
spark.stop()
