// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object RTMAlerts {
  def main(args: Array[String]): Unit = {
    // Parse Glue job arguments (passed as --key value pairs)
    val argMap = args.grouped(2).collect { case Array(k, v) => k.stripPrefix("--") -> v }.toMap
    val bootstrapServers = argMap("BOOTSTRAP_SERVERS")
    val checkpointPath = argMap("CHECKPOINT_PATH")

    val spark = SparkSession.builder()
      .config("spark.sql.streaming.realTimeMode.allowlistCheck", "false")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    println(s"Spark version: ${spark.version}")
    println(s"Bootstrap: $bootstrapServers")

    val kafkaStream = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", "trade-risk-vectors")
      .option("startingOffsets", "earliest")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
      .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
      .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
      // Reduce fetch wait from default 500ms to 100ms for tighter RTM latency
      .option("kafka.fetch.max.wait.ms", "100")
      .option("kafka.fetch.min.bytes", "1")
      .load()

    // Capture Kafka's metadata timestamp (when MSK received the trade)
    val enriched = kafkaStream
      .select(
        col("value").cast("string").as("json_str"),
        col("timestamp").as("kafka_ingest_ts")
      )
      .selectExpr(
        "kafka_ingest_ts",
        "get_json_object(json_str, '$.trade_id') AS trade_id",
        "get_json_object(json_str, '$.asset_class') AS asset_class",
        "get_json_object(json_str, '$.desk') AS desk",
        "get_json_object(json_str, '$.region') AS region",
        "get_json_object(json_str, '$.execution_time') AS trade_created",
        "get_json_object(json_str, '$.pricing_vector') AS pv_raw"
      )
      .withColumn("notional",
        coalesce(
          get_json_object(col("pv_raw"), "$.notional").cast("double"),
          get_json_object(col("pv_raw"), "$.notional_usd").cast("double"),
          get_json_object(col("pv_raw"), "$.dv01").cast("double").multiply(150)
        ))
      .withColumn("risk_flag",
        when(col("notional") > 50000000, "CRITICAL")
          .when(col("notional") > 25000000, "HIGH")
          .otherwise("NORMAL"))
      .filter(col("risk_flag") =!= "NORMAL")
      .withColumn("alert_generated_at", current_timestamp())
      .select(col("trade_id").as("key"), to_json(struct(col("*"))).as("value"))

    // Write alerts to Kafka with RTM
    val alertQuery = enriched.writeStream.format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("topic", "trade-alerts")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
      .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
      .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
      .option("checkpointLocation", s"$checkpointPath/rtm_alerts/")
      .outputMode("update")
      .trigger(Trigger.RealTime("1 minute"))
      .start()

    println("✅ RTM Alert streaming started with Trigger.RealTime")

    // Run for 5 minutes
    Thread.sleep(300000)
    alertQuery.stop()
    println("✅ RTM Alert streaming stopped")

    // === Alert Summary with TRUE latency (MSK source ts → MSK sink ts) ===
    println("\n=== Alert Summary ===")
    val alerts = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", "trade-alerts")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
      .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
      .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
      .load()
      .select(
        col("value").cast("string").as("alert_json"),
        col("timestamp").as("alert_kafka_ts")  // When MSK received the alert
      )

    val alertCount = alerts.count()
    println(s"✅ Total alerts produced: $alertCount")

    // Latency = alert_kafka_ts - kafka_ingest_ts (both from MSK, same clock)
    alerts.select(
      get_json_object(col("alert_json"), "$.trade_id").as("trade_id"),
      get_json_object(col("alert_json"), "$.risk_flag").as("risk_flag"),
      get_json_object(col("alert_json"), "$.notional").as("notional"),
      get_json_object(col("alert_json"), "$.kafka_ingest_ts").as("trade_received_by_msk"),
      col("alert_kafka_ts").as("alert_received_by_msk"),
      (
        col("alert_kafka_ts").cast("double") -
        to_timestamp(get_json_object(col("alert_json"), "$.kafka_ingest_ts")).cast("double")
      ).as("rtm_latency_seconds")
    ).show(10, truncate = false)

    spark.stop()
  }
}
