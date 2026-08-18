# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import sys
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['BOOTSTRAP_SERVERS'])
BOOTSTRAP = args['BOOTSTRAP_SERVERS']

spark = SparkSession.builder.getOrCreate()
print(f"Spark version: {spark.version}")
print(f"Bootstrap: {BOOTSTRAP}")

# Create topics via Kafka AdminClient
jvm = spark._jvm
props = jvm.java.util.Properties()
props.put("bootstrap.servers", BOOTSTRAP)
props.put("security.protocol", "SASL_SSL")
props.put("sasl.mechanism", "AWS_MSK_IAM")
props.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
props.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler")

admin = jvm.org.apache.kafka.clients.admin.AdminClient.create(props)
NewTopic = jvm.org.apache.kafka.clients.admin.NewTopic

topic1 = NewTopic("trade-risk-vectors", 2, jvm.java.lang.Short.valueOf(2))
topic2 = NewTopic("trade-alerts", 2, jvm.java.lang.Short.valueOf(2))
topics = jvm.java.util.ArrayList()
topics.add(topic1)
topics.add(topic2)

try:
    admin.createTopics(topics).all().get()
    print("✅ Topics created: trade-risk-vectors, trade-alerts")
except Exception as e:
    if "TopicExistsException" in str(e):
        print("✅ Topics already exist")
    else:
        print(f"❌ Topic creation failed: {str(e)[:200]}")
admin.close()
spark.stop()
