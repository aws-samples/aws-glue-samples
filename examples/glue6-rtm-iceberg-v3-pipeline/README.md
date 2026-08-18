# Real-Time Market Risk Pipeline with AWS Glue 6.0 and Apache Iceberg v3

This directory contains the code artifacts for the AWS Big Data Blog post: **Build a Real-Time Market Risk Pipeline with AWS Glue 6.0, Apache Iceberg v3, and Spark Real-Time Mode**.

## Architecture

The pipeline processes trade pricing vectors from a bank's front-office systems through three paths:

1. **Real-Time Path (Spark RTM, Scala)** — Reads trades from Amazon MSK, evaluates risk thresholds, and writes alerts to a downstream Kafka topic within milliseconds.
2. **Near-Real-Time Path (Micro-batch, PySpark)** — Reads from the same MSK topic and stores the full trade history in an Iceberg v3 table with Variant columns and shredding enabled.
3. **Batch Path (PySpark, Arrow UDFs)** — Reads from the Iceberg v3 table, extracts risk metrics from Variant columns using `variant_get()`, and computes Value at Risk (VaR) using Arrow-native UDFs.

### AWS Glue 6.0 Features Demonstrated

| Feature | Where Used |
|---------|-----------|
| Spark Real-Time Mode (RTM) | RTM alerts job — millisecond-level alerting |
| Iceberg v3 Variant type | NRT ingestion — `PARSE_JSON()` stores heterogeneous pricing vectors natively |
| Variant shredding | NRT ingestion — auto-infers schema for columnar performance |
| `variant_get()` extraction | Batch VaR — type-safe extraction from nested Variant fields |
| Arrow-native UDFs (`@arrow_udf`) | Batch VaR — vectorized VaR computation (1000 Monte Carlo scenarios) |
| Deletion vectors (merge-on-read) | Batch VaR — fast trade amendments and cancellations |
| Column DEFAULT values | NRT ingestion — auto-populated fields |

## Prerequisites

- An AWS account and a Region where AWS Glue 6.0 is available
- A VPC with at least 2 subnets in different Availability Zones
- A dedicated security group that allows all inbound TCP from itself (self-referencing rule). Glue requires all ports open between workers. Do not use the default VPC security group.
- The VPC's main route table ID (for the S3 gateway endpoint)

## Deploy

Deploy the stack using the AWS Console or CLI:

```bash
aws cloudformation create-stack \
  --stack-name glue6-rtm-pipeline \
  --template-body file://template.yaml \
  --parameters \
    ParameterKey=VpcId,ParameterValue=<your-vpc-id> \
    ParameterKey=SubnetIds,ParameterValue="<subnet-1>\,<subnet-2>" \
    ParameterKey=SecurityGroupId,ParameterValue=<your-sg-id> \
    ParameterKey=RouteTableId,ParameterValue=<your-route-table-id> \
  --capabilities CAPABILITY_NAMED_IAM \
  --region <region>
```

The stack creates:
- A provisioned Amazon MSK cluster (2 brokers, IAM auth, TLS encryption, no public access)
- An S3 bucket for Iceberg table storage and checkpoints (public access blocked)
- An S3 gateway VPC endpoint and a Glue interface VPC endpoint for private access
- A Glue network connection for MSK VPC connectivity
- A Glue database (`risk_analytics_<account-id>_glue6b1`)
- An IAM role scoped to the pipeline's resources
- The two Kafka topics (`trade-risk-vectors`, `trade-alerts`), created automatically
- 4 AWS Glue 6.0 jobs: producer, RTM alerts, NRT ingestion, and batch VaR

Stack creation takes approximately 15–20 minutes (MSK cluster provisioning). The Kafka topics are created for you during stack creation, so no separate topic-creation step is required.

### Run the Pipeline

After the stack completes, run the jobs in this order (job names use your account ID):

1. **Start both consumers** (RTM alerts + NRT ingestion), in parallel:

```bash
aws glue start-job-run --job-name rtm-alerts-<account-id>-glue6b1 --region <region>
aws glue start-job-run --job-name nrt-ingestion-<account-id>-glue6b1 --region <region>
```

2. **Once both show RUNNING** in the Glue Console, **start the producer**:

```bash
aws glue start-job-run --job-name producer-<account-id>-glue6b1 --region <region>
```

3. **After the producer finishes**, run the batch VaR job:

```bash
aws glue start-job-run --job-name batch-var-<account-id>-glue6b1 --region <region>
```

## View Results

Check CloudWatch Logs (`/aws-glue/jobs/output`) for each job's run ID to see:
- **RTM alerts:** Sub-second latency table showing trade → alert timing
- **NRT ingestion:** Row counts and sample `variant_get()` queries on the Iceberg v3 table
- **Batch VaR:** Arrow UDF VaR results, risk summary by desk/asset class, deletion vector demo

## Parameters

| Parameter | Description |
|-----------|-------------|
| `VpcId` | VPC for the MSK cluster. Must have DNS resolution enabled. |
| `SubnetIds` | At least 2 subnets in different AZs within the VPC. |
| `SecurityGroupId` | A dedicated security group allowing all inbound TCP from itself (Glue requires all ports open between workers). Do not use the default VPC security group. |
| `RouteTableId` | Main route table ID (for the S3 gateway endpoint). Find it in VPC Console → Route Tables → Main=Yes. |

## Security

- **No public access** on any resource
- S3 bucket: `PublicAccessBlockConfiguration` enabled (all four flags) and default encryption (AES256)
- MSK: IAM authentication only, unauthenticated access disabled, TLS enforced
- All Glue jobs run inside the VPC via a Glue network connection
- S3 access via a gateway endpoint (traffic stays on the AWS backbone)
- IAM role follows the least-privilege principle, scoped to the pipeline's cluster, topics, database, and bucket

## Clean Up

Delete the CloudFormation stack to remove all resources:

```bash
aws cloudformation delete-stack --stack-name glue6-rtm-pipeline --region <region>
```

The S3 bucket is emptied automatically during stack deletion, so no manual bucket cleanup is required.

## Cost

Estimated cost while the pipeline is running:

| Resource | Cost |
|----------|------|
| MSK (2x kafka.m5.large) | ~$0.35/hour |
| Glue jobs (G.1X workers) | ~$0.44/DPU-hour |
| S3 storage | Negligible for demo data |

**Important:** Delete the stack when done to avoid ongoing MSK charges.

## Repository Structure

```
├── template.yaml              # CloudFormation template
├── architecture.drawio        # Architecture diagram (draw.io)
├── scripts/
│   ├── create_topics.py       # Reference topic-creation script (topics are auto-created by the stack)
│   ├── producer.py            # Generates trade events (equities, FX, rates)
│   ├── rtm_alerts.scala       # Spark RTM — risk scoring → alerts topic
│   ├── nrt_ingestion.py       # Micro-batch → Iceberg v3 (Variant + shredding)
│   └── batch_var.py           # Arrow UDF VaR + variant_get + deletion vectors
└── README.md
```

## License

This library is licensed under the MIT-0 License. See the [LICENSE](../../LICENSE.txt) file.

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
