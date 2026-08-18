# Native Geospatial and Variant Support in Iceberg v3 on AWS Glue 6.0

This directory contains the code artifacts for the AWS Big Data Blog post: **Introducing Native Geospatial and Variant Support in Iceberg v3 on AWS Glue 6.0**.

It demonstrates the new Apache Iceberg v3 column-level capabilities in AWS Glue 6.0 using a connected vehicle fleet monitoring use case, all in a single Iceberg v3 table:

- **Geospatial types** — `GEOMETRY` (with spatial predicates such as `ST_Intersects`) and `GEOGRAPHY` (stored natively)
- **Nanosecond-precision timestamps** — `TIMESTAMP_NTZ(9)`
- **VARIANT** — semi-structured sensor payloads with automatic shredding, queried with `variant_get()`
- **DEFAULT** column values

## Prerequisites

- An AWS account and a Region where AWS Glue 6.0 is available
- An IAM role with permissions to deploy AWS CloudFormation stacks and create AWS Glue, Amazon S3, and CloudWatch Logs resources

## Deploy

Deploy the stack using the AWS Console or CLI. No parameters are required.

```bash
aws cloudformation create-stack \
  --stack-name glue6-iceberg-v3-types \
  --template-body file://template.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --region <region>
```

The stack creates:
- An S3 bucket for Iceberg table storage (public access blocked, AES256 encryption)
- An IAM role scoped to the demo's database, table, and bucket
- A Glue database (`fleet_monitoring_<account-id>`)
- 2 AWS Glue 6.0 jobs: ingestion and queries

Stack creation takes approximately 2–5 minutes.

### Run the Jobs

Run the jobs in this order (job names use your account ID):

1. **Ingestion** — creates the Iceberg v3 table and inserts sample telemetry:

```bash
aws glue start-job-run --job-name fleet-telemetry-ingest-<account-id> --region <region>
```

2. **Queries** — demonstrates all four capabilities (after ingestion succeeds):

```bash
aws glue start-job-run --job-name fleet-telemetry-queries-<account-id> --region <region>
```

Check CloudWatch Logs (`/aws-glue/jobs/output`) for each run to see the query results.

## Notes

- Geospatial types require the Spark configuration `spark.sql.geospatial.enabled=true`, which the template already sets in each job's `--conf` argument. The other v3 types (TIMESTAMP_NTZ, VARIANT, DEFAULT) need no extra configuration.
- Spatial querying in this example uses the `GEOMETRY` column (`ST_Intersects` geofence). `GEOGRAPHY` is stored natively and is portable to any Iceberg v3-compatible engine; geodesic spatial predicates over `GEOGRAPHY` are engine-dependent.

## Security

- **No public access** on any resource
- S3 bucket: `PublicAccessBlockConfiguration` enabled (all four flags) and default encryption (AES256)
- IAM role follows the least-privilege principle, scoped to the demo's Glue database, table, log group, and bucket

## Clean Up

Delete the CloudFormation stack to remove all resources. The S3 bucket is emptied automatically during stack deletion.

```bash
aws cloudformation delete-stack --stack-name glue6-iceberg-v3-types --region <region>
```

## Repository Structure

```
├── template.yaml              # CloudFormation template
├── architecture.drawio        # Architecture diagram (draw.io)
├── scripts/
│   ├── fleet_telemetry.py     # Creates the Iceberg v3 table and inserts sample telemetry
│   └── fleet_queries.py       # Demonstrates geospatial, nanosecond, variant, and default queries
└── README.md
```

## License

This library is licensed under the MIT-0 License. See the [LICENSE](../../LICENSE.txt) file.

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
