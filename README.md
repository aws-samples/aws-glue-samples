# AWS Glue ETL Code Samples

This repository has samples that demonstrate various aspects of the new
[AWS Glue](https://aws.amazon.com/glue) service, as well as various
AWS Glue utilities.

These are all licensed under the [Amazon Software License](http://aws.amazon.com/asl/)
(the "License"). They may not be used except in compliance with the License, a copy
of which is included here in the LICENSE file.

You can find the AWS Glue open-source Python libraries in a separate
repository at: [awslabs/aws-glue-libs](https://github.com/awslabs/aws-glue-libs).

### Content

 - [FAQ and How-to](FAQ_and_How_to.md)

   Helps you get started using the many ETL capabilities of AWS Glue, and
   answers some of the more common questions people have.

 - [Join and Relationalize Data in S3](examples/join_and_relationalize.md)

   This sample ETL script shows you how to use AWS Glue to load, transform,
   and rewrite data in AWS S3 so that it can easily and efficiently be queried
   and analyzed.

 - [Clean and Process](examples/data_cleaning_and_lambda.md)

   This sample ETL script shows you how to take advantage of both Spark and
   AWS Glue features to clean and transform data for efficient analysis.

 - [The `resolveChoice` Method](examples/resolve_choice.md)

   This sample explores all four of the ways you can resolve choice types
   in a dataset using DynamicFrame's `resolveChoice` method.

 - [Hive metastore migration](utilities/Hive_metastore_migration/README.md)

   This utility can help you migrate your Hive metastore to the
   AWS Glue Data Catalog.

 - [Crawler undo and redo](utilities/Crawler_undo_redo/README.md)

   These scripts can undo or redo the results of a crawl under
   some circumstances.