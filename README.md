# AWS Glue ETL Code Samples

This repository has samples that demonstrate various aspects of the new
[AWS Glue](https://aws.amazon.com/glue) service, as well as various
AWS Glue utilities.

You can find the AWS Glue open-source Python libraries in a separate
repository at: [awslabs/aws-glue-libs](https://github.com/awslabs/aws-glue-libs).

### Content

 - [FAQ and How-to](FAQ_and_How_to.md)

   Helps you get started using the many ETL capabilities of AWS Glue, and
   answers some of the more common questions people have.

### Examples
 You can run these sample job scripts on any of AWS Glue ETL jobs, [container](https://aws.amazon.com/blogs/big-data/developing-aws-glue-etl-jobs-locally-using-a-container/), or [local environment](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html).

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

 - [Converting character encoding](examples/converting_char_encoding.md)
 
   This sample ETL script shows you how to use AWS Glue job to convert character encoding.

### Utilities

 - [Hive metastore migration](utilities/Hive_metastore_migration/README.md)

   This utility can help you migrate your Hive metastore to the
   AWS Glue Data Catalog.

 - [Crawler undo and redo](utilities/Crawler_undo_redo/README.md)

   These scripts can undo or redo the results of a crawl under
   some circumstances.

 - [Spark UI](utilities/Spark_UI/README.md)

   You can use this Dockerfile to run Spark history server in your container.
   See details: [Launching the Spark History Server and Viewing the Spark UI Using Docker ](https://docs.aws.amazon.com/glue/latest/dg/monitor-spark-ui-history.html#monitor-spark-ui-history-local)

 - [use only IAM access controls](utilities/use_only_IAM_access_controls/README.md)
 
   AWS Lake Formation applies its own permission model when you access data in Amazon S3 and metadata in AWS Glue Data Catalog through use of Amazon EMR, Amazon Athena and so on. 
   If you currently use Lake Formation and instead would like to use only IAM Access controls, this tool enables you to achieve it.

### GlueCustomConnectors
AWS Glue provides [built-in support](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-connect.html) for the most commonly used data stores such as Amazon Redshift, MySQL, MongoDB. Powered by Glue ETL Custom Connector, you can subscribe a third-party connector from AWS Marketplace or build your own connector to connect to data stores that are not natively supported.

 ![marketplace](GlueCustomConnectors/marketplace.jpg)
 
 - [Development](GlueCustomConnectors/development/README.md)

   Development guide with examples of connectors with simple, intermediate, and advanced functionalities. These examples demonstrate how to implement Glue Custom Connectors based on Spark Data Source or [Amazon Athena Federated Query](https://github.com/awslabs/aws-athena-query-federation) interfaces and plug them into Glue Spark runtime.

 - [Local Validation Tests](GlueCustomConnectors/localValidation/README.md)

   This user guide describes validation tests that you can run locally on your laptop to integrate your connector with Glue Spark runtime.
   
 - [Validation](GlueCustomConnectors/glueJobValidation/README.md)

   This user guide shows how to validate connectors with Glue Spark runtime in a Glue job system before deploying them for your workloads.

 - [Glue Spark Script Examples](GlueCustomConnectors/gluescripts/README.md)

   Python scripts examples to use Spark, Amazon Athena and JDBC connectors with Glue Spark runtime.

 - [Create and Publish Glue Connector to AWS Marketplace](GlueCustomConnectors/marketplace)

   If you would like to partner or publish your Glue custom connector to AWS Marketplace, please refer to this guide and reach out to us at glue-connectors@amazon.com for further details on your connector.

## License Summary

This sample code is made available under the MIT-0 license. See the LICENSE file.
