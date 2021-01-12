# Glue Custom Connectors: Job Validation Guide

This guide shows how you can package, validate and deploy your connector on the AWS Glue job system.

## Implement a connector

These tests assume you have developed a connector ready that has been validated locally with your tests. 

## Validation Tests for different Connector Interfaces
The table below shows the test, functionality and features, and the corresponding connector interfaces. Please refer to the AWS Glue public doc for [Job Authoring with Custom Connectors](https://docs.aws.amazon.com/glue/latest/ug/connectors-chapter.html#job-authoring-custom-connectors) to understand the specific feature options.

| Test Name  | Description | JDBC | Spark | In Testing Script |
|---|---|:----:|:----:|:----:|
|DataSourceTest|Test connector connectivity and reading functionality.| x | x | testing option template provided |
|ColumnPartitioningTest|Test Partner JDBC connector column partitioning functionality.| x | - | testing option template provided |
|DataTypeMappingTest|Test Partner JDBC connector custom data type mapping functionality.| x | - | testing option template provided |
|DbtableQueryTest|Test Partner JDBC connector dbTable/query option functionality.| x | - | testing option template provided |
|JDBCUrlTest|Test Partner JDBC connector extra connection options functionality.| x | - | testing option template provided |
|SecretsManagerTest|Test secrets manager integration.| x | x | testing option template provided |
|FilterPredicateTest|Test Partner JDBC connector filter predicate functionality.| x | - | testing option template provided |
|ReadWriteTest|Test reading and writing end-to-end workflow.| x | x | include 'write data to s3' as part of the test |
|DataSinkTest|Test connector connectivity and writing functionality| x | x | check 'DataSinkTest' part |
|DataSchemaTest|Test data schema from reading with the connector.| x | x | include 'validate data reading' part in the test |  

**Note:**
Please read through each test to understand the test workflow.

## Configure and Run Test
1. Please make sure the testing data source is up and connection information is correct.
    1. For JDBC connectors, developer can verify the connection first with the JDBC driver.
2. You may use glue_job_validation.py to test your connector in AWS Glue ETL job system.
    1. For your specific test, please select and customize the right testing connection option.
    2. Use the selected connection option to create the dynamic frame.
    3. Note: for JDBC connector, if you have a large testing data set, please consider using column partitioning in your test for better performance.
3. Run the tests and check if it pass.
    1. You may configure the validation logic for your specific data source and test scenarios.
    2. It is important to make sure the testing jobs succeeded and verify the testing output data has the expected data size, schema etc. 
    Please see Section "validate data reading" in the validation script for a simple schema and data size validation example.

## Reporting
Once validation is done, please document the test matrix to indicate what tests pass/fail.
1. An example test matrix template with Glue job runs:

| Test Name  | Tested | Passed | Jobrun Id |
|---|:----:|:----:|:----:|
|DataSourceTest| x | Y | jr_1 |
|ColumnPartitioningTest| - | - | jr_2 |
|DataTypeMappingTest| - | - | jr_3 |
|DbtableQueryTest| - | - | jr_4 |
|JDBCUrlTest| - | - | jr_5 |
|SecretsManagerTest| x | Y | jr_6 |
|FilterPredicateTest| - | - | jr_7 |
|ReadWriteTest| x | Y | jr_8 |
|DataSinkTest| x | N | jr_9 |
|DataSchemaTest| x | Y | jr_10 |

2. Please also document the validation details from log for the schema and data size check for each test.
Please see Section "validate data reading" in the validation script for code snippet to print out the validation result. 
An example screenshot is also included for your reference.
    1. Expected schema and output schema.
    2. Expected data record count and output record count.

3. Please reach out to us at glue-connectors@amazon.com if you would like to partner or add your connector to AWS Marketplace. You can also reference the guide to [Publish your connectors to AWS Marketplace](marketplace/publishGuide.pdf)