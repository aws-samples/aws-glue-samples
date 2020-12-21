# Glue Custom Connectors: Local Validation Tests Guide

These validation tests support developers to test a connector locally with their data store. This also enables to test the integration of the connector with Glue Spark runtime and its features.

This guide shows how to set up the environment, configure and run each test.

***You should test your connectors in AWS Glue Job system to further validate before using them for your workloads. 
Please refer to [Glue Job Validation Test Guide](../glueJobValidation/README.md) for more details on testing with AWS Glue Job System. ***

## Implement a connector

These tests assume you have a connector ready for test. 
Please refer to the [Development Guide](../development/README.md) section on connector implementation.

## Prerequisite

User will need to set up local credential to access some AWS service for some tests such as secret manager and writing data to S3. 
User may setup AWS CLI and set their local credentials with command `aws configure` which can generate the AWS credential file *~/.aws/credentials*, will be used by glue local job run.

## Setup Environment

Please refer to [GlueSparkRuntime](../development/GlueSparkRuntime/README.md) to set up your local testing environment. Here we highlight the key steps of the configuration.

1. Build a local Scala environment with local Glue ETL maven library:
    1. Follow the Scala section to pull the library: [Developing Locally with Scala](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html).
    2. Add the [ScalaTest](https://www.scalatest.org/user_guide) dependency to the project pom.xml.
    ```
     <dependency>
            <groupId>org.scalatest</groupId>
            <artifactId>scalatest_2.11</artifactId>
            <version>3.0.5</version>
            <scope>test</scope>
        </dependency>
    ```
    3. Copy the integration tests to the local project directory.
    4. Build the project.

2. If your connector is already packaged as a jar, you may install the connector jar in local maven repository,
    ```text
       mvn install:install-file \
         -Dfile="path_to_connector.jar" \
         -DgroupId="your_groupId" \
         -DartifactId="your_artifactId" \
         -Dversion="your_version" \
         -Dpackaging="jar"
    ```
    and add the installed connector dependency to the project pom.xml.
    ```text
      <dependency>
          <groupId>your_groupId</groupId>
          <artifactId>your_artifactId</artifactId>
          <version>your_version</version>
      </dependency>
    ```

## Validation Tests for different Connector Interfaces
The table below shows the test, functionality and features, and the corresponding connector interfaces. Please refer to the AWS Glue public doc for [Job Authoring with Custom Connectors](https://docs.aws.amazon.com/glue/latest/ug/connectors-chapter.html#job-authoring-custom-connectors) to understand the specific feature options.

| Test Name  | Description | JDBC | Spark | Athena |
|---|---|:----:|:----:|:----:|
|DataSourceTest|Test connector connectivity and reading functionality.| x | x | x |
|ReadWriteTest|Test reading and writing end-to-end workflow.| x | x | x |
|CatalogConnectionTest|Test catalog connection integration.| x | x | x |
|DataSchemaTest|Test data schema from reading with the connector.| x | x | x |
|SecretsManagerTest|Test AWS Secrets Manager integration.| x | x | - |
|DataSinkTest|Test connector connectivity and writing functionality| x | x | - |
|ColumnPartitioningTest|Test connector column partitioning functionality.| x | - | - |
|FilterPredicateTest|Test connector filter predicate functionality.| x | - | - |
|JDBCUrlTest|Test connector extra parameters for JDBC Url functionality.| x | - | - |
|DbtableQueryTest|Test connector dbTable/query option functionality.| x | - | - |
|DataTypeMappingTest|Test connector custom data type mapping functionality.| x | - | - |

**Note:**
1. Functionality such as AWS Glue job bookmarks that allow incremental loads can be tested on the AWS Glue job system only.
2. Please read through each test to understand the test workflow.

## Configure and Run Test
1. Please make sure the testing data source is up and connection information is correct.
    For JDBC connectors, developer can verify the connection first with the JDBC driver.
2. Run the tests and verify if they pass.
    Developer can configure the validation logic for their specific data source.

## Test Connector In AWS Glue Job System and Update Support Document

***Developers should further test their connector in AWS Glue Job system before releasing it to AWS Marketplace or using it with their workloads with the BYOC Custom Connector flow in AWS Glue.***

1. Please follow [Glue job validation guide](../glueJobValidation/README.md) to validate the connector on AWS Glue.
2. To test marketplace workflow for the connector in Glue job system, please replace the `connectionType` from `custom.jdbc|spark|athena` to `marketplace.jdbc|spark|athena`.
3. Developer will need to test Job Bookmark for their connectors in AWS Glue Job system as this feature is not supported locally.
4. Once developer test the connector on AWS Glue Job System, please publish the list of validation tests in your AWS Marketplace listing also.
5. AWS Glue job system Spark script examples can be found in the [Glue Job Scripts](../gluescripts/README.md) section.
    
***Note: if you have a large testing data set, please consider using column partitioning for better performance.***

    