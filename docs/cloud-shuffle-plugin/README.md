# Using AWS Glue Cloud Shuffle Plugin for Apache Spark with Other Cloud Storage Services

This document describes how to use [Cloud Shuffle Plugin](https://docs.aws.amazon.com/glue/latest/dg/monitor-spark-shuffle-manager.html) with other Cloud Storage Service other than natively supported Amazon S3.

## Use Cloud Shuffle Plugin with Google Cloud Storage
1. Get Google Cloud Storage Connector for Spark and Hadoop library.
    1. You can get the library from [Maven repository](https://mvnrepository.com/artifact/com.google.cloud.bigdataoss/gcs-connector/).
        1. Choose the appropriate version of GCS connector.
        2. Choose **View All** under **Files**.
        3. Download the file with `-shaded.jar` (e.g. [gcs-connector-hadoop3-2.2.3-shaded.jar](https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.3/gcs-connector-hadoop3-2.2.3-shaded.jar))
    2. Use spark-submit `--packages` to import the jars
2. Create a Google Cloud Storage bucket `<GCS Shuffle Bucket>` for shuffle files storage.
3. Add the Cloud Shuffle Plugin jar and the shaded jar of Google Cloud Storage Connector for Spark and Hadoop into Spark driver/executor Classpath.
    1. `spark.driver.extraClassPath=<path to jars>`
    2. `spark.executor.extraClassPath=<path to jars>`
4. Set up the permission to access the shuffle bucket, and use [appropriate authentication mechanism](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md#authentication) to pass the credentials to the filesystem.
5. Set the following Spark configurations for working with Google Cloud Storage.
   1.  ```
       --conf spark.shuffle.sort.io.plugin.class=com.amazonaws.spark.shuffle.io.cloud.ChopperPlugin \
       --conf spark.shuffle.storage.path=gs://<GCS Shuffle Bucket>/<shuffle dir> \
       --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
       ```
6. Set up the permission to access the shuffle bucket, choose [appropriate authentication mechanism](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md#authentication), and set the credentials for Google Cloud Storage. Here's two examples for authentication:
    1. JSON keyfile service account authentication
        1. ```
           --conf spark.hadoop.fs.gs.auth.type=SERVICE_ACCOUNT_JSON_KEYFILE \
           --conf spark.hadoop.fs.gs.auth.service.account.json.keyfile=<Your JSON key file path. The file must exist at the same path on all nodes>
           ```
    2. Google Compute Engine service account authentication (Be careful not to set the key in plaintext, instead you should load the private key and set programmatically via `SparkConf`.)
       1. ```
          --conf spark.hadoop.fs.gs.auth.service.account.enable=true \
          --conf spark.hadoop.fs.gs.auth.service.account.email=<Your Service Account email> \
          --conf spark.hadoop.fs.gs.auth.service.account.private.key.id=<Your private key ID extracted from the credential's JSON>
          --conf spark.hadoop.fs.gs.auth.service.account.private.key=<Your private key extracted from the credential's JSON>

For details about configurations and authentication, see https://github.com/GoogleCloudDataproc/hadoop-connectors/ and https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md.

## Use Cloud Shuffle Plugin with Microsoft Azure Blob Storage
1. Get hadoop-azure library.
    1. You can get the library from [Maven repository](https://mvnrepository.com/artifact/com.google.cloud.bigdataoss/gcs-connector/hadoop3-2.2.3).
2. Get azure-storage library.
    1. You can get the library from [Maven repository](https://mvnrepository.com/artifact/com.microsoft.azure/azure-storage).
3. Create a storage account in Microsoft Azure.
4. Create a Blob storage container `<Azure Blob container>` for shuffle files storage.
5. Add the Cloud Shuffle Plugin jar, hadoop-azure jar, and azure-storage jar into Spark driver/executor Classpath.
    1. `spark.driver.extraClassPath=<path to jars>`
    2. `spark.executor.extraClassPath=<path to jars>`
6. Set up the permission to access the shuffle bucket, and use [appropriate authentication mechanism](https://hadoop.apache.org/docs/current/hadoop-azure/index.html#Configuring_Credentials) to pass the credentials to the filesystem.
7. Set the following Spark configurations for working with Microsoft Azure Blob Storage.
    ```
    --conf spark.shuffle.sort.io.plugin.class=com.amazonaws.spark.shuffle.io.cloud.ChopperPlugin \
    --conf spark.shuffle.storage.path=wasbs://<Azure Blob container>@<Storage account>.blob.core.windows.net/<shuffle dir> \
    --conf spark.hadoop.fs.azure.account.key.<Storage account>.blob.core.windows.net=<Your Azure Key>
    ```
For details about configurations, see https://hadoop.apache.org/docs/current/hadoop-azure/.