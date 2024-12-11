## Spark UI

### Launching the Spark History Server and Viewing the Spark UI Using Docker

If you prefer local access (not to have EC2 instance for Apache Spark history server), you can also use a Docker to start the Apache Spark history server and view the Spark UI locally. This Dockerfile is a sample that you should modify to meet your requirements. 

#### Pre-requisite
- Install Docker

#### Build docker image
1. Download the Dockerfile and the pom file from the GitHub repository
2. Run commands shown below
    ``` 
        $ docker build -t glue/sparkui:latest . 
    ```


#### Start the Spark history server

**Using AWS named profile**
1.  Run commands shown below
    - Set **LOG_DIR** by replacing **s3a://path_to_eventlog** with your event log directory
    - Set **PROFILE_NAME** with your AWS named profile
    ``` 
        $ LOG_DIR="s3a://path_to_eventlog/"
        $ PROFILE_NAME="profile_name"
        $ docker run -itd -v ~/.aws:/root/.aws -e AWS_PROFILE=$PROFILE_NAME -e SPARK_HISTORY_OPTS="$SPARK_HISTORY_OPTS -Dspark.history.fs.logDirectory=$LOG_DIR  -Dspark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain" -p 18080:18080 glue/sparkui:latest "/opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer"
    ```

**Using a pair of AWS access key and secret key**
1.  Run commands shown below
    - Set **LOG_DIR** by replacing **s3a://path_to_eventlog** with your event log directory
    - Set **AWS_ACCESS_KEY_ID** and **AWS_SECRET_ACCESS_KEY** with your valid AWS credential
    ``` 
        $ LOG_DIR="s3a://path_to_eventlog/"
        $ AWS_ACCESS_KEY_ID="AKIAxxxxxxxxxxxx"
        $ AWS_SECRET_ACCESS_KEY="yyyyyyyyyyyyyyy"
        $ docker run -itd -e SPARK_HISTORY_OPTS="$SPARK_HISTORY_OPTS -Dspark.history.fs.logDirectory=$LOG_DIR -Dspark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID -Dspark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY" -p 18080:18080 glue/sparkui:latest "/opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer"
    ```

**Using AWS temporary credentials**
1.  Run commands shown below
    - Set **LOG_DIR** by replacing **s3a://path_to_eventlog** with your event log directory
    - Set **AWS_ACCESS_KEY_ID**, **AWS_SECRET_ACCESS_KEY**, and **AWS_SESSION_TOKEN** with your valid AWS credential
    ``` 
        $ LOG_DIR="s3a://path_to_eventlog/"
        $ AWS_ACCESS_KEY_ID="ASIAxxxxxxxxxxxx"
        $ AWS_SECRET_ACCESS_KEY="yyyyyyyyyyyyyyy"
        $ AWS_SESSION_TOKEN="zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
        $ docker-compose up -d
    
These configuration parameters come from the [Hadoop-AWS Module](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html). You may need to add specific configuration based on your use case. For example: users in isolated regions will need to configure the `spark.hadoop.fs.s3a.endpoint`.

For Beijing region, add following config:

```
-Dspark.hadoop.fs.s3a.endpoint=s3.cn-north-1.amazonaws.com.cn
```

For Ningxia region, add following config:
```
-Dspark.hadoop.fs.s3a.endpoint=s3.cn-northwest-1.amazonaws.com.cn
```


#### View the Spark UI using Docker
1. Open http://localhost:18080 in your browser
