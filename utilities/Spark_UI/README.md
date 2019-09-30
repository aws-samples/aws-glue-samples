## Spark UI

### Launching the Spark History Server and Viewing the Spark UI Using Docker

If you prefer local access (not to have EC2 instance for Apache Spark history server), you can also use a Docker to start the Apache Spark history server and view the Spark UI locally. This Dockerfile is a sample that you should modify to meet your requirements. 

**Pre-requisite**
- Install Docker

**To start the Spark history server and view the Spark UI using Docker**

1. Download a Dockerfile from GitHub
2. Run commands shown below
    - Replace **AWS_ACCESS_KEY_ID** and **AWS_SECRET_ACCESS_KEY** with your valid AWS credential
    - Replace **s3a://path_to_eventlog** with your event log directory
    ``` 
        $ docker build -t glue/sparkui:latest . 
        $ docker run -itd -e SPARK_HISTORY_OPTS="$SPARK_HISTORY_OPTS -Dspark.history.fs.logDirectory=s3a://path_to_eventlog -Dspark.hadoop.fs.s3a.access.key=AWS_ACCESS_KEY_ID -Dspark.hadoop.fs.s3a.secret.key=AWS_SECRET_ACCESS_KEY" -p 18080:18080 glue/sparkui:latest "/opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer"
    ```
3. Open http://localhost:18080 in your browser
