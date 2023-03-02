FROM amazonlinux:2
FROM amazoncorretto:8
FROM maven:3.6-amazoncorretto-8

RUN yum install -y procps

WORKDIR /tmp/
ADD pom.xml /tmp
RUN curl -o ./spark-3.3.0-bin-without-hadoop.tgz https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-without-hadoop.tgz
RUN tar -xzf spark-3.3.0-bin-without-hadoop.tgz && \
    mv spark-3.3.0-bin-without-hadoop /opt/spark && \
    rm spark-3.3.0-bin-without-hadoop.tgz
RUN mvn dependency:copy-dependencies -DoutputDirectory=/opt/spark/jars/
RUN rm /opt/spark/jars/jsr305-3.0.0.jar && \
    rm /opt/spark/jars/jersey-*-1.19.jar && \
    rm /opt/spark/jars/jackson-dataformat-cbor-2.6.7.jar && \
    rm /opt/spark/jars/joda-time-2.8.1.jar && \
    rm /opt/spark/jars/jmespath-java-*.jar && \
    rm /opt/spark/jars/aws-java-sdk-core-*.jar && \
    rm /opt/spark/jars/aws-java-sdk-kms-*.jar && \
    rm /opt/spark/jars/aws-java-sdk-s3-*.jar && \
    rm /opt/spark/jars/ion-java-1.0.2.jar

RUN echo $'\n\
spark.eventLog.enabled                      true\n\
spark.history.ui.port                       18080\n\
' > /opt/spark/conf/spark-defaults.conf

RUN echo $'\n\
log4j.rootLogger = info, console\n\
log4j.appender.console=org.apache.log4j.ConsoleAppender\n\
log4j.appender.console.target = System.out\n\
log4j.appender.console.layout = org.apache.log4j.PatternLayout\n\
log4j.appender.console.layout.ConversionPattern = %d{yyyy-MM-dd HH:mm:ss} %p %c{2}: %m%n\n\
' > /opt/spark/conf/log4j.properties

ENTRYPOINT ["/bin/bash", "-c"]:
