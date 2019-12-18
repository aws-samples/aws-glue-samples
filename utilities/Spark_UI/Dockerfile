FROM amazonlinux:2
FROM amazoncorretto:8
FROM maven:3.6-amazoncorretto-8

RUN yum install -y procps

WORKDIR /tmp/
ADD pom.xml /tmp
RUN curl -o ./spark-2.4.3-bin-without-hadoop.tgz https://archive.apache.org/dist/spark/spark-2.4.3/spark-2.4.3-bin-without-hadoop.tgz
RUN tar -xzf spark-2.4.3-bin-without-hadoop.tgz && \
    mv spark-2.4.3-bin-without-hadoop /opt/spark && \
    rm spark-2.4.3-bin-without-hadoop.tgz
RUN mvn dependency:copy-dependencies -DoutputDirectory=/opt/spark/jars/

RUN curl -o ./httpclient-4.5.9.jar http://repo.maven.apache.org/maven2/org/apache/httpcomponents/httpclient/4.5.9/httpclient-4.5.9.jar
RUN mv httpclient-4.5.9.jar /opt/spark/jars/

RUN echo $'\n\
spark.eventLog.enabled                      true\n\
spark.history.ui.port                       18080\n\
' > /opt/spark/conf/spark-defaults.conf

ENTRYPOINT ["/bin/bash", "-c"]:

