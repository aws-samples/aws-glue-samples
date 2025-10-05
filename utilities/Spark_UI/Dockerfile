FROM public.ecr.aws/amazonlinux/amazonlinux:2023

RUN dnf --setopt=install_weak_deps=False install -y java-17-amazon-corretto maven procps tar gzip && dnf clean all

WORKDIR /tmp/
ADD pom.xml /tmp

ARG SPARK_VERSION=3.5.7
RUN curl -o ./spark-${SPARK_VERSION}-bin-without-hadoop.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop.tgz
RUN tar -xzf spark-${SPARK_VERSION}-bin-without-hadoop.tgz && \
    mv spark-${SPARK_VERSION}-bin-without-hadoop /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-without-hadoop.tgz
RUN mvn dependency:copy-dependencies -DoutputDirectory=/opt/spark/jars/

RUN echo $'\n\
spark.eventLog.enabled                      true\n\
spark.history.ui.port                       18080\n\
' > /opt/spark/conf/spark-defaults.conf

ENV JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto/

ENTRYPOINT ["/bin/bash", "-c"]
