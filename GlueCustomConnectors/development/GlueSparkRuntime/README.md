# Download and Install the Glue Spark Runtime from Maven


## Prerequisite

Users may set up AWS local credential to access some AWS services such as AWS Secret Manager and S3 for some tests.
User may setup AWS CLI and set their local credentials with command [`aws configure`](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) which can generate the AWS credential file *~/.aws/credentials*, will be used by glue local job run.

## Step 1: Install Apache Maven
Follow the section Developing Locally with Scala of this [Glue doc](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html) to install Apache Maven from the following location: https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-common/apache-maven-3.6.0-bin.tar.gz. The 'mvn' tool is in the ./bin
folder of this package.


## Step 2: Install Glue libraries
1. Download the Apache Spark distribution from one of the following locations:
    1. For AWS Glue version 0.9: https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-0.9/spark-2.2.1-bin-hadoop2.7.tgz
    2. For AWS Glue version 1.0 and 2.0: https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-1.0/spark-2.4.3-bin-hadoop2.8.tgz
    3. For AWS Glue version 3.0: https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-3.0/spark-3.1.1-amzn-0-bin-3.2.1-amzn-3.tgz

2. Unzip Glue libraries at some location such /home/$USER/

3. Export the SPARK_HOME environment variable, setting it to the root location extracted from the Spark archive. For example:
    1. For AWS Glue version 0.9: export SPARK_HOME=/home/$USER/spark-2.2.1-bin-hadoop2.7
    2. For AWS Glue version 1.0 and 2.0: export SPARK_HOME=/home/$USER/spark-2.4.3-bin-spark-2.4.3-bin-hadoop2.8
    3. For AWS Glue version 3.0: export SPARK_HOME=/home/$USER/spark-3.1.1-amzn-0-bin-3.2.1-amzn-3

Note: please refer to this [Glue doc](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html) for up-to-date information.

## Step 3: Configure Your Maven Project
1. If your connector is already packaged as a jar, you may install the connector jar in local maven repository,
    ```
       mvn install:install-file \
         -Dfile="path_to_connector.jar" \
         -DgroupId="your_groupId" \
         -DartifactId="your_artifactId" \
         -Dversion="your_version" \
         -Dpackaging="jar"
    ```
    and add the installed connector dependency to the project pom.xml.
    ```
      <dependency>
          <groupId>your_gorupId</groupId>
          <artifactId>your_artifactId</artifactId>
          <version>your_version</version>
      </dependency>
    ```
    For example, add the following dependencies to the POM file for the open-source Snowflake Spark connector:
    
      ```
              <dependency>
                  <groupId>net.snowflake</groupId>
                  <artifactId>spark-snowflake_2.11</artifactId>
                  <version>2.7.0-spark_2.4</version>
                  <scope>provided</scope>
              </dependency>
              <dependency>
                  <groupId>net.snowflake</groupId>
                  <artifactId>snowflake-jdbc</artifactId>
                  <version>3.12.3</version>
                  <scope>provided</scope>
              </dependency>

              <!-- For Glue 3.0 -->
              <dependency>
                  <groupId>net.snowflake</groupId>
                  <artifactId>spark-snowflake_2.12</artifactId>
                  <version>2.9.1-spark_3.1</version>
                  <scope>provided</scope>
              </dependency>
              <dependency>
                  <groupId>net.snowflake</groupId>
                  <artifactId>snowflake-jdbc</artifactId>
                  <version>3.13.6</version>
                  <scope>provided</scope>
              </dependency>
      ```  

2. To validate your connector against Glue features locally, add the [ScalaTest](https://www.scalatest.org/user_guide) dependency to the project pom.xml. Please 
follow this [Using the ScalaTest Maven plugin](https://www.scalatest.org/user_guide/using_the_scalatest_maven_plugin) to disable SureFire and enable ScalaTest in the pom.xml.
    ```
    <dependency>
            <groupId>org.scalatest</groupId>
            <artifactId>scalatest_2.11</artifactId>
            <version>3.0.5</version>
            <scope>test</scope>
    </dependency>

    <!-- For Glue 3.0 -->
    <dependency>
            <groupId>org.scalatest</groupId>
            <artifactId>scalatest_2.12</artifactId>
            <version>3.0.5</version>
            <scope>test</scope>
    </dependency>        
    ```
    Copy the integration tests to the local project directory.
3. Build the project.

Note: Remember to replace the Glue version string with 3.0.0 for AWS Glue 3.0, and with 2.0.0 for AWS Glue 2.0 or 1.0.0 for AWS Glue version 1.0 and 2.0. 
    
The following is a sample POM file for the Maven project with Snowflake open-source spark
```
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.amazonaws</groupId>
    <artifactId>AWSGlueApp</artifactId>
    <version>1.0-SNAPSHOT</version>
    <name>${project.artifactId}</name>
    <description>AWS Glue ETL application</description>

        <properties>
            <scala.version>2.11.1</scala.version>
            <scala.version>2.12.10</scala.version> <!-- For Glue 3.0. -->
        </properties>

    <dependencies>
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
        </dependency>
        <dependency>
            <groupId>com.amazonaws</groupId>
            <artifactId>AWSGlueETL</artifactId>
            <version>1.0.0</version>
            <version>3.0.0</version> <!-- For Glue 3.0. -->
        </dependency>
        <dependency>
            <groupId>net.snowflake</groupId>
            <artifactId>spark-snowflake_2.11</artifactId>
            <artifactId>spark-snowflake_2.12</artifactId> <!-- For Glue 3.0. -->
            <version>2.7.0-spark_2.4</version> 
            <version>2.9.1-spark_3.1</version> <!-- For Glue 3.0. -->
            <scope>provided</scope>
         </dependency>
         <dependency>
            <groupId>net.snowflake</groupId>
            <artifactId>snowflake-jdbc</artifactId>
            <version>3.12.3</version>
            <version>3.13.6</version> <!-- For Glue 3.0 -->
            <scope>provided</scope>
          </dependency>
        <dependency>
            <groupId>org.scalatest</groupId>
            <artifactId>scalatest_2.11</artifactId>
            <artifactId>scalatest_2.12</artifactId> <!-- For Glue 3.0 -->
            <version>3.0.5</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <repositories>
        <repository>
            <id>aws-glue-etl-artifacts</id>
            <url>https://aws-glue-etl-artifacts.s3.amazonaws.com/release/</url>
        </repository>
    </repositories>
    <build>
        <sourceDirectory>src/main/scala</sourceDirectory>
        <plugins>
            <plugin>
                <!-- see http://davidb.github.com/scala-maven-plugin -->
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.4.0</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>compile</goal>
                            <goal>testCompile</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>org.codehaus.mojo</groupId>
                <artifactId>exec-maven-plugin</artifactId>
                <version>1.6.0</version>
                <executions>
                    <execution>
                        <goals>
                        <goal>java</goal>
                        </goals>
                    </execution>
                </executions>
                <configuration>
                <systemProperties>
                    <systemProperty>
                        <key>spark.master</key>
                        <value>local[*]</value>
                    </systemProperty>
                    <systemProperty>
                        <key>spark.app.name</key>
                        <value>localrun</value>
                    </systemProperty>
                    <systemProperty>
                        <key>org.xerial.snappy.lib.name</key>
                        <value>libsnappyjava.jnilib</value>
                    </systemProperty>
                </systemProperties>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-enforcer-plugin</artifactId>
                <version>3.0.0-M2</version>
                <executions>
                    <execution>
                        <id>enforce-maven</id>
                        <goals>
                            <goal>enforce</goal>
                        </goals>
                        <configuration>
                            <rules>
                                <requireMavenVersion>
                                    <version>3.5.3</version>
                                </requireMavenVersion>
                            </rules>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <configuration>
                    <source>8</source>
                    <target>8</target>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>2.7</version>
                <configuration>
                <skipTests>true</skipTests>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.scalatest</groupId>
                <artifactId>scalatest-maven-plugin</artifactId>
                <version>1.0</version>
                <configuration>
                    <reportsDirectory>${project.build.directory}/surefire-reports</reportsDirectory>
                    <junitxml>.</junitxml>
                    <filereports>WDF TestSuite.txt</filereports>
                </configuration>
                <executions>
                    <execution>
                        <id>test</id>
                        <goals>
                            <goal>test</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```