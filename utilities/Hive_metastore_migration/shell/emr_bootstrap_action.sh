wget https://raw.githubusercontent.com/awslabs/aws-glue-samples/master/utilities/Hive_metastore_migration/src/hive_metastore_migration.py -O /home/hadoop/hive_metastore_migration.py
wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-5.1.42.tar.gz -O /home/hadoop/mysql-connector-java-5.1.42.tar.gz
tar -xf /home/hadoop/mysql-connector-java-5.1.42.tar.gz -C /home/hadoop/
sudo cp /home/hadoop/mysql-connector-java-5.1.42/*.jar /usr/lib/hadoop/
