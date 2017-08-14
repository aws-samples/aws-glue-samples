S3_SCRIPT_DIR="s3://mydefaultgluetest/"
S3_JOB_SCRIPT="$S3_SCRIPT_DIR/metastore_extraction.py"

aws s3 cp $S3_JOB_SCRIPT /home/hadoop/metastore_extraction.py
wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-5.1.42.tar.gz -O /home/hadoop/mysql-connector-java-5.1.42.tar.gz
tar -xf /home/hadoop/mysql-connector-java-5.1.42.tar.gz -C /home/hadoop/
sudo cp /home/hadoop/mysql-connector-java-5.1.42/*.jar /usr/lib/hadoop/
