#!/bin/bash -eu
if ! command -v mvn > /dev/null; then
  echo "This script requires Maven to be installed and included in the PATH"
  exit 1 
fi

if ! command -v python3 > /dev/null; then
  echo "This script requires a version of Python3 to be installed and included in the PATH"
  exit 1
fi

python3 -m venv venv
echo "Created venv"
source venv/bin/activate
pip install pyspark==3.3.0
PYSPARK_PATH=$(python -c '
import pyspark
import os
print(os.path.dirname(os.path.realpath(pyspark.__file__)))
')
echo "Installed PySpark under: $PYSPARK_PATH"
echo "Replacing jars with the ones from Glue 4"
JARS_PATH="$PYSPARK_PATH/jars"
mv $JARS_PATH ${JARS_PATH}_bak | true
mvn -f configuration/pom.xml dependency:copy-dependencies -DoutputDirectory=${JARS_PATH}
echo "Installing Glue module from Github"
pip install git+https://github.com/awslabs/aws-glue-libs.git
pip install pytest pytest-cov
echo "---------------------------------------------------------------------------------------------------------------------"
echo "Done, to run the test you can run 'pytest' once you have activated the venv using 'source venv/bin/activate'"
echo "---------------------------------------------------------------------------------------------------------------------"

