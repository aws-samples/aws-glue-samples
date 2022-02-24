# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#! /bin/bash

regions=
profile=

usage() {
  echo "Usage: $0 [ -r COMMA_SEPARATED_REGION_NAMES ] [ -p PROFILE_NAME ]"
  echo " - Glue jobs in all regions are listed if the option '-r' is not specified."
}

exit_abnormal() {
  usage
  exit 1
}

while getopts r:p:h OPT; do
  case $OPT in
     p) profile=${OPTARG};;
     r) IFS=',' read -r -a regions <<< ${OPTARG};;
     *) exit_abnormal;;
  esac
done

profile_option=""
if [ ! -z "${profile}" ]
then
  echo "Profile: $profile"
  profile_option="--profile $profile"
fi

if [ -z "${regions}" ]
then
  regions=$(aws ec2 describe-regions --query 'Regions[].{Name:RegionName}' $profile_option --output text|sort -r)
fi

echo "Following Glue jobs are going to be deprecated as per AWS Glue's version support policy. "
for region in $regions
do
    echo "Region: $region"
    aws glue get-jobs --query 'Jobs[? (GlueVersion==`"0.9"` || GlueVersion==null) && (Command.Name==`"glueetl"` || Command.Name==`"gluestreaming"`) || Command.PythonVersion==`"2"`].{JobName:Name,JobType:Command.Name,GlueVersion:GlueVersion,PythonVersion:Command.PythonVersion}' $profile_option --region $region --output table
done
