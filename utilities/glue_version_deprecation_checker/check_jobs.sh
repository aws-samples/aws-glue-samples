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
    echo "[End of Support: 2022/6/1] Spark | Glue 0.9"
    aws glue get-jobs --query 'Jobs[? (GlueVersion==`"0.9"` || GlueVersion==null) && (Command.Name==`"glueetl"` || Command.Name==`"gluestreaming"`)].{JobName:Name,JobType:Command.Name,GlueVersion:GlueVersion,JobLanguage:DefaultArguments."--job-language",PythonVersion:Command.PythonVersion}' $profile_option --region $region --output table

    echo "[End of Support: 2022/6/1] Spark | Glue 1.0 (Python 2)"
    aws glue get-jobs --query 'Jobs[? GlueVersion==`"1.0"` && (Command.Name==`"glueetl"` || Command.Name==`"gluestreaming"`) && Command.PythonVersion==`"2"` && DefaultArguments."--job-language"!=`"scala"`].{JobName:Name,JobType:Command.Name,GlueVersion:GlueVersion,JobLanguage:DefaultArguments."--job-language",PythonVersion:Command.PythonVersion}' $profile_option --region $region --output table

    echo "[End of Support: 2022/6/1] Python shell | Glue 1.0 (Python 2)"
    aws glue get-jobs --query 'Jobs[? Command.Name==`"pythonshell"` && Command.PythonVersion==`"2"`].{JobName:Name,JobType:Command.Name,GlueVersion:GlueVersion,JobLanguage:DefaultArguments."--job-language",PythonVersion:Command.PythonVersion}' $profile_option --region $region --output table

    echo "[End of Support: 2022/9/30] Spark | Glue 1.0 (Python 3, Scala 2)"
    aws glue get-jobs --query 'Jobs[? GlueVersion==`"1.0"` && (Command.Name==`"glueetl"` || Command.Name==`"gluestreaming"`) && (Command.PythonVersion!=`"2"` || DefaultArguments."--job-language"==`"scala"`)].{JobName:Name,JobType:Command.Name,GlueVersion:GlueVersion,JobLanguage:DefaultArguments."--job-language",PythonVersion:Command.PythonVersion}' $profile_option --region $region --output table

    echo "[End of Support: 2024/1/31] Spark | Glue 2.0"
    aws glue get-jobs --query 'Jobs[? GlueVersion==`"2.0"` && (Command.Name==`"glueetl"` || Command.Name==`"gluestreaming"`)].{JobName:Name,JobType:Command.Name,GlueVersion:GlueVersion,JobLanguage:DefaultArguments."--job-language",PythonVersion:Command.PythonVersion}' $profile_option --region $region --output table

    echo "[End of Support: 2026/3/1] Python shell 3.6"
    aws glue get-jobs --query 'Jobs[? Command.Name==`"pythonshell"` && Command.PythonVersion==`"3"`].{JobName:Name,JobType:Command.Name,GlueVersion:GlueVersion,JobLanguage:DefaultArguments."--job-language",PythonVersion:Command.PythonVersion}' $profile_option --region $region --output table

done
