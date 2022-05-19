# Copyright 2019-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import boto3
from botocore.client import ClientError
import argparse
from distutils.util import strtobool
import urllib
import tempfile
import os
import sys
import logging

# Configure credentials and required parameters
parser = argparse.ArgumentParser()
parser.add_argument('--src-job-names', dest='src_job_names', type=str,
                    help='The comma separated list of the names of AWS Glue jobs which are going to be copied from source AWS account. If it is not set, all the Glue jobs in the source account will be copied to the destination account.')
parser.add_argument('--src-profile', dest='src_profile', type=str,
                    help='AWS named profile name for source AWS account.')
parser.add_argument('--src-region', dest='src_region',
                    type=str, help='Source region name.')
parser.add_argument('--dst-profile', dest='dst_profile', type=str,
                    help='AWS named profile name for destination AWS account.')
parser.add_argument('--dst-region', dest='dst_region',
                    type=str, help='Destination region name.')
parser.add_argument('--sts-role-arn', dest='sts_role_arn',
                    type=str, help='IAM role arn to be assumed to access destination account resources.')
parser.add_argument('--skip-no-dag-jobs', dest='skip_no_dag_jobs', type=strtobool, default=True,
                    help='Skip Glue jobs which do not have DAG. (possible values: [true, false]. default: true)')
parser.add_argument('--overwrite-jobs', dest='overwrite_jobs', type=strtobool, default=True,
                    help='Overwrite Glue jobs when the jobs already exist. (possible values: [true, false]. default: true)')
parser.add_argument('--copy-job-script', dest='copy_job_script', type=strtobool, default=True,
                    help='Copy Glue job script from the source account to the destination account. (possible values: [true, false]. default: true)')
parser.add_argument('--config-path', dest='config_path', type=str,
                    help='The config file path to provide parameter mapping. You can set S3 path or local file path.')
parser.add_argument('-v', '--verbose', dest='verbose', action="store_true", help='(Optional) Display verbose logging (default: false)')
args, unknown = parser.parse_known_args()

logger = logging.getLogger()
logger_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(logger_handler)
if args.verbose:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)
for libname in ["boto3", "botocore", "urllib3", "s3transfer"]:
    logging.getLogger(libname).setLevel(logging.WARNING)


src_session_args = {}
if args.src_profile is not None:
    src_session_args['profile_name'] = args.src_profile
    logger.info(f"Source account: boto3 Session uses {args.src_profile} profile based on the argument.")
if args.src_region is not None:
    src_session_args['region_name'] = args.src_region
    logger.info(f"Source account: boto3 Session uses {args.src_region} region based on the argument.")

dst_session_args = {}
if args.dst_profile is not None:
    dst_session_args['profile_name'] = args.dst_profile
    logger.info(f"Destination account: boto3 Session uses {args.dst_profile} profile based on the argument.")
if args.dst_region is not None:
    dst_session_args['region_name'] = args.dst_region
    logger.info(f"Destination account: boto3 Session uses {args.dst_region} region based on the argument.")

src_session = boto3.Session(**src_session_args)
src_glue = src_session.client('glue')
src_s3 = src_session.resource('s3')

if args.sts_role_arn is not None:
    src_sts = src_session.client('sts')
    res = src_sts.assume_role(
        RoleArn=args.sts_role_arn, RoleSessionName='glue-job-sync')
    dst_session_args['aws_access_key_id'] = res['Credentials']['AccessKeyId']
    dst_session_args['aws_secret_access_key'] = res['Credentials']['SecretAccessKey']
    dst_session_args['aws_session_token'] = res['Credentials']['SessionToken']

if args.dst_profile is None and args.sts_role_arn is None:
    logger.error("You need to set --dst-profile or --sts-role-arn to create resources in the destination account.")
    sys.exit(1)

dst_session = boto3.Session(**dst_session_args)
dst_glue = dst_session.client('glue')
dst_s3 = dst_session.resource('s3')
dst_s3_client = dst_session.client('s3')


def load_mapping_config_file(path):
    """Function to load mapping config (JSON) file from S3 or local.

    Args:
        path: S3 path (s3://path_to_file) or local path.

    Returns:
        mapping: Mapping config dictionary

    """
    # Load from S3
    if path.startswith("s3://"):
        logger.debug(f"Loading mapping conf file file from S3 path: {path}")
        with tempfile.TemporaryDirectory() as tmpdir:
            conf_o = urllib.parse.urlparse(path)
            conf_bucket = conf_o.netloc
            conf_key = urllib.parse.unquote(conf_o.path)[1:]
            conf_basename = os.path.basename(conf_key)
            conf_local_path = f"{tmpdir}/{conf_basename}"
            try:
                src_s3.meta.client.download_file(
                    conf_bucket, conf_key, conf_local_path)
            except ClientError as ce:
                logger.error(f"Failed to download config file from S3. Exception: {ce}")
                sys.exit(1)
    # Load from Local
    else:
        logger.debug(f"Loading mapping conf file file from local path: {path}")
        conf_local_path = path

    with open(conf_local_path, 'r') as rf:
        mapping = json.load(rf)
    return mapping


def replace_param_with_mapping(param, mapping):
    """Function to replace specific string in job parameters with pre-defined mapping configuration (JSON file).
    This method is designed for replacing account specific resources (e.g. S3 path, IAM role ARN).

    Args:
        param: Input job parameter.
        mapping: Mapping configuration to replace the job parameter.

    """
    for k, v in param.items():
        if isinstance(v, dict):
            replace_param_with_mapping(v, mapping)
        elif isinstance(v, str):
            for mk, mv in mapping.items():
                if mk in v:
                    param[k] = v.replace(mk, mv)
                    logger.info(f"Mapped param {k}: {v} -> {param[k]}")


def organize_job_param(job, mapping):
    """Function to organize a job parameters to prepare for create_job API.

    Args:
        job: Input job parameter.
        mapping: Mapping configuration to replace the job parameter.

    Returns:
        job: Organized job parameter.
    """
    # Drop unneeded parameters
    del job['AllocatedCapacity']
    del job['MaxCapacity']
    del job['CreatedOn']
    del job['LastModifiedOn']

    # Overwrite parameters
    if mapping:
        replace_param_with_mapping(job, mapping)

    return job


def copy_job_script(src_s3path, dst_s3path):
    """Function to copy a job script file from source S3 path to destination S3 path.
    This method downloads the script to local temporary directory, and uploads the script to destination.
    When destination S3 bucket does not exist, a new S3 bucket is created.

    Args:
        src_s3path: Source S3 path of AWS Glue job script.
        dst_s3path: Destination S3 path of AWS Glue job script.

    """
    with tempfile.TemporaryDirectory() as tmpdir:
        src_o = urllib.parse.urlparse(src_s3path)
        src_bucket = src_o.netloc
        src_key = urllib.parse.unquote(src_o.path)[1:]
        src_basename = os.path.basename(src_key)

        src_s3.meta.client.download_file(
            src_bucket, src_key, f"{tmpdir}/{src_basename}")
        dst_o = urllib.parse.urlparse(dst_s3path)
        dst_bucket = dst_o.netloc
        dst_key = urllib.parse.unquote(dst_o.path)[1:]

        # Create a script bucket in the destination account
        try:
            dst_s3_client.head_bucket(Bucket=dst_bucket)
            logger.debug("Script bucket already exists: ", dst_bucket)
        except ClientError as ce:
            logger.info("Creating script bucket: ", dst_bucket)
            if args.dst_region == "us-east-1":
                dst_s3_client.create_bucket(Bucket=dst_bucket)
            else:
                location = {'LocationConstraint': args.dst_region}
                dst_s3_client.create_bucket(
                    Bucket=dst_bucket, CreateBucketConfiguration=location)

        # Upload the job script to the script bucket in the destination account
        dst_s3.meta.client.upload_file(
            f"{tmpdir}/{src_basename}", dst_bucket, dst_key)


def synchronize_job(job_name, mapping):
    """Function to synchronize an AWS Glue job.

    Args:
        job_name: The name of AWS Glue job which is going to be synchronized.
        mapping: Mapping configuration to replace the job parameter.

    """
    logger.debug(f"Synchronizing job '{job_name}'")
    # Get DAG per job in the source account
    res = src_glue.get_job(JobName=job_name)
    job = res['Job']

    # Skip jobs which do not have DAG
    if args.skip_no_dag_jobs and 'CodeGenConfigurationNodes' not in job:
        logger.debug(f"Skipping job '{job_name}' because the parameter '--skip-no-dag-jobs' is true and this job does not have DAG.")
        return

    # Store source job script path
    src_job_script_s3_url = job['Command']['ScriptLocation']

    # Organize job parameters
    job = organize_job_param(job, mapping)

    # Store source job script path
    dst_job_script_s3_url = job['Command']['ScriptLocation']

    # Copy job script
    if args.copy_job_script:
        logger.debug(f"Copying job script for job '{job_name}' because the parameter 'copy-job-script' is true.")
        copy_job_script(src_job_script_s3_url, dst_job_script_s3_url)

    # Copy job configuration
    try:
        logger.debug(f"Checking if job '{job_name}' exists in the destination account.")
        current_job = dst_glue.get_job(JobName=job_name)
        logger.debug(f"Current job '{job_name}' configuration: {current_job}")
        if args.overwrite_jobs:
            del job['Name']
            job_update = {}
            job_update['JobName'] = job_name
            job_update['JobUpdate'] = job
            logger.debug(f"Updating job '{job_name}' with configuration: '{job_update}'")
            dst_glue.update_job(**job_update)
            logger.info(f"The job '{job_name}' has been overwritten.")
    except dst_glue.exceptions.EntityNotFoundException:
        logger.debug(f"fCreating job '{job_name}' with configuration: '{job}'")
        dst_glue.create_job(**job)
        logger.info(f"New job '{job_name}' has been created.")


def main():
    if args.config_path:
        logger.debug(f"Loading Mapping config file: {args.config_path}")
        mapping = load_mapping_config_file(args.config_path)
    else:
        logger.debug(f"Mapping config file is not given.")
        mapping = None

    if args.src_job_names is not None:
        logger.debug(f"Sync target: {args.src_job_names}")
        job_names = args.src_job_names.split(',')
        for job_name in job_names:
            synchronize_job(job_name, mapping)
    else:
        jobs = []
        get_jobs_paginator = src_glue.get_paginator('get_jobs')
        for page in get_jobs_paginator.paginate():
            jobs.extend(page['Jobs'])

        for j in jobs:
            synchronize_job(j['Name'], mapping)


if __name__ == "__main__":
    main()
