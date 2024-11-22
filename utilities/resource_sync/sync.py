# Copyright 2019-2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import boto3
import botocore
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
parser.add_argument('--targets', dest='targets', type=str, default="job",
                    help='The comma separated list of targets [job, catalog, connection]. (possible values: [job, catalog, connection]. default: job)')
parser.add_argument('--src-job-names', dest='src_job_names', type=str,
                    help='The comma separated list of the names of AWS Glue jobs which are going to be copied from source AWS account. If it is not set, all the Glue jobs in the source account will be copied to the destination account.')
parser.add_argument('--src-database-names', dest='src_database_names', type=str,
                    help='The comma separated list of the names of AWS Glue databases which are going to be copied from source AWS account. If it is not set, all the Glue databases in the source account will be copied to the destination account.')
parser.add_argument('--src-connection-names', dest='src_connection_names', type=str,
                    help='The comma separated list of the names of AWS Glue connections which are going to be copied from source AWS account. If it is not set, all the Glue connections in the source account will be copied to the destination account.')
parser.add_argument('--src-table-names', dest='src_table_names', type=str,
                    help='The comma separated list of the names of AWS Glue tables which are going to be copied from source AWS account. If it is not set, all the Glue tables in the specified databases will be copied to the destination account.')
parser.add_argument('--src-profile', dest='src_profile', type=str,
                    help='AWS named profile name for source AWS account.')
parser.add_argument('--src-region', dest='src_region',
                    type=str, help='Source region name.')
parser.add_argument('--src-s3-endpoint-url', dest='src_s3_endpoint_url',
                    type=str, help='Source endpoint URL for Amazon S3.')
parser.add_argument('--src-sts-endpoint-url', dest='src_sts_endpoint_url',
                    type=str, help='Source endpoint URL for AWS STS.')
parser.add_argument('--src-glue-endpoint-url', dest='src_glue_endpoint_url',
                    type=str, help='Source endpoint URL for AWS Glue.')
parser.add_argument('--dst-profile', dest='dst_profile', type=str,
                    help='AWS named profile name for destination AWS account.')
parser.add_argument('--dst-region', dest='dst_region',
                    type=str, help='Destination region name.')
parser.add_argument('--dst-s3-endpoint-url', dest='dst_s3_endpoint_url',
                    type=str, help='Destination endpoint URL for Amazon S3.')
parser.add_argument('--dst-sts-endpoint-url', dest='dst_sts_endpoint_url',
                    type=str, help='Destination endpoint URL for AWS STS.')
parser.add_argument('--dst-glue-endpoint-url', dest='dst_glue_endpoint_url',
                    type=str, help='Destination endpoint URL for AWS Glue.')
parser.add_argument('--sts-role-arn', dest='sts_role_arn',
                    type=str, help='IAM role ARN to be assumed to access destination account resources.')
parser.add_argument('--src-role-arn', dest='src_role_arn', type=str,
                    help='IAM role ARN to be assumed to access source account resources.')
parser.add_argument('--dst-role-arn', dest='dst_role_arn', type=str,
                    help='IAM role ARN to be assumed to access destination account resources.')
parser.add_argument('--skip-no-dag-jobs', dest='skip_no_dag_jobs', type=strtobool, default=True,
                    help='Skip Glue jobs which do not have DAG. (possible values: [true, false]. default: true)')
parser.add_argument('--overwrite-jobs', dest='overwrite_jobs', type=strtobool, default=True,
                    help='Overwrite Glue jobs when the jobs already exist. (possible values: [true, false]. default: true)')
parser.add_argument('--overwrite-databases', dest='overwrite_databases', type=strtobool, default=True,
                    help='Overwrite Glue databases when the tables already exist. (possible values: [true, false]. default: true)')
parser.add_argument('--overwrite-tables', dest='overwrite_tables', type=strtobool, default=True,
                    help='Overwrite Glue tables when the tables already exist. (possible values: [true, false]. default: true)')
parser.add_argument('--overwrite-connections', dest='overwrite_connections', type=strtobool, default=True,
                    help='Overwrite Glue connections when the connections already exist. (possible values: [true, false]. default: true)')
parser.add_argument('--copy-job-script', dest='copy_job_script', type=strtobool, default=True,
                    help='Copy Glue job script from the source account to the destination account. (possible values: [true, false]. default: true)')
parser.add_argument('--config-path', dest='config_path', type=str,
                    help='The config file path to provide parameter mapping. You can set S3 path or local file path.')
parser.add_argument('--serialize-to-file', dest='serialize_file', type=str,
                    help='Serialize jobs and/or tables to a local file instead of deploying.')
parser.add_argument('--deserialize-from-file', dest='deserialize_file', type=str,
                    help='Deserialize jobs and/or tables from a local file instead of source account.')
parser.add_argument('--skip-errors', dest='skip_errors', action="store_true", help='(Optional) Skip errors and continue execution. (default: false)')
parser.add_argument('--dryrun', dest='dryrun', action="store_true", help='(Optional) Display the operations that would be performed using the specified command without actually running them (default: false)')
parser.add_argument('--skip-prompt', dest='skip_prompt', action="store_true", help='(Optional) Skip prompt (default: false)')
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

logger.debug(f"Python version: {sys.version}")
logger.debug(f"Version info: {sys.version_info}")
logger.debug(f"boto3 version: {boto3.__version__}")
logger.debug(f"botocore version: {botocore.__version__}")

if args.serialize_file and args.deserialize_file:
    logger.error("You cannot specify both --serialize-to-file and --deserialize-from-file simultaneously.")
    sys.exit(1)

# Only require source credentials when not deserializing
if args.deserialize_file is None:
    if args.src_profile is None and args.src_role_arn is None:
        logger.error("You need to set --src-profile or --src-role-arn to access the source account.")
        sys.exit(1)

# Only require destination credentials when not serializing
if args.serialize_file is None:
    if args.dst_profile is None and args.dst_role_arn is None and args.sts_role_arn is None:
        logger.error("You need to set --dst-profile, --dst-role-arn, or --sts-role-arn to access the destination account.")
        sys.exit(1)

src_session_args = {}
if args.src_profile is not None:
    src_session_args['profile_name'] = args.src_profile
    logger.info(f"Source: boto3 Session uses {args.src_profile} profile based on the argument.")
if args.src_region is not None:
    src_session_args['region_name'] = args.src_region
    logger.info(f"Source: boto3 Session uses {args.src_region} region based on the argument.")
if args.src_role_arn is not None:
    src_sts = boto3.client('sts', endpoint_url=args.src_sts_endpoint_url)
    res = src_sts.assume_role(RoleArn=args.src_role_arn, RoleSessionName='glue-job-sync-src')
    src_session_args['aws_access_key_id'] = res['Credentials']['AccessKeyId']
    src_session_args['aws_secret_access_key'] = res['Credentials']['SecretAccessKey']
    src_session_args['aws_session_token'] = res['Credentials']['SessionToken']

dst_session_args = {}
if args.dst_profile is not None:
    dst_session_args['profile_name'] = args.dst_profile
    logger.info(f"Destination: boto3 Session uses {args.dst_profile} profile based on the argument.")
if args.dst_region is not None:
    dst_session_args['region_name'] = args.dst_region
    logger.info(f"Destination: boto3 Session uses {args.dst_region} region based on the argument.")
if args.dst_role_arn is not None or args.sts_role_arn is not None:
    dst_sts = boto3.client('sts', endpoint_url=args.dst_sts_endpoint_url)
    role_arn = args.dst_role_arn if args.dst_role_arn is not None else args.sts_role_arn
    res = dst_sts.assume_role(RoleArn=role_arn, RoleSessionName='glue-job-sync-dst')
    dst_session_args['aws_access_key_id'] = res['Credentials']['AccessKeyId']
    dst_session_args['aws_secret_access_key'] = res['Credentials']['SecretAccessKey']
    dst_session_args['aws_session_token'] = res['Credentials']['SessionToken']

src_session = boto3.Session(**src_session_args)
src_glue = src_session.client('glue', endpoint_url=args.src_glue_endpoint_url)
src_s3 = src_session.resource('s3', endpoint_url=args.src_s3_endpoint_url)

dst_session = boto3.Session(**dst_session_args)
dst_glue = dst_session.client('glue', endpoint_url=args.dst_glue_endpoint_url)
dst_s3 = dst_session.resource('s3', endpoint_url=args.dst_s3_endpoint_url)
dst_s3_client = dst_session.client('s3', endpoint_url=args.dst_s3_endpoint_url)

do_update = not args.dryrun

resources = {}


def prompt(message):
    answer = input(message)
    if answer.lower() in ["n", "no"]:
        sys.exit(0)
    elif answer.lower() not in ["y", "yes"]:
        prompt(message)


if do_update:
    if not args.skip_prompt:
        prompt(f"Are you sure to make modifications on Glue resources? (y/n): ")
else:
    logger.info("Running in dry run mode. There are no updates triggered by this execution.")


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
    """Function to replace specific string in parameters with pre-defined mapping configuration (JSON file).
    This method is designed for replacing account specific resources (e.g. S3 path, IAM role ARN).

    Args:
        param: Input parameter.
        mapping: Mapping configuration to replace the parameter.

    """
    if isinstance(param, dict):
        items = param.items()
    elif isinstance(param, (list, tuple)):
        items = enumerate(param)
    elif isinstance(param, str):
        for mk, mv in mapping.items():
             if mk in param:
                 value_old = param
                 value = param.replace(mk, mv)
                 logger.info(f"Mapped param: {value_old} -> {value}")
                 param = value
        return param
    else:
        return param

    for key, value in items:
        param[key] = replace_param_with_mapping(value, mapping)
    return param


def organize_job_param(job, mapping):
    """Function to organize a job parameters to prepare for create_job API.

    Args:
        job: Input job parameter.
        mapping: Mapping configuration to replace the job parameter.

    Returns:
        job: Organized job parameter.
    """
    # Drop unneeded parameters
    if 'AllocatedCapacity' in job:
      del job['AllocatedCapacity']
    if 'MaxCapacity' in job:
      del job['MaxCapacity']
    if 'CreatedOn' in job:
      del job['CreatedOn']
    if 'LastModifiedOn' in job:
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
            logger.debug(f"Script bucket already exists: '{dst_bucket}'")
        except ClientError as ce:
            logger.info(f"Creating script bucket: '{dst_bucket}'")
            if args.dst_region == "us-east-1":
                dst_s3_client.create_bucket(Bucket=dst_bucket)
            else:
                location = {'LocationConstraint': args.dst_region}
                dst_s3_client.create_bucket(
                    Bucket=dst_bucket, CreateBucketConfiguration=location)

        # Upload the job script to the script bucket in the destination account
        dst_s3.meta.client.upload_file(
            f"{tmpdir}/{src_basename}", dst_bucket, dst_key)


def get_job_names():
    if args.deserialize_file:
        return [job['Name'] for job in resources.get('jobs', [])]
    else:
        if args.src_job_names:
            return args.src_job_names.split(',')
        else:
            job_names = []
            get_job_names_paginator = src_glue.get_paginator('list_jobs')
            for page in get_job_names_paginator.paginate():
                job_names.extend(page['JobNames'])
            return job_names


def get_job_definition(job_name):
    if args.deserialize_file:
        job_definitions = {job['Name']: job for job in resources.get('jobs', [])}
        job = job_definitions.get(job_name)
        if not job:
            logger.error(f"Job '{job_name}' not found in deserialized data.")
        return job
    else:
        res = src_glue.get_job(JobName=job_name)
        job = res['Job']
        return job


def synchronize_job(job_name, mapping, job):
    logger.debug(f"Synchronizing job '{job_name}'")

    # Skip jobs which do not have DAG
    if args.skip_no_dag_jobs and 'CodeGenConfigurationNodes' not in job:
        logger.debug(f"Skipping job '{job_name}' because the parameter '--skip-no-dag-jobs' is true and this job does not have DAG.")
        return

    # Store source job script path
    src_job_script_s3_url = job['Command']['ScriptLocation']

    # Organize job parameters
    job = organize_job_param(job, mapping)

    # Store destination job script path
    dst_job_script_s3_url = job['Command']['ScriptLocation']

    # Copy job script
    if args.copy_job_script:
        logger.debug(f"Copying job script for job '{job_name}' because the parameter 'copy-job-script' is true.")
        try:
            if args.deserialize_file:
                logger.debug(f"Skipping copying job script for job '{job_name}' because the parameter 'deserialize-from-file' is true.")
            else:
                if do_update:
                    copy_job_script(src_job_script_s3_url, dst_job_script_s3_url)
        except Exception as e:
            logger.error(f"Error occurred in copying job script: '{job_name}'")
            if args.skip_errors:
                logger.error(f"Skipping error: {e}", exc_info=True)
            else:
                raise

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
            logger.debug(f"Updating job '{job_name}' with configuration: '{json.dumps(job_update, indent=4, default=str)}'")
            if do_update:
                dst_glue.update_job(**job_update)
            logger.info(f"The job '{job_name}' has been overwritten.")
    except dst_glue.exceptions.EntityNotFoundException:
        logger.debug(f"Creating job '{job_name}' with configuration: '{json.dumps(job, indent=4, default=str)}'")
        if do_update:
            dst_glue.create_job(**job)
        logger.info(f"New job '{job_name}' has been created.")
    except Exception as e:
        logger.error(f"Error occurred in copying job: '{job_name}'")
        if args.skip_errors:
            logger.error(f"Skipping error: {e}", exc_info=True)
        else:
            raise


def organize_partition_param(database_name, table_name, partition_argument, mapping):
    """Function to organize a partition argument parameters to prepare for batch_create_partition API.

    Args:
        partition_argument argument: Input partition argument parameter.
        mapping: Mapping configuration to replace the parameter.

    Returns:
        partition_argument: Organized partition argument parameter.
    """
    partition_argument['DatabaseName'] = database_name
    partition_argument['TableName'] = table_name

    for arg in partition_argument['PartitionInputList']:
        # Drop unneeded parameters
        if 'CatalogId' in arg:
          del arg['CatalogId']
        if 'DatabaseName' in arg:
          del arg['DatabaseName']
        if 'TableName' in arg:
          del arg['TableName']
        if 'CreationTime' in arg:
          del arg['CreationTime']

    # Overwrite parameters
    if mapping:
        replace_param_with_mapping(partition_argument, mapping)

    return partition_argument


def organize_table_param(table_argument, mapping):
    """Function to organize a table argument parameters to prepare for create_database API.

    Args:
        table_argument: Input table argument parameter.
        mapping: Mapping configuration to replace the parameter.

    Returns:
        table_argument: Organized table argument parameter.
    """
    table_argument['DatabaseName'] = table_argument['TableInput']['DatabaseName']

    # Drop unneeded parameters
    if 'CatalogId' in table_argument['TableInput']:
      del table_argument['TableInput']['CatalogId']
    if 'CatalogIdentifier' in table_argument['TableInput']:
      del table_argument['TableInput']['CatalogIdentifier']
    if 'DatabaseName' in table_argument['TableInput']:
      del table_argument['TableInput']['DatabaseName']
    if 'DatabaseId' in table_argument['TableInput']:
        del table_argument['TableInput']['DatabaseId']
    if 'CreateTime' in table_argument['TableInput']:
      del table_argument['TableInput']['CreateTime']
    if 'UpdateTime' in table_argument['TableInput']:
      del table_argument['TableInput']['UpdateTime']
    if 'CreatedBy' in table_argument['TableInput']:
      del table_argument['TableInput']['CreatedBy']
    if 'IsRegisteredWithLakeFormation' in table_argument['TableInput']:
      del table_argument['TableInput']['IsRegisteredWithLakeFormation']
    if 'IsMultiDialectView' in table_argument['TableInput']:
      del table_argument['TableInput']['IsMultiDialectView']
    if 'VersionId' in table_argument['TableInput']:
      del table_argument['TableInput']['VersionId']

    # Overwrite parameters
    if mapping:
        replace_param_with_mapping(table_argument, mapping)

    return table_argument


def organize_database_param(database_argument, mapping):
    """Function to organize a database argument parameters to prepare for create_database API.

    Args:
        database_argument: Input database argument parameter.
        mapping: Mapping configuration to replace the parameter.

    Returns:
        database_argument: Organized database argument parameter.
    """
    # Drop unneeded parameters
    if 'CatalogId' in database_argument['DatabaseInput']:
      del database_argument['DatabaseInput']['CatalogId']
    if 'CatalogIdentifier' in database_argument['DatabaseInput']:
      del database_argument['DatabaseInput']['CatalogIdentifier']
    if 'DatabaseId' in database_argument['DatabaseInput']:
      del database_argument['DatabaseInput']['DatabaseId']
    if 'CreateTime' in database_argument['DatabaseInput']:
      del database_argument['DatabaseInput']['CreateTime']

    # Overwrite parameters
    if mapping:
        replace_param_with_mapping(database_argument, mapping)

    return database_argument


def organize_connection_param(connection_argument, mapping):
    """Function to organize a connection argument parameters to prepare for create_connection API.

    Args:
        connection_argument: Input connection argument parameter.
        mapping: Mapping configuration to replace the parameter.

    Returns:
        connection_argument: Organized connection argument parameter.
    """
    # Drop unneeded parameters
    if 'CreationTime' in connection_argument['ConnectionInput']:
      del connection_argument['ConnectionInput']['CreationTime']
    if 'LastConnectionValidationTime' in connection_argument['ConnectionInput']:
      del connection_argument['ConnectionInput']['LastConnectionValidationTime']
    if 'LastUpdatedBy' in connection_argument['ConnectionInput']:
      del connection_argument['ConnectionInput']['LastUpdatedBy']
    if 'LastUpdatedTime' in connection_argument['ConnectionInput']:
      del connection_argument['ConnectionInput']['LastUpdatedTime']
    if 'ConnectionSchemaVersion' in connection_argument['ConnectionInput']:
      del connection_argument['ConnectionInput']['ConnectionSchemaVersion']
    if 'Status' in connection_argument['ConnectionInput']:
      del connection_argument['ConnectionInput']['Status']
    if 'StatusReason' in connection_argument['ConnectionInput']:
      del connection_argument['ConnectionInput']['StatusReason']

    # Add required parameters
    if 'ConnectionProperties' not in connection_argument['ConnectionInput']:
      connection_argument['ConnectionInput']['ConnectionProperties'] = {}

    # Overwrite parameters
    if mapping:
        replace_param_with_mapping(connection_argument, mapping)

    return connection_argument


def get_partition_input(partition_argument, value):
    for p in partition_argument['PartitionInputList']:
        if p['Values'] == value:
            return p
    return None

def synchronize_partitions(database_name, table_name, partitions, mapping):
    """Function to synchronize an AWS Glue partition.

    Args:
        database_name: The name of AWS Glue database which is going to be synchronized.
        table_name: The name of AWS Glue table which is going to be synchronized.
        partitions: The info of AWS Glue partitions which are going to be synchronized.
        mapping: Mapping configuration to replace the parameter.

    """
    logger.debug(f"Synchronizing partitions under the table '{database_name}'.'{table_name}'")

    # Organize partition parameters
    partition_argument = {}
    partition_argument['PartitionInputList'] = partitions
    partition_argument = organize_partition_param(database_name, table_name, partition_argument, mapping)

    partition_argument_for_overwrite = {}
    partition_argument_for_overwrite['DatabaseName'] = database_name
    partition_argument_for_overwrite['TableName'] = table_name
    partition_argument_for_overwrite['Entries'] = []

    # Copy partition configuration
    if do_update:
        res = dst_glue.batch_create_partition(**partition_argument)
        for err in res['Errors']:
            if err['ErrorDetail']['ErrorCode'] == "AlreadyExistsException":
                partition_values = err['PartitionValues']
                partition_argument_entry = {}
                partition_argument_entry['PartitionValueList'] = partition_values
                partition_argument_entry['PartitionInput'] = get_partition_input(partition_argument, partition_values)
                partition_argument_for_overwrite['Entries'].append(partition_argument_entry)
            else:
                logger.error(f"Error occurred in batch_create_partition called for the table '{database_name}'.'{table_name}'")
                if args.skip_errors:
                    logger.error(f"Skipping error: {err}")
                else:
                    raise Exception(f"Error: {err}") 
        if len(partition_argument_for_overwrite['Entries']) > 0:
            res = dst_glue.batch_update_partition(**partition_argument_for_overwrite)
            for err in res['Errors']:
                logger.error(f"Error occurred in batch_update_partition called for the table '{database_name}'.'{table_name}'")
                if args.skip_errors:
                    logger.error(f"Skipping error: {err}")
                else:
                    raise Exception(f"Error: {err}")
    logger.info(f"The {len(partitions)} partitions in the table '{database_name}'.'{table_name}' have been created or updated.")


def get_partitions(database_name, table_name):
    if args.deserialize_file:
        return [p for p in resources.get('partitions', []) if p['DatabaseName'] == database_name and p['TableName'] == table_name]
    else:
        partitions = []
        get_partitions_paginator = src_glue.get_paginator('get_partitions')
        for page in get_partitions_paginator.paginate(DatabaseName=database_name, TableName=table_name):
            partitions.extend(page['Partitions'])
        return partitions


def synchronize_table(table, mapping):
    database_name = table['DatabaseName']
    table_name = table['Name']
    logger.debug(f"Synchronizing table '{database_name}'.'{table_name}'")

    if table['TableType'] == "GOVERNED":
        logger.warning(f"Table '{database_name}'.'{table_name}' is skipped since it is an AWS Lake Formation governed table.")
        return

    if 'TargetTable' in table:
        logger.warning(f"Table '{database_name}'.'{table_name}' is skipped since it is a resource link.")
        return

    # Organize table parameters
    table_argument = {}
    table_argument['TableInput'] = table
    table_argument = organize_table_param(table_argument, mapping)

    # Copy table configuration
    try:
        logger.debug(f"Checking if table '{database_name}'.'{table_name}' exists in the destination account.")
        current_table = dst_glue.get_table(DatabaseName=database_name, Name=table_name)
        logger.debug(f"Current table '{database_name}'.'{table_name}' configuration: {current_table}")
        if args.overwrite_tables:
            logger.debug(f"Updating table '{database_name}'.'{table_name}' with configuration: '{table_argument}'")
            if do_update:
                dst_glue.update_table(**table_argument)
            logger.info(f"The table '{database_name}'.'{table_name}' has been overwritten.")
    except dst_glue.exceptions.EntityNotFoundException:
        logger.debug(f"Creating table '{database_name}'.'{table_name}' with configuration: '{table_argument}'")
        if do_update:
            dst_glue.create_table(**table_argument)
        logger.info(f"New table '{database_name}'.'{table_name}' has been created.")
    except Exception as e:
        logger.error(f"Error occurred in copying table: '{database_name}'.'{table_name}'")
        if args.skip_errors:
            logger.error(f"Skipping error: {e}", exc_info=True)
        else:
            raise

    # Synchronize partitions
    partitions = get_partitions(database_name, table_name)
    n = 100
    for i in range(0, len(partitions), n):
        synchronize_partitions(database_name, table_name, partitions[i: i+n], mapping)


def get_database_names():
    if args.deserialize_file:
        return [db['Name'] for db in resources.get('databases', [])]
    else:
        if args.src_database_names:
            return args.src_database_names.split(',')
        else:
            database_names = []
            get_databases_paginator = src_glue.get_paginator('get_databases')
            for page in get_databases_paginator.paginate():
                database_names.extend([db['Name'] for db in page['DatabaseList']])
            return database_names


def get_database_definition(database_name):
    if args.deserialize_file:
        database_definitions = {db['Name']: db for db in resources.get('databases', [])}
        database = database_definitions.get(database_name)
        if not database:
            logger.error(f"Database '{database_name}' not found in deserialized data.")
        return database
    else:
        res = src_glue.get_database(Name=database_name)
        database = res['Database']
        return database


def get_table_names(database_name):
    if args.deserialize_file:
        return [table['Name'] for table in resources.get('tables', []) if table['DatabaseName'] == database_name]
    else:
        if args.src_table_names:
            return args.src_table_names.split(',')
        else:
            table_names = []
            get_tables_paginator = src_glue.get_paginator('get_tables')
            for page in get_tables_paginator.paginate(DatabaseName=database_name):
                table_names.extend([table['Name'] for table in page['TableList']])
            return table_names


def get_table_definition(database_name, table_name):
    if args.deserialize_file:
        table_definitions = {(table['DatabaseName'], table['Name']): table for table in resources.get('tables', [])}
        table = table_definitions.get((database_name, table_name))
        if not table:
            logger.error(f"Table '{database_name}'.'{table_name}' not found in deserialized data.")
        return table
    else:
        res = src_glue.get_table(DatabaseName=database_name, Name=table_name)
        table = res['Table']
        return table


def synchronize_database(database_name, mapping, database):
    logger.debug(f"Synchronizing database '{database_name}'")

    if 'TargetDatabase' in database:
        logger.warning(f"Database '{database_name}' is skipped since it is a resource link.")
        return

    # Organize database parameters
    database_argument = {}
    database_argument['DatabaseInput'] = database
    database_argument = organize_database_param(database_argument, mapping)

    # Copy database configuration
    try:
        logger.debug(f"Checking if database '{database_name}' exists in the destination account.")
        current_database = dst_glue.get_database(Name=database_name)
        logger.debug(f"Current database '{database_name}' configuration: {current_database}")
        if args.overwrite_databases:
            database_argument['Name'] = database_name
            logger.debug(f"Updating database '{database_name}' with configuration: '{database_argument}'")
            if do_update:
                dst_glue.update_database(**database_argument)
            logger.info(f"The database '{database_name}' has been overwritten.")
    except dst_glue.exceptions.EntityNotFoundException:
        logger.debug(f"Creating database '{database_name}' with configuration: '{database_argument}'")
        database_argument['DatabaseInput']['Name'] = database_name
        if do_update:
            dst_glue.create_database(**database_argument)
        logger.info(f"New database '{database_name}' has been created.")
    except Exception as e:
        logger.error(f"Error occurred in copying database: '{database_name}'")
        if args.skip_errors:
            logger.error(f"Skipping error: {e}", exc_info=True)
        else:
            raise

    # Proceed to synchronize tables
    table_names = get_table_names(database_name)
    for table_name in table_names:
        table = get_table_definition(database_name, table_name)
        if table:
            synchronize_table(table, mapping)


def get_connection_names():
    if args.deserialize_file:
        return [db['Name'] for db in resources.get('connections', [])]
    else:
        if args.src_connection_names:
            return args.src_connection_names.split(',')
        else:
            connection_names = []
            get_connections_paginator = src_glue.get_paginator('get_connections')
            for page in get_connections_paginator.paginate():
                connection_names.extend([db['Name'] for db in page['ConnectionList']])
            return connection_names


def get_connection_definition(connection_name):
    if args.deserialize_file:
        connection_definitions = {db['Name']: db for db in resources.get('connections', [])}
        connection = connection_definitions.get(connection_name)
        if not connection:
            logger.error(f"Connection '{connection_name}' not found in deserialized data.")
        return connection
    else:
        res = src_glue.get_connection(Name=connection_name)
        connection = res['Connection']
        return connection


def synchronize_connection(connection_name, mapping, connection):
    logger.debug(f"Synchronizing connection '{connection_name}'")

    # Organize connection parameters
    connection_argument = {}
    connection_argument['ConnectionInput'] = connection
    connection_argument = organize_connection_param(connection_argument, mapping)

    # Copy connection configuration
    try:
        logger.debug(f"Checking if connection '{connection_name}' exists in the destination account.")
        current_connection = dst_glue.get_connection(Name=connection_name)
        logger.debug(f"Current connection '{connection_name}' configuration: {current_connection}")
        if args.overwrite_connections:
            connection_argument['Name'] = connection_name
            logger.debug(f"Updating connection '{connection_name}' with configuration: '{connection_argument}'")
            if do_update:
                dst_glue.update_connection(**connection_argument)
            logger.info(f"The connection '{connection_name}' has been overwritten.")
    except dst_glue.exceptions.EntityNotFoundException:
        logger.debug(f"Creating connection '{connection_name}' with configuration: '{connection_argument}'")
        connection_argument['ConnectionInput']['Name'] = connection_name
        if do_update:
            dst_glue.create_connection(**connection_argument)
        logger.info(f"New connection '{connection_name}' has been created.")
    except Exception as e:
        logger.error(f"Error occurred in copying connection: '{connection_name}'")
        if args.skip_errors:
            logger.error(f"Skipping error: {e}", exc_info=True)
        else:
            raise


def main():
    global resources

    if args.config_path:
        logger.debug(f"Loading Mapping config file: {args.config_path}")
        mapping = load_mapping_config_file(args.config_path)
    else:
        logger.debug(f"Mapping config file is not given.")
        mapping = None

    if args.serialize_file and args.deserialize_file:
        logger.error("You cannot specify both --serialize-to-file and --deserialize-from-file simultaneously.")
        sys.exit(1)

    # Handle serialization
    if args.serialize_file:
        # Initialize resources dictionary
        resources = {}

        # Serialize jobs
        if "job" in args.targets:
            job_names = get_job_names()
            jobs = []
            for job_name in job_names:
                job = get_job_definition(job_name)
                if job:
                    jobs.append(job)
            resources['jobs'] = jobs

        # Serialize Glue Catalog resources
        if "catalog" in args.targets:
            database_names = get_database_names()
            databases = []
            tables = []
            partitions = []
            for database_name in database_names:
                database = get_database_definition(database_name)
                if database:
                    databases.append(database)
                    table_names = get_table_names(database_name)
                    for table_name in table_names:
                        table = get_table_definition(database_name, table_name)
                        if table:
                            tables.append(table)
                            partition_list = get_partitions(database_name, table_name)
                            partitions.extend(partition_list)
            resources['databases'] = databases
            resources['tables'] = tables
            resources['partitions'] = partitions

        # Serialize connections
        if "connection" in args.targets:
            connection_names = get_connection_names()
            connections = []
            for connection_name in connection_names:
                connection = get_connection_definition(connection_name)
                if connection:
                    connections.append(connection)
            resources['connections'] = connections

        # Write resources to the file
        with open(args.serialize_file, 'w') as f:
            json.dump(resources, f, default=str)
        logger.info(f"Resources serialized to file {args.serialize_file}")
        sys.exit(0)

    # Handle deserialization
    if args.deserialize_file:
        # Read resources from the file
        with open(args.deserialize_file, 'r') as f:
            resources = json.load(f)
        logger.info(f"Resources deserialized from file {args.deserialize_file}")

    # Proceed with synchronization
    if "job" in args.targets:
        job_names = get_job_names()
        for job_name in job_names:
            job = get_job_definition(job_name)
            if job:
                synchronize_job(job_name, mapping, job)

    if "catalog" in args.targets:
        database_names = get_database_names()
        for database_name in database_names:
            database = get_database_definition(database_name)
            if database:
                synchronize_database(database_name, mapping, database)

    if "connection" in args.targets:
        connection_names = get_connection_names()
        for connection_name in connection_names:
            connection = get_connection_definition(connection_name)
            if connection:
                synchronize_connection(connection_name, mapping, connection)


if __name__ == "__main__":
    main()
