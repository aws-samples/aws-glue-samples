## Glue Job Sync Utility

This utility helps you to synchronize Glue Visual jobs from one environment to another without losing visual representation.

## How to use it

You can run this utility in any location where you have Python and the following environment.

### Pre-requisite
* Python 3.6 or later
* Latest version of boto3

### Command line Syntax
```
$ sync.py [-h] [--src-job-names SRC_JOB_NAMES] [--src-profile SRC_PROFILE] [--src-region SRC_REGION] [--dst-profile DST_PROFILE] [--dst-region DST_REGION] [--sts-role-arn STS_ROLE_ARN] [--skip-no-dag-jobs SKIP_NO_DAG_JOBS]
                  [--overwrite-jobs OVERWRITE_JOBS] [--copy-job-script COPY_JOB_SCRIPT] [--config-path CONFIG_PATH] [-v]

optional arguments:
  -h, --help            show this help message and exit
  --src-job-names SRC_JOB_NAMES
                        The comma separated list of the names of AWS Glue jobs which are going to be copied from source AWS account. If it is not set, all the Glue jobs in the source account will be copied to the destination account.
  --src-profile SRC_PROFILE
                        AWS named profile name for source AWS account.
  --src-region SRC_REGION
                        Source region name.
  --dst-profile DST_PROFILE
                        AWS named profile name for destination AWS account.
  --dst-region DST_REGION
                        Destination region name.
  --sts-role-arn STS_ROLE_ARN
                        IAM role arn to be assumed to access destination account resources.
  --skip-no-dag-jobs true/false
                        Skip Glue jobs which do not have DAG. (possible values: [true, false]. default: true)
  --overwrite-jobs true/false
                        Overwrite Glue jobs when the jobs already exist. (possible values: [true, false]. default: true)
  --copy-job-script true/false
                        Copy Glue job script from the source account to the destination account. (possible values: [true, false]. default: true)
  --config-path CONFIG_PATH
                        The config file path to provide parameter mapping. You can set S3 path or local file path.
  -v, --verbose         (Optional) Display verbose logging (default: false)
```

### Config file
When you synchronize your Glue Visual jobs into another account, all the account specific parameters need to be replaced.
This utility allows you to provide simple config file to determine the mapping rules to replace values.

The config file needs to be written in flat JSON. You can place this config file in local or S3.

Example:
```
{
    "123456789012": "234567890123",
    "aws-glue-assets-123456789012-eu-west-3": "aws-glue-assets-234567890123-eu-west-3",
    "GlueServiceRoleSourceAccount": "GlueServiceRoleDestinationAccount"
}
```


### Credentials
There are two options to configure credentials for accessing resources in target accounts.

#### Profile

When you provide `--dst-profile`, this utility uses AWS Named profile set in the option to access resources in the destination account.

#### STS AssumeRole

When you provide `--sts-role-arn`, this utility assumes the role and use the role to access resources in the destination account.
You need to create IAM role in the destination account, and configure the trust relationship for the role to allow `AssumeRole` calls from the source account.

## Examples

### Example 1. Synchronize single Glue Visual job to another account using AWS named profile
```
$ python3 sync.py --src-profile test1 --src-region eu-west-3 --dst-profile test2 --dst-region eu-west-3 --src-job-names dag-test --config-path mapping.json
```
* This command synchronizes only one job named `dag-test`.

### Example 2. Synchronize all Glue Visual jobs to another account using AWS named profile
```
$ python3 sync.py --src-profile test1 --src-region eu-west-3 --dst-profile test2 --dst-region eu-west-3 --config-path mapping.json
```
* This command synchronizes all the Glue Visual jobs because `--src-job-names` is not provided.

### Example 3. Synchronize all Glue Studio jobs to another account using STS temporary credential
```
$ python3 sync.py --sts-role-arn arn:aws:iam::234567890123:role/admin --src-region eu-west-3 --dst-region eu-west-3 --config-path s3://path_to_config/mapping.json
```
* This example expects that you have default credential for the source account (e.g. IAM role, etc.).
* As you can see in this example, the mapping config file can be placed in S3.