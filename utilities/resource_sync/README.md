## Glue Resource Sync Utility

Glue Resource Sync Utility is a python application that enables you to synchronize your AWS Glue resources (jobs, databases, tables, and partitions) from one environment (region, account) to another.

This utility is built on top of Glue Visual Job API, and the utility lets you synchronize the jobs to different accounts without losing the visual representation. 
The utility also supports synchronizing Glue Catalog resources including databases, tables, and partitions.
Optionally, you can provide a list of resources that you want to synchronize, and specify how the utility should replace your environment-specific objects using a mapping file. For example, Amazon Simple Storage Service (Amazon S3) locations in your development environment and role can be different than your production environment. The mapping config file will be used to replace the environment specific objects.

Note: This utility is not designed for synchronizing huge number of tables and partitions at one time, and it can take long to complete that.

## How to use it

You can run this utility in any location where you have Python and the following environment.

### Pre-requisite
* Python 3.6 or later
* Latest version of boto3

### Command line Syntax
```
$ sync.py [-h] [--targets TARGETS] [--src-job-names SRC_JOB_NAMES] [--src-database-names SRC_DATABASE_NAMES] [--src-table-names SRC_TABLE_NAMES] [--src-profile SRC_PROFILE] [--src-region SRC_REGION]
               [--src-s3-endpoint-url SRC_S3_ENDPOINT_URL] [--src-sts-endpoint-url SRC_STS_ENDPOINT_URL] [--src-glue-endpoint-url SRC_GLUE_ENDPOINT_URL] [--dst-profile DST_PROFILE]
               [--dst-region DST_REGION] [--dst-s3-endpoint-url DST_S3_ENDPOINT_URL] [--dst-sts-endpoint-url DST_STS_ENDPOINT_URL] [--dst-glue-endpoint-url DST_GLUE_ENDPOINT_URL]
               [--sts-role-arn STS_ROLE_ARN] [--src-role-arn SRC_ROLE_ARN] [--dst-role-arn DST_ROLE_ARN] [--skip-no-dag-jobs SKIP_NO_DAG_JOBS] [--overwrite-jobs OVERWRITE_JOBS]
               [--overwrite-databases OVERWRITE_DATABASES] [--overwrite-tables OVERWRITE_TABLES] [--copy-job-script COPY_JOB_SCRIPT] [--config-path CONFIG_PATH] [--skip-errors] [--dryrun]
               [--skip-prompt] [-v]

optional arguments:
  -h, --help            show this help message and exit
  --targets TARGETS     The comma separated list of targets [job, catalog]. If it is not set, [job] will be chosen. (possible values: [job, catalog]. default: job)
  --src-job-names SRC_JOB_NAMES
                        The comma separated list of the names of AWS Glue jobs which are going to be copied from source AWS account. If it is not set, all the Glue jobs in the source account will be copied to the destination account.
  --src-database-names SRC_DATABASE_NAMES
                        The comma separated list of the names of AWS Glue databases which are going to be copied from source AWS account. If it is not set, all the Glue databases in the source account will be copied to the destination account.
  --src-table-names SRC_TABLE_NAMES
                        The comma separated list of the names of AWS Glue tables which are going to be copied from source AWS account. If it is not set, all the Glue tables in the specified databases will be copied to the destination account.
  --src-profile SRC_PROFILE
                        AWS named profile name for source AWS account.
  --src-region SRC_REGION
                        Source region name.
  --src-role-arn SRC_ROLE_ARN
                        IAM role ARN to be assumed to access source account resources.
  --src-s3-endpoint-url SRC_S3_ENDPOINT_URL
                        Source endpoint URL for Amazon S3.
  --src-sts-endpoint-url SRC_STS_ENDPOINT_URL
                        Source endpoint URL for AWS STS.
  --src-glue-endpoint-url SRC_GLUE_ENDPOINT_URL
                        Source endpoint URL for AWS Glue.
  --dst-profile DST_PROFILE
                        AWS named profile name for destination AWS account.
  --dst-region DST_REGION
                        Destination region name.
  --dst-s3-endpoint-url DST_S3_ENDPOINT_URL
                        Destination endpoint URL for Amazon S3.
  --dst-sts-endpoint-url DST_STS_ENDPOINT_URL
                        Destination endpoint URL for AWS STS.
  --dst-glue-endpoint-url DST_GLUE_ENDPOINT_URL
                        Destination endpoint URL for AWS Glue.
  --sts-role-arn STS_ROLE_ARN
                        IAM role arn to be assumed to access destination account resources.
  --dst-role-arn DST_ROLE_ARN
                        IAM role ARN to be assumed to access destination account resources.
  --skip-no-dag-jobs SKIP_NO_DAG_JOBS
                        Skip Glue jobs which do not have DAG. (possible values: [true, false]. default: true)
  --overwrite-jobs OVERWRITE_JOBS
                        Overwrite Glue jobs when the jobs already exist. (possible values: [true, false]. default: true)
  --overwrite-databases OVERWRITE_DATABASES
                        Overwrite Glue databases when the tables already exist. (possible values: [true, false]. default: true)
  --overwrite-tables OVERWRITE_TABLES
                        Overwrite Glue tables when the tables already exist. (possible values: [true, false]. default: true)
  --copy-job-script COPY_JOB_SCRIPT
                        Copy Glue job script from the source account to the destination account. (possible values: [true, false]. default: true)
  --config-path CONFIG_PATH
                        The config file path to provide parameter mapping. You can set S3 path or local file path.
  --skip-errors         (Optional) Skip errors and continue execution. (default: false)
  --dryrun              (Optional) Display the operations that would be performed using the specified command without actually running them (default: false)
  --skip-prompt         (Optional) Skip prompt (default: false)
  -v, --verbose         (Optional) Display verbose logging (default: false)
```

### Config file
When you synchronize your Glue resources into another account, all the account specific parameters need to be replaced.
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

When you provide `--src-role-arn`, `--dst-role-arn`, or `--sts-role-arn`, this utility assumes the role and uses it to access resources in the respective account.
You need to create IAM roles in the source and/or destination accounts, and configure the trust relationship for the roles to allow `AssumeRole` calls from the account running the script.

- Use `--src-role-arn` for accessing the source account resources.
- Use `--dst-role-arn` or `--sts-role-arn` for accessing the destination account resources. Both serve the same purpose, with `--sts-role-arn` maintained for backward compatibility.

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

### Example 4. Synchronize all Glue Catalog resources to another account using AWS named profile
```
$ python3 sync.py --targets catalog --src-profile test1 --src-region eu-west-3 --dst-profile test2 --dst-region eu-west-3 --config-path mapping.json
```

### Example 5. Synchronize selected Glue Catalog resources which match specific database name and table name to another account using AWS named profile
```
$ python3 sync.py --targets catalog --src-profile test1 --src-region eu-west-3 --dst-profile test2 --dst-region eu-west-3 --src-database-names test --src-table-names products1,products2 --config-path mapping.json
```

### Limitations
* Following resources are not synchronized.
  * Catalog
  * Crawlers
  * Classifiers
  * Lake Formation resource links
  * Lake Formation governed tables
  * Partition indexes
  * Connections
  * Schema Registry
  * Security configurations
  * Triggers
  * Workflows
  * Blueprints
  * Development Endpoints
  * Job Bookmarks
  * Interactive Sessions
  * ML Transforms
  * Data Quality Rules
  * AWS Tags
