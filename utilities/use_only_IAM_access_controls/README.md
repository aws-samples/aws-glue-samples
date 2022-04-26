# Using only IAM access controls

AWS Lake Formation applies its own permission model when you access data in Amazon S3 and metadata in AWS Glue Data Catalog through use of Amazon EMR, Amazon Athena and so on. If you currently use Lake Formation and instead would like to use only IAM Access controls, this tool enables you to achieve it.

### Command line Syntax
```
$ update_permission.py [-h] [-p PROFILE] [-r REGION] [-g GLOBAL_CONF] [-d TARGET_DATABASES] [--skip-errors] [--dryrun] [-v]

optional arguments:
  -h, --help            show this help message and exit
  -p PROFILE, --profile PROFILE
                        AWS named profile name
  -r REGION, --region REGION
                        AWS Region (e.g. "us-east-1")
  -g GLOBAL_CONF, --global GLOBAL_CONF
                        (Optional) Update global configurations; Data Lake Settings and Data lake locations (default: true)
  -d TARGET_DATABASES, --databases TARGET_DATABASES
                        (Optional) The comma separated list of target database names (default: all databases)
  --skip-errors         (Optional) Skip errors and continue execution. (default: false)
  --dryrun              (Optional) Display the operations that would be performed using the specified command without actually running them (default: false)
  -v, --verbose         (Optional) Display verbose logging (default: false)
```

## How to use it

You can run this script in any location where you have Python3 and AWS credentials.

```
$ python3 update_permission.py
```

You can provide an AWS named profile name, and/or Region name.

```
$ python3 update_permission.py -r us-west-2 -p <your-profile-name>
```

You need use AWS credentials of the IAM user/role who has Lake Formation admin permission. If you run the script without Lake Formation admin permission, you will see ‘Access Denied’ exception.


## How it works

It will perform following actions.

1. Modify data lake settings to use only IAM access controls
2. De-register all the data lake locations
3. Grant CREATE_DATABASE to IAM_ALLOWED_PRINCIPALS for catalog
4. Grant ALL to IAM_ALLOWED_PRINCIPALS for existing databases and tables
5. Revoke all the permissions except IAM_ALLOWED_PRINCIPALS
