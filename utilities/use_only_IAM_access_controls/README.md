# Using only IAM access controls

AWS Lake Formation applies its own permission model when you access data in Amazon S3 and metadata in AWS Glue Data Catalog through use of Amazon EMR, Amazon Athena and so on. If you currently use Lake Formation and instead would like to use only IAM Access controls, this tool enables you to achieve it.

## How to use it

You can run this script in any location where you have Python3 and AWS credentials.

```
$ python update_permission.py
```

You need use AWS credentials of the IAM user/role who has Lake Formation admin permission. If you run the script without Lake Formation admin permission, you will see ‘Access Denied’ exception.


## How it works

It will perform following actions.

1. Modify data lake settings to use only IAM access controls
2. De-register all the data lake locations
3. Grant CREATE_DATABASE to IAM_ALLOWED_PRINCIPALS for catalog
4. Grant ALL to IAM_ALLOWED_PRINCIPALS for existing databases and tables
5. Revoke all the permissions except IAM_ALLOWED_PRINCIPALS
