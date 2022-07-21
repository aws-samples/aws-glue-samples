## Glue Job Version Deprecation Checker

Per [AWS Glue version support policy](https://docs.aws.amazon.com/glue/latest/dg/glue-version-support-policy.html), Glue jobs using specific versions are going to be deprecated.
This command line utility helps you to identify the target Glue jobs which will be deprecated.

## How to use it

You can run this shell script in any location where you have Bash and the following environment.

### Pre-requisite
* Configure default region. See details: [Configuring the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html)
* Install [AWS CLI](https://aws.amazon.com/cli/)

### Syntax
```
$ ./check_jobs.sh [ -r COMMA_SEPARATED_REGION_NAMES ] [ -p PROFILE_NAME ]
```

* Glue jobs in all regions are listed if the option `-r` is not specified.

### Example 1. Check Glue jobs in all regions
```
$ ./check_jobs.sh
```

### Example 2. Check Glue jobs only in us-east-1 and us-west-2
```
$ ./check_jobs.sh -r us-east-1,us-west-2
```

### Example 3. Check Glue jobs only in us-east-1 and us-west-2 using AWS named profile
```
$ ./check_jobs.sh -r us-west-2 -p test_profile
```

### Example of command output
```
Following Glue jobs are going to be deprecated as per AWS Glue's version support policy. 
Region: us-west-2
[End of Support: 2022/6/1] Spark | Glue 0.9
-------------------------------------------------------------------------------------------
|                                         GetJobs                                         |
+-------------+------------------------------------------+--------------+-----------------+
| GlueVersion |                 JobName                  |   JobType    |  PythonVersion  |
+-------------+------------------------------------------+--------------+-----------------+
|  0.9        |  sample_spark_job                        |  glueetl     |  2              |
+-------------+------------------------------------------+--------------+-----------------+
...
[End of Support: 2022/6/1] Python shell | Glue 1.0 (Python 2)
-------------------------------------------------------------------------------------------
|                                         GetJobs                                         |
+-------------+------------------------------------------+--------------+-----------------+
| GlueVersion |                 JobName                  |   JobType    |  PythonVersion  |
+-------------+------------------------------------------+--------------+-----------------+
|  1.0        |  sample_pythonshell                      |  pythonshell |  2              |
+-------------+------------------------------------------+--------------+-----------------+
...
...

```
