## Crawler Scripts

Crawler scripts are AWS Glue ETL scripts to help manage the effects of your AWS Glue crawlers. These scripts help maintain the integrity of your AWS Glue Data Catalog and ensure that unwanted effects can be undone.

### Crawler Undo Script

The goal of the crawler undo script (crawler_undo.py) is to ensure that the effects of a crawler can be undone. This enables you to back out updates if they were responsible for unwanted changes in your  Data Catalog. The script follows these steps:

1.	Given the name of an AWS Glue crawler, the script determines the database for this crawler and the timestamp at which the crawl was last started.
2.	Then, the script stores a backup of the current database in a json file to an Amazon S3 location you specify (if you don't specify any, no backup is collected).
3.	Next, the script gets the most recent version of each table which is earlier than the timestamp, then updates each table to that version.
4.	The script deletes all tables and partitions that were created after the timestamp, and removes the deprecate flag for all tables that were deprecated after the timestamp.

To use this script, follow these steps:

1.	First, store the `crawler_undo.py` and `script_utils.py` scripts on an Amazon S3 bucket you own.
2.	Go to the AWS Glue console and choose  Add Job from the jobs list page .
3.	Specify a job name and an IAM role. Make sure the IAM role has permissions to read from and write to your AWS Glue Data Catalog, as well as, S3 read and write permission if a backup location is used.
4.	Specify the script file name as crawler_undo.py and specify the S3 path location where your copy of the script is stored. Specify other fields required to define a job.
5.	Under Script libraries and job parameters, specify the Python library path S3 location where your copy of the `script_utils.py` script is stored. You can leave other parameters to their defaults. You do not need a connection for this job.
6.	Run your job with the following parameters:
   - **crawler name** (required): the name of the crawler to undo.
   - **backup location** (optional): the name of an S3 location where a backup for this operation is stored. If not specified, no backup is created. 
   - **database name** (optional): the name of the database that contains the tables in which to undo changes. If not specified, the database target of the crawler specified by the crawler definition is used instead.
   - **timestamp** (optional): Timestamp in epoch time (specified in milliseconds) after which all changes are undone. If not specified, the start timestamp of the crawler specified by the crawler definition is used instead.
   - **region** (optional): AWS region of the crawler to undo. If not specified, defaults to us-east-1.
 
#### Notes

 - **The automatic database name and timestamp determination will not work properly until the AWS Glue model file is made available. Until then, database name and timestamp are required.**
 - You do not have to specify an S3 backup location, but we recommend it. Be wary that by specifying a backup location, the IAM role you pick to run this script must have S3 put-object permission on the specified location.

#### Limitations

 - The script cannot undo a hard deletion (a call to the delete API of the AWS Glue Data Catalog). If you wish to be able to undo deletions, use the DEPRECATE mode when specifying crawler behavior.
 - The script does not undo the updates made to partitions created before the last run of a crawler.
 - The script does not revert the creation and last update timestamps of objects created before the last run of a crawler. Those timestamps are updated to correspond to the time at which the crawler undo script was run.
 - This script is not meant to be used while a crawler is running. The behavior is non-deterministic if a crawler is still running when this script is run.
 - The behavior of this script is also non-deterministic if a user changes tables created by this crawler while this script is running.

### Crawler Redo From Backup Script

The goal of the crawler redo-from-backup script is to ensure that the effects of a crawler can be redone after an undo. The script follows these steps:

1. Given the name of an AWS Glue crawler, the script determines the database for this crawler.
2. The crawler fetches a backup specified by an S3 location. The S3 backup location itself should point to the output of backing up when running the undo script.
3. The backup is re-applied to the database.

To use this script, follow these steps:

 1. First, store the `crawler_redo_from_backup.py` and `script_utils.py` scripts on an Amazon S3 bucket you own.
 2. Go to the Glue console and choose `Add Job` from the jobs list page.
 3. Specify a job name and an IAM role. Make sure your IAM role has permissions to read from and write to your Glue data catalog, as well as, Amazon S3 read and write permission if a backup location is used.
 4. Specify the script file name as `crawler_redo_from_backup.py` and specify the Amazon S3 path location where your copy of the script is stored. Specify other fields required to define a job.
 5. Under Script libraries and job parameters, specify the Python library path S3 location where your copy of the `script_utils.py` script is stored. You can leave other parameters to their defaults. You do not need a connection for this job.
 6. Run your job with the following parameters:
   - **crawler name** (required): the name of the crawler to undo.
   - **backup location** (required): the name of the S3 location where the backup for this operation is stored. 
   - **database name** (optional): the name of the database that contains the tables in which to undo changes. If not specified, the database target of the crawler specified by the crawler definition is used instead.
   - **region** (optional): region of the crawler to redo. If not specified, defaults to `us-east-1`.

#### Note

 - **The automatic database name and timestamp determination will not work properly until the AWS Glue model file is made available. Until then, database name and timestamp are required.**

#### Limitations

- The redo from backup can only redo the changes that have been stored in the backup location.
- This script is not meant to be used while a crawler is running. The behavior is non deterministic if a crawler is still running when this script is run.
- The behavior of this script is also non deterministic if a user changes tables created by this crawler while this script is running.
