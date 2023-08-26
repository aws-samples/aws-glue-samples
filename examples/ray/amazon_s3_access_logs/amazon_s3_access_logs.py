#  Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT-0

# Initializations
import io
import logging
import os
from collections import namedtuple
from datetime import datetime, timedelta, timezone
from typing import Union
from urllib.parse import urlparse

import boto3
import pandas as pd
import pyarrow
import ray
from dateutil import rrule
from pyarrow import csv, parquet
from ray.data import Dataset
from ray.types import ObjectRef

# Logging Config
logging = logging.getLogger()
logging.setLevel("INFO")


# Class for Parsing S3 URLs
class S3Url(object):
    def __init__(self, url):
        self._parsed = urlparse(url, allow_fragments=False)

    @property
    def bucket(self):
        return self._parsed.netloc

    @property
    def key(self):
        if self._parsed.query:
            return self._parsed.path.lstrip("/") + "?" + self._parsed.query
        else:
            return self._parsed.path.lstrip("/")

    @property
    def url(self):
        return self._parsed.geturl()


# Args and CONSTANTS
req_params = ["source-s3-uri", "dest-s3-uri", "checkpoint-uri"]

for param in req_params:
    if param in os.environ:
        logging.info(f"Required job parameter {param} passed in. Using value: {os.environ[param]}")
    else:
        raise ValueError(
            f"Required job parameter {param} not passed in. Please pass it with `--{param}` in the Job parameters."
        )

if "log-object-prefix" in os.environ:
    logging.info(f"log-object-prefix passed in. Using value: {os.environ['log-object-prefix']}")
else:
    logging.info('No log-object-prefix passed in. Using default value "".')

if "list-bucket-size" in os.environ:
    logging.info(f"list-bucket-size passed in. Using value: {os.environ['list-bucket-size']}")
else:
    logging.info('No list-bucket-size passed in. Using default value "HOURLY".')

# Parse S3 URLs
# "S3://SOURCE_BUCKET/SOURCE_PATH/"
SOURCE_URI = S3Url(os.environ["source-s3-uri"])

# "s3://DESTINATION_BUCKET/ACCESS_LOGS_PARQUET/"
DEST_URI = S3Url(os.environ["dest-s3-uri"])

# "s3://DESTINATION_BUCKET/CHECKPOINT.txt"
CHECKPOINT_URI = S3Url(os.environ["checkpoint-uri"])

# S3 Access Log files can have a prefix to the file name before the date
# "SOURCE_BUCKET"
LOG_PREFIX = f"{SOURCE_URI.key}{os.getenv('log-object-prefix', default='')}"

# Specify the split size to use in listing.
LIST_BATCH_SIZE = os.getenv("list-bucket-size", default="HOURLY")

# Ray Init
logging.info("Initializing ray")
ray.init()

s3 = boto3.client("s3")  # type:ignore

# Get checkpoint file from S3 for date selection. Create one if it does not exist.


def checkpoint_from_s3(bucket_name: str, object_key: str) -> datetime:
    """Retrieve a checkpoint file from S3.
    If the file does not exist, return the current time minus one batch size.
    If the file exists, return the timestamp contained in the file.
    Args:
        bucket_name (str): The name of the bucket to retrieve the file from.
        object_key (str): The path and name of the file to retrieve.
    Returns:
        datetime: The timestamp contained in the file.
    """
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=object_key)["Body"].read()
        _ts_checkpoint = datetime.fromisoformat(obj.decode())
    except s3.exceptions.NoSuchKey:
        logging.info("Error accessing checkpoint file: Key Does not Exist")
        # Run the first batch of all the logs in the last 24 hours if no checkpoint exists
        _ts_checkpoint = datetime.now(timezone.utc) - timedelta(days=1)
        logging.info(f"No Checkpoint: Using past hour {_ts_checkpoint.isoformat()}")
    except s3.exceptions.NoSuchBucket as err:
        logging.info("Error accessing checkpoint file: Bucket Does not Exist")
        raise
    except s3.exceptions.ClientError as err:
        if "(AccessDenied)" in str(err):
            logging.warning(err)
        raise
    return _ts_checkpoint


# Write Checkpoint File to S3
def checkpoint_to_s3(bucket_name: str, object_key: str, timestamp: datetime) -> datetime:
    """Write a timestamp to a checkpoint file in S3.
    Args:
        bucket_name (str): The name of the bucket to write the file to.
        object_key (str): The path and name of the file to write.
    Returns:
        datetime: The timestamp written to the file.

    """
    ts_str: str = timestamp.isoformat()
    # Encode timestamp string to bytes
    ts_bytes: bytes = ts_str.encode("utf-8")
    # Create a BytesIO object to hold timestamp bytes
    timestamp_bytes = io.BytesIO(ts_bytes)
    try:
        logging.info(f"Uploading Checkpoint of {ts_str} to s3://{bucket_name}/{object_key}")
        s3.upload_fileobj(timestamp_bytes, bucket_name, object_key)
        return timestamp
    except s3.exceptions.ClientError as err:
        if "(AccessDenied)" in str(err):
            logging.warning("Failed timestamp upload: Access Denied")
            raise
        else:
            logging.warning("Failed to upload checkpoint: Another Error has occurred")
            raise


# Generate list of hourly prefixes based on start_date and end_date to filter by with rrule
def generate_prefix_list(prefix_size: str, start_date: datetime, end_date: datetime) -> list[str]:
    """
    Generate a list of time-based prefixes formatted like the SAL filename.
    Using the list of dates, generate a list of S3 prefixes for
    the objects representing a given period of time.
    For datasets with fewer objects performance may improve
    by using a larger prefix such as DAY or MONTH

    Args:
        prefix_size (str): The size of the prefix to be generated.
        Must be HOURLY, DAILY, MONTHLY or YEARLY
        start_date (datetime): Time of first prefix to be included.
        end_date (datetime): Time of last prefix to be included.

    Returns:
        list[str]: List of S3 prefixes.
    """
    PrefixSize = namedtuple("PrefixSize", ["date_pattern", "rrule_size"])
    _prefixes: dict[str, PrefixSize] = {
        "YEARLY": PrefixSize("%Y", rrule.YEARLY),
        "MONTHLY": PrefixSize("%Y-%m", rrule.MONTHLY),
        "DAILY": PrefixSize("%Y-%m-%d", rrule.DAILY),
        "HOURLY": PrefixSize("%Y-%m-%d-%H", rrule.HOURLY),
    }
    chosen_prefix = _prefixes[prefix_size] if prefix_size is not None else _prefixes["DAILY"]

    prefix_list = [
        f"{LOG_PREFIX}{dt.strftime(chosen_prefix.date_pattern)}"
        for dt in rrule.rrule(chosen_prefix.rrule_size, dtstart=start_date, until=end_date)
    ]

    print(f"Total number of generated prefixes: {len(prefix_list)}")
    return prefix_list


def invalid_row_handler(row: str) -> str:
    """Ray Data Invalid Row Handler..
    This function tells Ray Data to ignore rows that do not match the declared schema.
    Args:
        row (str): Row to be handled.
    Returns:
        str: "skip" to skip row.
    """
    logging.warning(f"Invalid Row: {row}")
    return "skip"


# Declare Pandas UDF For Cleaning
def log_transforms_udf(data: pd.DataFrame) -> pd.DataFrame:
    """
    pandas transformations to clean the logs and add region & storage class.
    This transformation logic should be modified to fit your needs.
    Args:
        data (pd.DataFrame): DataFrame to be cleaned.
    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    fmt = "%d/%b/%Y:%H:%M:%S"
    data["requestdatetime"] = pd.to_datetime(data["requestdatetime"].str.strip("[]"), format=fmt)
    data["requestdate"] = data["requestdatetime"].dt.date
    data["requesthour"] = data["requestdatetime"].dt.hour
    # drop requestdatetime as asked, and tzoffset which is an extra column
    data = data.drop(["requestdatetime", "tzoffset"], axis=1)
    data["requesttype"] = data["request_uri"].str.strip("[]").str.split(" ").str[0]
    return data


# Declare Read Logs Logic for each Task
def read_logs(object_list: list[str]) -> Dataset:
    """Read the S3 Access Logs as a Ray Dataset. Specifying strict structure to allow Arrow to parse it.
    Column info found at https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html#log-record-fields

    Args:
        object_list (list[str]): List of S3 objects to be read.
    Returns:
        Dataset: Ray Dataset of the S3 Access Logs.
    """
    # Set Reader Properties
    col_names = [
        "bucket_owner",
        "bucket",
        "requestdatetime",
        "tzoffset",  # Added due to non-escaped space; not part of actual schema
        "remote_ip",
        "requester",
        "request_id",
        "operation",
        "key",
        "request_uri",
        "http_status",
        "error_code",
        "bytes_sent",
        "object_size",
        "total_time",
        "turn_around_time",
        "referrer",
        "user_agent",
        "version_id",
        "host_id",
        "signature_version",
        "cipher_suite",
        "auth_type",
        "host_header",
        "tls_version",
        "access_point_arn",
        "acl_required",
    ]
    column_types = pyarrow.schema(
        [
            ("bucket_owner", pyarrow.string()),
            ("bucket", pyarrow.string()),
            ("requestdatetime", pyarrow.string()),
            ("tzoffset", pyarrow.string()),
            ("remote_ip", pyarrow.string()),
            ("requester", pyarrow.string()),
            ("request_id", pyarrow.string()),
            ("operation", pyarrow.string()),
            ("key", pyarrow.string()),
            ("request_uri", pyarrow.string()),
            ("http_status", pyarrow.int16()),
            ("error_code", pyarrow.string()),
            ("bytes_sent", pyarrow.int64()),
            ("object_size", pyarrow.float64()),
            ("total_time", pyarrow.float64()),
            ("turn_around_time", pyarrow.float64()),
            ("referrer", pyarrow.string()),
            ("user_agent", pyarrow.string()),
            ("version_id", pyarrow.string()),
            ("host_id", pyarrow.string()),
            ("signature_version", pyarrow.string()),
            ("cipher_suite", pyarrow.string()),
            ("auth_type", pyarrow.string()),
            ("host_header", pyarrow.string()),
            ("tls_version", pyarrow.string()),
            ("access_point_arn", pyarrow.string()),
            ("acl_required", pyarrow.string()),
        ]
    )
    read_options = csv.ReadOptions(column_names=col_names)
    parse_options = csv.ParseOptions(
        delimiter=" ", quote_char='"', invalid_row_handler=invalid_row_handler
    )
    convert_options = csv.ConvertOptions(
        null_values=["-"],
        strings_can_be_null=True,
        include_missing_columns=True,
        include_columns=col_names,
        column_types=column_types,
    )
    # Using fastMetadataProvider to read faster & materialize due to 2.4 bug with fastMetadataProvider.
    ds: Dataset = ray.data.read_csv(
        object_list,
        meta_provider=ray.data.datasource.file_meta_provider.FastFileMetadataProvider(),
        parse_options=parse_options,
        read_options=read_options,
        convert_options=convert_options,
    )

    # If using FastFileMetadataProvider or pipelining need to read all metadata
    logging.info("Materializing Data Read")
    ds.materialize()
    logging.info(f"Dataset Info: ")
    logging.info(f"ds Size: {ds.size_bytes()}")
    logging.info(f"ds Block Count: {ds.num_blocks()}")
    logging.info(msg=f"ds Default Batch Format: {ds.default_batch_format()}")

    return ds


# Declare Ray Task to list files in a given S3 bucket and prefix.
def write_to_parquet(cleaned: Dataset) -> None:
    """Write the data to partitioned parquet by converting to ArrowRefs and writing as separate Tasks
    Args:
        cleaned (Dataset): Cleaned Ray Dataset.
    Returns:
        None (writes to S3 directly via Arrow API, no return value needed for this function call)
    """
    logging.info("Converting to Arrow and writing data")
    # Convert to Arrow Table
    arrow_refs: list[ObjectRef[pyarrow.Table]] = cleaned.to_arrow_refs()

    # Task to write Arrow Tables
    @ray.remote
    def write_arrow_table(arrow_table: ObjectRef[pyarrow.Table], write_path: str):
        """Ray Task to actually write the arrow refs to partitioned parquet.

        Args:
            arrow_table: (ObjectRef[pyarrow.Table]) Arrow Table to write.
            write_path: (str) S3 path to write to.
        Returns:
            None (writes to S3 directly via Arrow API, no return value needed for this function call)
        """
        results = parquet.write_to_dataset(arrow_table, write_path, partition_cols=["requestdate"])
        return results

    write_futures = [write_arrow_table.remote(arrow_ref, DEST_URI.url) for arrow_ref in arrow_refs]
    ray.get(write_futures)
    logging.info("Data Written")


@ray.remote
def process_prefix_task(bucket_name: str, prefix: str) -> Union[None, tuple[str, str]]:
    """
    Ray Task to list all files in a given S3 bucket and prefix and process them with Ray Data

    Args:
        bucket_name (str): Name of the S3 bucket
        prefix (str): Prefix of the S3 bucket

    Returns:
        _ Union[None, tuple[str, str]]:
    """
    s3r = boto3.resource("s3")  # can't pass boto3 via Ray, so init within task.
    bucket = s3r.Bucket(bucket_name)
    _prefix_list: list[str] = [
        f"s3://{bucket.name}/{obj.key}"
        for obj in bucket.objects.filter(Prefix=prefix)
        if obj.size != 0
    ]
    if _prefix_list == []:
        logging.info(f"No objects found in s3://{bucket_name}/{prefix}")
        return None
    else:
        logging.info(f"Listing Sample from s3://{bucket_name}/{prefix}:")
        logging.info(f"Sample: {_prefix_list[0]}")
        logging.info(f"Begin Processing Files in: s3://{bucket_name}/{prefix}")
        raw_logs: Dataset = read_logs(_prefix_list)
        logging.info("Cleaning Data")
        cleaned: Dataset = raw_logs.map_batches(log_transforms_udf)
        write_to_parquet(cleaned)
        return (prefix, "HAD DATA")


if __name__ == "__main__":
    # Get Timestamp for checkpointing
    end_time: datetime = datetime.now(timezone.utc)
    start_time: datetime = checkpoint_from_s3(CHECKPOINT_URI.bucket, CHECKPOINT_URI.key)
    logging.info(f"Start Date: {start_time}; End Date: {end_time}")

    # Generate the list of prefixes
    prefix_list: list[str] = generate_prefix_list(LIST_BATCH_SIZE, start_time, end_time)
    logging.info(f"Prefix List Sample: {prefix_list[0]}")

    # Process the list of prefixes
    logging.info("Processing Files in prefixes")
    processed_prefix_refs: list[ObjectRef] = [
        process_prefix_task.remote(SOURCE_URI.bucket, prefix) for prefix in prefix_list
    ]  # type:ignore
    processed: list[tuple[str, str]] = [x for x in ray.get(processed_prefix_refs) if x is not None]
    if processed != []:
        checkpoint_to_s3(CHECKPOINT_URI.bucket, CHECKPOINT_URI.key, end_time)
        logging.info(f"Updated checkpoint at {CHECKPOINT_URI.url}")
    else:
        logging.info("No new objects to process. Not Updating Checkpoint.")

    logging.info("Done")
