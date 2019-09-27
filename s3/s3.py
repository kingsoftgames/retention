#!/usr/bin/env python3
import logging
import os
import boto3
from util import util

logger = logging.getLogger()

AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")


def init_bucket_from_env():
    if util.is_empty(AWS_REGION) or util.is_empty(S3_BUCKET):
        logger.error(f'Params error. AWS_REGION or S3_BUCKET is empty')
        raise RuntimeError()

    aws_access_key_id_name = "AWS_ACCESS_KEY_ID"
    aws_secret_access_key_name = "AWS_SECRET_ACCESS_KEY"
    aws_access_key_id = os.getenv(aws_access_key_id_name)
    aws_secret_access_key = os.getenv(aws_secret_access_key_name)
    if os.environ.get(aws_access_key_id_name) is not None:
        if util.is_empty(aws_access_key_id):
            os.environ.pop("AWS_ACCESS_KEY_ID")
    if os.environ.get(aws_secret_access_key_name) is not None:
        if util.is_empty(aws_secret_access_key):
            os.environ.pop("AWS_SECRET_ACCESS_KEY")
    return boto3.resource("s3", AWS_REGION).Bucket(S3_BUCKET)
