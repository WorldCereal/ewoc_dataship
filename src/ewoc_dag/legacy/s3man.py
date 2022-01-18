import logging
import os
from pathlib import Path

import boto3
import botocore
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


# Some s3 functions from argo workflow coded by Alex G.
def get_s3_client():
    client_config = botocore.config.Config(max_pool_connections=100)
    s3_client = None
    if "amazon" in os.environ["S3_ENDPOINT"]:
        s3_client = boto3.client('s3',
                                 aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
                                 aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
                                 region_name="eu-central-1",
                                 config=client_config)
    if "cloudferro" in os.environ["S3_ENDPOINT"]:
        s3_client = boto3.client('s3',
                                 aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
                                 aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
                                 endpoint_url=os.environ["S3_ENDPOINT"],
                                 config=client_config)

    return s3_client



def upload_object(bucket, filepath: Path, object_name: str)-> bool:
    """ Upload a object to a bucket

    Args:
        bucket (boto3 bucket): bucket object creates with boto3
        filepath (Path): Filepath of the object to write
        object_name (str): key in the object storage of the object

    Returns:
        [bool]: if upload succeed
    """
    try:
        bucket.upload_file(filepath, object_name)
        logging.info('Uploaded %s (%s) to %s', filepath, filepath.stat().st_size, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_file(s3_client, local_file, bucket, s3_obj):
    try:
        s3_client.upload_file(local_file, bucket, s3_obj)
        print("Sent {} to s3://{}/{}".format(local_file, bucket, s3_obj))
    except ClientError:
        print("Failed to upload file {} to s3://{}/{}".format(local_file, bucket, s3_obj))
        return False
    return True


def recursive_upload_dir_to_s3(s3_client, local_path, s3_path, bucketname):
    tif_files_number = 0
    total_output_size = 0
    paths = []
    for (root, dir_names, filenames) in os.walk(local_path):
        for file in filenames:
            old_file = os.path.join(root, file)
            if os.path.isfile(old_file):
                if file.endswith('.tif'):
                    tif_files_number += 1
                new_file = os.path.join(s3_path, root.replace(local_path, ''), file)
                total_output_size = total_output_size + os.path.getsize(old_file)
                upload_file(s3_client, old_file, bucketname, new_file)
                if os.path.dirname(new_file) not in paths:
                    paths.append(os.path.dirname(new_file))
    if len(paths) == 1:
        print(f'\n Uploaded {tif_files_number} tif files to bucket | s3://{bucketname}/{paths[0]}')
    else:
        print("Error, incorrect number of directories : ")
        print(f'\n Uploaded {tif_files_number} tif files to bucket | s3://{bucketname}/{f" ; s3://{bucketname}/".join(paths)}')
    return tif_files_number, total_output_size


def download_s3file(s3_full_key,out_file, bucket):
    """
    Download file from s3 object storage
    :param s3_full_key: Object full path (prefix, and key)
    :param out_file: Full path and name of the output file
    :param bucket: Bucket name
    """
    s3_client = get_s3_client()
    s3_client.download_file(Bucket=bucket, Key=s3_full_key, Filename=out_file,
                            ExtraArgs=dict(RequestPayer='requester'))
