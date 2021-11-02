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


def create_s3_resource(s3_resource_name):
    """ Create s3 resource from boto3 for supported object storage

    We suport AWS, creodias eodata and ewoc object storage.

    The following env variables are needed:
      - for ewoc:
        - EWOC_S3_ACCESS_KEY_ID
        - EWOC_S3_SECRET_ACCESS_KEY
        - EWOC_ENDPOINT_URL
      - for aws
        - AWS_ACCESS_KEY_ID
        - AWS_SECRET_ACCESS_KEY

    For more information:
     - for creodias eodata case: https://creodias.eu/faq-s3/-/asset_publisher/SIs09LQL6Gct/content/how-to-download-a-eo-data-file-using-boto3-?inheritRedirect=true
     - for creodias ewo case: https://creodias.eu/-/how-to-access-private-object-storage-using-s3cmd-or-boto3-?inheritRedirect=true&redirect=%2Ffaq-s3

    Args:
      s3 resource name: str
        Resource name supported: aws, creodias_eodata, ewoc.

    Returns:
      A boto3 Subclass of ServiceResource

    Raises:
      ValueError: When the resource name is not provided or the env variable not set for ewoc case.
      NotImplementedError: When the resource name is not supported
    """
    if s3_resource_name is None:
        logging.critical('S3 ressource name not provided!')
        raise ValueError
    elif s3_resource_name == 'aws':
        return boto3.resource('s3')
    elif s3_resource_name == 'creodias_eodata':
        return boto3.resource('s3',
                              aws_access_key_id=str(None),
                              aws_secret_access_key=str(None),
                              endpoint_url='http://data.cloudferro.com')
    elif s3_resource_name == 'ewoc':
        ewoc_access_key_id = os.getenv('EWOC_S3_ACCESS_KEY_ID')
        ewoc_secret_access_key_id = os.getenv('EWOC_S3_SECRET_ACCESS_KEY')
        CREODIAS_EWOC_ENDPOINT_URL= 'https://s3.waw2-1.cloudferro.com'
        ewoc_endpoint_url = os.getenv('EWOC_ENDPOINT_URL', CREODIAS_EWOC_ENDPOINT_URL)
        logging.debug('EWoC endpoint URL: %s', ewoc_endpoint_url)

        if ewoc_access_key_id is None or ewoc_secret_access_key_id is None:
            logging.critical('S3 resource credentials not provided for EWoC object storage!')
            raise ValueError
        return boto3.resource('s3',
                              aws_access_key_id=ewoc_access_key_id,
                              aws_secret_access_key=ewoc_secret_access_key_id,
                              endpoint_url=ewoc_endpoint_url)
    else:
        logging.critical('S3 resource %s not supported', s3_resource_name)
        raise NotImplementedError


def download_object(bucket, object_name: str, filepath: Path, request_payer: bool=False):
    """ Download a object from a bucket

    Args:
        bucket (boto3 bucket): bucket object creates with boto3
        object_name (str): key in the object storage of the object
        filepath (Path): Filepath where the object will be writen
        request_payer (bool, optional): [description]. Defaults to False.
    """
    extra_args=None
    if request_payer is True:
        extra_args=dict(RequestPayer='requester')

    try:
        bucket.download_file(object_name, filepath, ExtraArgs=extra_args)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            logger.error("The object does not exist.")
        else:
            raise


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


def upload_objects(bucket, dirpath: Path, object_prefix:str, file_suffix :str='.tif'):
    """ Upload a set of objects from a directory to a bucket

    Args:
        bucket (boto3 bucket): bucket object creates with boto3
        dirpath (Path): Directory which contains the files to upload
        object_prefix (str): where to put the objects
        file_suffix (str, optional): extension use to filter the files in the directory. Defaults to '.tif'.

    Returns:
        [type]: [description]
    """
    filepaths = sorted(dirpath.glob(file_suffix))
    upload_object_size = 0
    for filepath in filepaths:
        upload_object_size += filepath.stat().st_size
        object_name = object_prefix + '/' + filepath
        upload_object(bucket, filepath, object_name)

    logging.info('Uploaded %s tif files for a total size of %s.', len(filepaths),
                                                                  upload_object_size)

    return len(filepaths), upload_object_size


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


