from datetime import date
import logging
import os
from pathlib import Path

import boto3
import botocore
from botocore.exceptions import ClientError

from dataship.dag.s1_prd_id import S1PrdIdInfo
from dataship.dag.s2_prd_id import S2PrdIdInfo

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
    """ Get a s3 ressources from boto3.

    Retrieves rows pertaining to the given keys from the Table instance
    represented by table_handle.  String keys will be UTF-8 encoded.

    Args:
      table_handle:
        An open smalltable.Table instance.
      keys:
        A sequence of strings representing the key of each table row to
        fetch.  String keys will be UTF-8 encoded.
      require_all_keys:
        Optional; If require_all_keys is True only rows with values set
        for all keys will be returned.

    Returns:
      A dict mapping keys to the corresponding table row data
      fetched. Each row is represented as a tuple of strings. For
      example:

      {b'Serak': ('Rigel VII', 'Preparer'),
       b'Zim': ('Irk', 'Invader'),
       b'Lrrr': ('Omicron Persei 8', 'Emperor')}

      Returned keys are always bytes.  If a key from the keys argument is
      missing from the dictionary, then that row was not found in the
      table (and require_all_keys must have been False).

    Raises:
      IOError: An error occurred accessing the smalltable.
    """
    if s3_resource_name is None:
        logging.critical('S3 ressource name not provided!')
        raise ValueError
    elif s3_resource_name == 'aws':
        return boto3.resource('s3')
    elif s3_resource_name == 'creodias_eodata':
        # cf. https://creodias.eu/faq-s3/-/asset_publisher/SIs09LQL6Gct/content/how-to-download-a-eo-data-file-using-boto3-?inheritRedirect=true
        return boto3.resource('s3',
                              aws_access_key_id=str(None),
                              aws_secret_access_key=str(None),
                              endpoint_url='http://data.cloudferro.com')
    elif s3_resource_name == 'ewoc':
        ewoc_access_key_id = os.getenv('EWOC_S3_ACCESS_KEY_ID')
        ewoc_secret_access_key_id = os.getenv('EWOC_S3_SECRET_ACCESS_KEY')
        # cf. https://creodias.eu/-/how-to-access-private-object-storage-using-s3cmd-or-boto3-?inheritRedirect=true&redirect=%2Ffaq-s3
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

def download_object(s3_resource, bucket_name, object_name, filepath, request_payer=False):
    extra_args=None
    if request_payer==True:
        extra_args=dict(RequestPayer='requester')

    try:
        s3_resource.Bucket(bucket_name).download(object_name, filepath, ExtraArgs=extra_args)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            logger.error("The object does not exist.")
        else:
            raise

def upload_object(s3_resource, bucket_name, filepath, object_name):
    try:
        s3_resource.Bucket(bucket_name).upload_file(filepath, object_name)
        logging.info('Uploaded %s (%s) to %s', filepath, filepath.stat().st_size, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def upload_objects(s3_ressource, bucket_name, dirpath, object_prefix, file_suffix='.tif'):
    filepaths=sorted(dirpath.glob(file_suffix))
    upload_object_size= 0
    for filepath in filepaths:
        upload_object_size += filepath.stat().st_size
        object_name = object_prefix + '/' + filepath
        upload_object(s3_ressource, bucket_name, filepath, object_name)

    logging.info('Uploaded %s tif files for a total size of %s.', len(filepaths), upload_object_size)

    return len(filepaths), upload_object_size

def upload_file(s3_client, local_file, bucket, s3_obj):
    try:
        s3_client.upload_file(local_file, bucket, s3_obj)
        print("Sent {} to s3://{}/{}".format(local_file, bucket, s3_obj))
    except ClientError:
        print("Failed to upload file {} to s3://{}/{}".format(local_file, bucket, s3_obj))
        return False
    return True

def download_prd_from_creodias(prd_prefix, out_dirpath):
    s3_resource = create_s3_resource('creodias_eodata')
    bucket_name = 'DIAS'
    bucket = s3_resource.Bucket(bucket_name)
    logger.debug('Product prefix: %s', prd_prefix)
    for obj in bucket.objects.filter(Prefix=prd_prefix):
        if obj.key[-1]== '/':
            dirname = obj.key.split(sep='/', maxsplit=6)[-1]
            output_dir = out_dirpath /  dirname
            output_dir.mkdir(parents=True, exist_ok=True)
            continue
        filename =  obj.key.split(sep='/', maxsplit=6)[-1]
        output_filepath = out_dirpath / filename
        logging.debug('Try to download %s to %s', obj.key, output_filepath)
        download_object(s3_resource, bucket_name, obj.key, str(output_filepath))

CREODIAS_BUCKET_FORMAT_PREFIX='/%Y/%m/%d/'

def download_s1_prd_from_creodias(prd_id, out_dirpath):
    s1_prd_info = S1PrdIdInfo(prd_id)
    s1_bucket_prefix='Sentinel-1/SAR/'
    prd_prefix = s1_bucket_prefix + s1_prd_info.product_type + '/' \
                 + s1_prd_info.start_time.date().strftime(CREODIAS_BUCKET_FORMAT_PREFIX) \
                 + prd_id + '/'
    download_prd_from_creodias(prd_prefix, out_dirpath)

def download_s2_prd_from_creodias(prd_id, out_dirpath):
    s2_prd_info = S2PrdIdInfo(prd_id)
    s2_bucket_prefix='Sentinel-2/MSI/'
    prd_prefix = s2_bucket_prefix + s2_prd_info.product_level + '/' \
                 + s2_prd_info.datatake_sensing_start_time.date().strftime(CREODIAS_BUCKET_FORMAT_PREFIX) \
                 + prd_id + '/'
    download_prd_from_creodias(prd_prefix, out_dirpath)


def recursive_upload_dir_to_s3(s3_client, local_path, s3_path, bucketname):
    tif_files_number = 0
    total_output_size = 0
    for (root, dir_names, filenames) in os.walk(local_path):
        for file in filenames:
            old_file = os.path.join(root, file)
            if os.path.isfile(old_file):
                if file.endswith('.tif'):
                    tif_files_number += 1
                new_file = os.path.join(s3_path, root.replace(local_path,''),file)
                total_output_size = total_output_size + os.path.getsize(old_file)
                upload_file(s3_client, old_file, bucketname, new_file)
    print(f'\n Uploaded {tif_files_number} tif files for a total size of {total_output_size}')
    return tif_files_number, total_output_size


def download_s3file(s3_full_key,out_file, bucket):
    """
    Download file from s3 object storage
    :param s3_full_key: Object full path (prefix, and key)
    :param out_file: Full path and name of the output file
    :param bucket: Bucket name
    """
    s3_client = get_s3_client()
    s3_client.download_file(Bucket=bucket, Key=s3_full_key, Filename=out_file, ExtraArgs=dict(RequestPayer='requester'))


if __name__ == "__main__":
    download_s2_prd_from_creodias('S2B_MSIL1C_20210714T235249_N0301_R130_T57KUR_20210715T005654.SAFE', Path('/tmp'))
    download_s2_prd_from_creodias('S2B_MSIL2A_20210714T131719_N0301_R124_T28WDB_20210714T160455.SAFE', Path('/tmp'))
    download_s1_prd_from_creodias('S1B_IW_GRDH_1SSH_20210714T083244_20210714T083309_027787_0350EB_E62C.SAFE', Path('/tmp'))
