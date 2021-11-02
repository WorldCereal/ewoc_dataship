import logging
from pathlib import Path

import boto3
import botocore

logger = logging.getLogger(__name__)


class EODataProvider:
    def __init__(self, s3_access_key_id=None,
                       s3_secret_access_key=None,
                       endpoint_url=None) -> None:
        if (s3_access_key_id is None and
           s3_secret_access_key is None and
           endpoint_url is None):
            self._s3_client = boto3.client('s3')
        else:
            self._s3_client = boto3.client('s3',
                aws_access_key_id=s3_access_key_id,
                aws_secret_access_key=s3_secret_access_key,
                endpoint_url=endpoint_url)

    def _download_prd(self, prd_prefix:str, out_dirpath:Path, bucket_name:str,
                      request_payer:bool=False)-> None:
        """ Download product from object storage

        Args:
            prd_prefix (str): prd key prefix
            out_dirpath (Path): directory where to write the objects of the product
            bucket_name (str): Name of the bucket
            request_payer (bool): requester activation
        """
        extra_args=None
        request_payer_arg=str()
        if request_payer is True:
            extra_args=dict(RequestPayer='requester')
            request_payer_arg = 'requester'

        logger.debug('Product prefix: %s', prd_prefix)
        response = self._s3_client.list_objects_v2(Bucket=bucket_name,
                                                   Prefix=prd_prefix,
                                                   RequestPayer=request_payer_arg)

        for obj in response['Contents']:
            logger.debug('obj.key: %s',obj['Key'])
            filename = obj['Key'].split(sep='/',
                                        maxsplit=len(prd_prefix.split('/'))-1)[-1]
            output_filepath = out_dirpath / filename
            (output_filepath.parent).mkdir(parents=True, exist_ok=True)
            logging.info('Try to download %s to %s', obj['Key'], output_filepath)
            self._s3_client.download_file(Bucket=bucket_name,
                                          Key=obj['Key'],
                                          Filename=str(output_filepath),
                                          ExtraArgs=extra_args)

    def _check_bucket(self, bucket_name):

        try:
            self._s3_client.head_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError as err:
            error_code = err.response['Error']['Code']
            if error_code == '404':
                logger.critical('Bucket %s does not exist!', bucket_name)
            elif error_code == '403':
                logger.critical('Acces forbidden to %s bucket!', bucket_name)
            return False

        return True
