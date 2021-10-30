import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class EODataProvider:
    def __init__(self) -> None:
        pass


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
