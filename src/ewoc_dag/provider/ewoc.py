import logging
import os
from pathlib import Path
from typing import List
import zipfile

import boto3

from ewoc_dag.provider.eodata import EODataProvider

logger = logging.getLogger(__name__)

class EWOCDataProvider(EODataProvider):
    # Currently the EWOC_ENDPOINT is hosted by Creodias
    _CREODIAS_EWOC_ENDPOINT_URL= 'https://s3.waw2-1.cloudferro.com'

    def __init__(self) -> None:

        ewoc_access_key_id = os.getenv('EWOC_S3_ACCESS_KEY_ID')
        ewoc_secret_access_key_id = os.getenv('EWOC_S3_SECRET_ACCESS_KEY')

        ewoc_endpoint_url = os.getenv('EWOC_ENDPOINT_URL', 
                                     self._CREODIAS_EWOC_ENDPOINT_URL)
        logging.debug('EWoC endpoint URL: %s', ewoc_endpoint_url)

        if ewoc_access_key_id is None or ewoc_secret_access_key_id is None:
            logging.critical('S3 resource credentials not provided for EWoC object storage!')
            return None

        self._s3_client = boto3.client('s3',
                            aws_access_key_id=ewoc_access_key_id,
                            aws_secret_access_key=ewoc_secret_access_key_id,
                            endpoint_url=ewoc_endpoint_url)
        self.bucket_name = 'world-cereal' # ewoc_aux_data

    def donwload_srtm_1s(self, srtm_tile_ids:List[str],  out_dirpath:Path)-> None:

        srtm_prefix = 'srtm30/'
        for srtm_tile_id in srtm_tile_ids:
            srtm_tile_id_filename = srtm_tile_id + ".SRTMGL1.hgt.zip"
            srtm_tile_id_filepath = out_dirpath / srtm_tile_id_filename
            srtm_object_key = srtm_prefix + srtm_tile_id_filename
            logger.info(srtm_object_key)
            self._s3_client.download_file(Bucket=self.bucket_name, 
                                          Key=srtm_object_key, 
                                          Filename=str(srtm_tile_id_filepath))

            with zipfile.ZipFile(srtm_tile_id_filepath, "r") as srtm_zipfile:
                srtm_zipfile.extractall(out_dirpath)

            srtm_tile_id_filepath.unlink()

    def donwload_srtm_3s(self, srtm_tile_ids,  out_dirpath)-> None:

        srtm_prefix = 'srtm90/'
        for srtm_tile_id in srtm_tile_ids:
            srtm_tile_id_filename = srtm_tile_id + ".zip"
            srtm_tile_id_filepath = out_dirpath / srtm_tile_id_filename
            srtm_object_key = srtm_prefix + srtm_tile_id_filename
            logger.info(srtm_object_key)
            self._s3_client.download_file(Bucket=self.bucket_name, 
                                          Key=srtm_object_key, 
                                          Filename=str(srtm_tile_id_filepath))

            with zipfile.ZipFile(srtm_tile_id_filepath, "r") as srtm_zipfile:
                srtm_zipfile.extractall(out_dirpath)

            srtm_tile_id_filepath.unlink()
