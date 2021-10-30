import logging
from pathlib import Path
from typing import List

from eotile.eotile_module import main

from ewoc_dag.provider.creodias import download_copdem_tiles_from_creodias
from ewoc_dag.provider.aws import download_copdem_tiles_from_aws


logger = logging.getLogger(__name__)

class EODataProvider:
    def __init__(self) -> None:
        pass

class EWOCDataProvider(EODataProvider):
    def __init__(self) -> None:
        import os
        import boto3
        ewoc_access_key_id = os.getenv('EWOC_S3_ACCESS_KEY_ID')
        ewoc_secret_access_key_id = os.getenv('EWOC_S3_SECRET_ACCESS_KEY')
        CREODIAS_EWOC_ENDPOINT_URL= 'https://s3.waw2-1.cloudferro.com'
        ewoc_endpoint_url = os.getenv('EWOC_ENDPOINT_URL', CREODIAS_EWOC_ENDPOINT_URL)
        logging.debug('EWoC endpoint URL: %s', ewoc_endpoint_url)

        if ewoc_access_key_id is None or ewoc_secret_access_key_id is None:
            logging.critical('S3 resource credentials not provided for EWoC object storage!')
            return None

        self._s3_client = boto3.client('s3',
                            aws_access_key_id=ewoc_access_key_id,
                            aws_secret_access_key=ewoc_secret_access_key_id,
                            endpoint_url=ewoc_endpoint_url)
        self.bucket_name = 'world-cereal'

    def donwload_srtm_1s(self, srtm_tile_ids,  out_dirpath)-> None:

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


class CREODIASDataProvider:
    def __init__(self) -> None:
        pass

    def download_s1_prd(self, prd_id, out_dirpath, finder = False)-> None:
        pass

    def download_s2_prd(self, prd_id, out_dirpath, l2_mask_only=False, finder = False)-> None:
        # detect level from prd_id
        pass

    def download_copdem_tiles(self, copdem_tiles_id, out_dirpath, resolution=30)-> None:
        pass

class AWSDataProvider:
    def __init__(self) -> None:
        pass

    def download_s1_prd(self, prd_id, out_dirpath)-> None:
        pass

    def download_s2_prd(self, prd_id, out_dirpath, l2_mask_only=False, l2_cogs=False)-> None:
        # detect level from prd_id
        pass

    def download_copdem_tiles(self, copdem_tiles_id, out_dirpath, resolution=30)-> None:
        pass