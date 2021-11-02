import logging
import os
from pathlib import Path
from typing import List
import zipfile

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
            return

        super().__init__(s3_access_key_id=ewoc_access_key_id,
                         s3_secret_access_key=ewoc_secret_access_key_id,
                         endpoint_url=ewoc_endpoint_url)
        self._bucket_name = 'world-cereal' # ewoc_aux_data

        if not self._check_bucket(self._bucket_name):
            raise ValueError('EWoC data provider not correctly intialized!')

        logger.debug('EWoC data provider correctly initialized')

    def download_srtm_tiles(self, srtm_tile_ids:List[str], out_dirpath:Path,
                            resolution:str='1s')-> None:
        if resolution == '1s':
            srtm_prefix = 'srtm30/'
            srtm_suffix = '.SRTMGL1.hgt.zip'
        elif resolution == '3s':
            srtm_prefix = 'srtm90/'
            srtm_suffix = '.zip'
        else:
            logger.error('Resolution of SRTM tiles is 1s or 3s!')
            return
        
        for srtm_tile_id in srtm_tile_ids:
            srtm_tile_id_filename = srtm_tile_id + srtm_suffix
            srtm_tile_id_filepath = out_dirpath / srtm_tile_id_filename
            srtm_object_key = srtm_prefix + srtm_tile_id_filename
            logger.info('Try to download %s to %s', srtm_object_key, srtm_tile_id_filepath)
            self._s3_client.download_file(Bucket=self._bucket_name, 
                                          Key=srtm_object_key, 
                                          Filename=str(srtm_tile_id_filepath))

            with zipfile.ZipFile(srtm_tile_id_filepath, "r") as srtm_zipfile:
                srtm_zipfile.extractall(out_dirpath)

            srtm_tile_id_filepath.unlink()

if __name__ == "__main__":
    import sys
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=logging.INFO, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )
    ewoc_data_provider = EWOCDataProvider()
    ewoc_data_provider.download_srtm_tiles(['',''], Path('/tmp'))
    ewoc_data_provider.download_srtm_tiles(['',''], Path('/tmp'), resolution='3s')
