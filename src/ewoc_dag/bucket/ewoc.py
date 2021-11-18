from distutils.util import strtobool
import logging
import os
from pathlib import Path
from tempfile import gettempdir
from typing import List
import zipfile

from ewoc_dag.bucket.eobucket import EOBucket

logger = logging.getLogger(__name__)

class EWOCBucket(EOBucket):

    _CREODIAS_EWOC_ENDPOINT_URL= 'https://s3.waw2-1.cloudferro.com'

    def __init__(self, bucket_name)->None:

        ewoc_access_key_id = os.getenv('EWOC_S3_ACCESS_KEY_ID')
        ewoc_secret_access_key_id = os.getenv('EWOC_S3_SECRET_ACCESS_KEY')

        ewoc_cloud_provider = os.getenv('EWOC_CLOUD_PROVIDER', 'CREODIAS')
        if ewoc_cloud_provider == 'CREODIAS':
            ewoc_endpoint_url = self._CREODIAS_EWOC_ENDPOINT_URL
        elif ewoc_cloud_provider == 'AWS':
            ewoc_endpoint_url=None
        else:
            raise ValueError(f'Cloud provider {ewoc_cloud_provider} not supported!')

        super().__init__(bucket_name,
                         s3_access_key_id=ewoc_access_key_id,
                         s3_secret_access_key=ewoc_secret_access_key_id,
                         endpoint_url=ewoc_endpoint_url)

        if not self._check_bucket():
            raise ValueError(f'EWoC {bucket_name} not correctly intialized!')

        logger.info('EWoC bucket %s is hosted on %s and functional',
            bucket_name,
            ewoc_cloud_provider)

class EWOCAuxDataBucket(EWOCBucket):

    def __init__(self) -> None:
        super().__init__('ewoc-aux-data')

    def download_srtm1s_tiles(self, srtm_tile_ids:List[str],
                              out_dirpath:Path=Path(gettempdir()))-> None:

        for srtm_tile_id in srtm_tile_ids:
            srtm_tile_id_filename = srtm_tile_id + '.zip'
            srtm_tile_id_filepath = out_dirpath / srtm_tile_id_filename
            srtm_object_key = 'srtm90/' + srtm_tile_id_filename
            logger.info('Try to download %s to %s', srtm_object_key, srtm_tile_id_filepath)
            self._s3_client.download_file(Bucket=self._bucket_name,
                                          Key=srtm_object_key,
                                          Filename=str(srtm_tile_id_filepath))

            with zipfile.ZipFile(srtm_tile_id_filepath, "r") as srtm_zipfile:
                srtm_zipfile.extractall(out_dirpath/'srtm1s')

            srtm_tile_id_filepath.unlink()

    def list_copdem20m_tiles(self):
        pass

    def list_agera5_prd(self):
        pass

    def upload_agera5_prd(self):
        pass

class EWOCARDBucket(EWOCBucket):

    def __init__(self, ewoc_dev_mode=None) -> None:
        if ewoc_dev_mode is None:
            ewoc_dev_mode = strtobool(os.getenv('EWOC_DEV_MODE', 'False'))
        if  not ewoc_dev_mode:
            super().__init__('ewoc-ard')
        elif ewoc_dev_mode:
            super().__init__('ewoc-ard-dev')

    def _list_ard_prds(self):
        pass

    def list_ard_sar_prd(self):
        self._list_ard_prds()

    def list_ard_optical_prd(self):
        self._list_ard_prds()

    def list_ard_tir_prd(self):
        self._list_ard_prds()

    def _upload_ard_prd(self):
        super()._upload_prd(Path('todo'), 'TODO')

    def upload_ard_s1_prd(self):
        self._upload_ard_prd()

    def upload_ard_s2_prd(self):
        self._upload_ard_prd()

    def upload_ard_tir_prd(self):
        self._upload_ard_prd()


class EWOCPRDBucket(EWOCBucket):

    def __init__(self, ewoc_dev_mode=None) -> None:
        if ewoc_dev_mode is None:
            ewoc_dev_mode = strtobool(os.getenv('EWOC_DEV_MODE', 'False'))
        if  not ewoc_dev_mode:
            super().__init__('ewoc-prd')
        elif ewoc_dev_mode:
            super().__init__('ewoc-prd-dev')

    def upload_ewoc_prd(self):
        super()._upload_prd(Path('todo'), 'TODO')


if __name__ == "__main__":
    import sys
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=logging.INFO, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )
    ewoc_auxdata_bucket = EWOCAuxDataBucket()
    ewoc_auxdata_bucket.download_srtm1s_tiles(['srtm_01_16','srtm_01_21'])

    # TODO: to be replaced by test of public method
    ewoc_ard_bucket = EWOCARDBucket(ewoc_dev_mode=True)
    logger.info(ewoc_ard_bucket._upload_file(Path('/tmp/upload.file'),'test.file'))

    ewoc_ard_bucket._upload_prd(Path('/tmp/upload_test_dir'),'test_up_dir')

    ewoc_ard_bucket._upload_prd(Path('/tmp/upload_test_dir'),'test_up_dir', file_suffix=None)
