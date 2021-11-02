import logging
from pathlib import Path
import zipfile

from ewoc_dag.eo_prd_id.s1_prd_id import S1PrdIdInfo
from ewoc_dag.eo_prd_id.s2_prd_id import S2PrdIdInfo
from ewoc_dag.provider.eodata import EODataProvider

logger = logging.getLogger(__name__)

class CREODIASDataProvider(EODataProvider):
    _CREODIAS_BUCKET_FORMAT_PREFIX='/%Y/%m/%d/'

    def __init__(self) -> None:
        super().__init__(s3_access_key_id='anystring',
                         s3_secret_access_key='anystring',
                         endpoint_url='http://data.cloudferro.com')
        self._bucket_name = 'DIAS'

        if not self._check_bucket(self._bucket_name):
            raise ValueError('Creodias data provider not correctly intialized!')

        logger.debug('Creodias data provider correctly initialized')

    def download_s1_prd(self, prd_id:str, out_dirpath:Path) -> None:
        """ Download Sentinel-1 product from creodias eodata object storage

        Args:
            prd_id (str): Sentinel-1 product id
            out_dirpath (Path): Directory where to put the product
        """
        s1_prd_info = S1PrdIdInfo(prd_id)
        s1_bucket_prefix='Sentinel-1/SAR/'
        prd_prefix = s1_bucket_prefix + s1_prd_info.product_type \
                    + s1_prd_info.start_time.date().strftime(self._CREODIAS_BUCKET_FORMAT_PREFIX) \
                    + prd_id + '/'
        self._download_prd(prd_prefix, out_dirpath, self._bucket_name)


    def download_s2_prd(self, prd_id:str, out_dirpath:Path,
                        l2_mask_only:bool=False) -> None:
        """ Download Sentinel-2 product from creodias eodata object storage

        Args:
            prd_id (str): Sentinel-2 product id
            out_dirpath (Path): Directory where to put the product
        """
        s2_prd_info = S2PrdIdInfo(prd_id)
        s2_bucket_prefix='Sentinel-2/MSI/'
        prd_prefix = s2_bucket_prefix + s2_prd_info.product_level \
                    + s2_prd_info.datatake_sensing_start_time.date().strftime(self._CREODIAS_BUCKET_FORMAT_PREFIX) \
                    + prd_id + '/'
        if not l2_mask_only:
            self._download_prd(prd_prefix, out_dirpath, self._bucket_name)
        else:
            if s2_prd_info.product_level == "L2A":
                mask_key = prd_prefix + 'path/to/mask'
                mask_filepath = out_dirpath / 'mask.tif'
                self._s3_client.download_file(Bucket=self._bucket_name,
                                            Key=mask_key,
                                            Filename=str(mask_filepath))
            else:
                logger.warning('Not possible!')

    def download_srtm1s_tiles(self, srtm_tile_ids, out_dirpath):

        srtm_prefix = 'auxdata/SRTMGL1/dem/'
        for srtm_tile_id in srtm_tile_ids:
            srtm_tile_id_filename = srtm_tile_id + ".SRTMGL1.hgt.zip"
            srtm_tile_id_filepath = out_dirpath / srtm_tile_id_filename
            srtm_object_key = srtm_prefix + srtm_tile_id_filename
            logger.info(srtm_object_key)
            self._s3_client.download_file(Bucket=self._bucket_name,
                                          Key=srtm_object_key,
                                          Filename=str(srtm_tile_id_filepath))

            with zipfile.ZipFile(srtm_tile_id_filepath, "r") as srtm_zipfile:
                srtm_zipfile.extractall(out_dirpath)

            srtm_tile_id_filepath.unlink()

    def download_copdem_tiles(self, copdem_tiles_id, out_dirpath, resolution=30)-> None:
        pass

if __name__ == "__main__":
    creo_data_provider = CREODIASDataProvider()
    creo_data_provider.download_s2_prd('S2B_MSIL1C_20210714T235249_N0301_R130_T57KUR_20210715T005654.SAFE',
                                        Path('/tmp'))
    creo_data_provider.download_s2_prd('S2B_MSIL2A_20210714T131719_N0301_R124_T28WDB_20210714T160455.SAFE',
                                  Path('/tmp'))
    creo_data_provider.download_s2_prd('S2B_MSIL2A_20210714T131719_N0301_R124_T28WDB_20210714T160455.SAFE',
                                  Path('/tmp'), l2_mask_only=True)
    creo_data_provider.download_s1_prd('S1B_IW_GRDH_1SSH_20210714T083244_20210714T083309_027787_0350EB_E62C.SAFE',
                                  Path('/tmp'))
