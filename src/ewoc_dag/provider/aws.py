import logging
from pathlib import Path
from typing import List

import boto3

from ewoc_dag.provider.eodata import EODataProvider
from ewoc_dag.eo_prd_id.l8_prd_id import L8C2PrdIdInfo
from ewoc_dag.eo_prd_id.s1_prd_id import S1PrdIdInfo
from ewoc_dag.eo_prd_id.s2_prd_id import S2PrdIdInfo


logger = logging.getLogger(__name__)


class AWSDataProvider(EODataProvider):
    def __init__(self) -> None:
        super().__init__()
        self._s3_client = boto3.client('s3')

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

    def download_l8_c2_prd(self, prd_id:str, out_dirpath_root:Path):
        out_dirpath = out_dirpath_root / prd_id.split('.')[0]
        out_dirpath.mkdir(exist_ok=True)

        l8_prd_info = L8C2PrdIdInfo(prd_id)

        prd_prefix = '/'.join(['collection02', 'level-2',
                               'standard', 'oli-tirs',
                               str(l8_prd_info.acquisition_date.year),
                               l8_prd_info.wrs2_path,
                               l8_prd_info.wrs2_row,
                               prd_id]) + '/'
        logger.debug('prd_prefix: %s', prd_prefix)
        self._download_prd(prd_prefix, out_dirpath, "usgs-landsat",
                           request_payer=True)


    def download_s1_prd(self, prd_id:str, out_dirpath_root:Path):
        out_dirpath = out_dirpath_root / prd_id.split('.')[0]
        out_dirpath.mkdir(exist_ok=True)

        s1_prd_info = S1PrdIdInfo(prd_id)

        prd_prefix = '/'.join([s1_prd_info.product_type,
                               str(s1_prd_info.start_time.date().year),
                               str(s1_prd_info.start_time.date().month),
                               str(s1_prd_info.start_time.date().day),
                               s1_prd_info.beam_mode,
                               s1_prd_info.polarisation,
                               prd_id.split('.')[0]]) + '/'
        logger.debug('prd_prefix: %s', prd_prefix)
        self._download_prd(prd_prefix, out_dirpath, "sentinel-s1-l1c", request_payer=True)

    def download_s2_prd(self, prd_id:str, out_dirpath_root:Path,
                        l2_mask_only:bool=False,
                        l2a_cogs:bool=False) -> None:
        out_dirpath = out_dirpath_root / prd_id.split('.')[0]
        out_dirpath.mkdir(exist_ok=True)

        s2_prd_info = S2PrdIdInfo(prd_id)
        prefix_components=[s2_prd_info.tile_id[0:2],
                            s2_prd_info.tile_id[2],
                            s2_prd_info.tile_id[3:5],
                            str(s2_prd_info.datatake_sensing_start_time.date().year),
                            str(s2_prd_info.datatake_sensing_start_time.date().month)]
        if l2a_cogs:
            prefix_components.insert(0,'sentinel-s2-l2a-cogs')
            product_name = '_'.join([s2_prd_info.mission_id,
                                    s2_prd_info.tile_id,
                                    s2_prd_info.datatake_sensing_start_time.date().strftime('%Y%m%d'),
                                    '0',
                                    'L2A'])
            (out_dirpath/product_name).mkdir(exist_ok=True)
            prefix_components.append(product_name)
            prd_prefix = '/'.join(prefix_components) + '/'
            logger.debug('prd_prefix: %s', prd_prefix)

            bucket_name = "sentinel-cogs"
            if l2_mask_only:
                mask_filename= 'SCL.tif'
                logging.info('Try to download %s to %s', prd_prefix + mask_filename, out_dirpath / mask_filename)
                self._s3_client.download_file(Bucket=bucket_name,
                                                Key=prd_prefix + mask_filename,
                                                Filename=str(out_dirpath / mask_filename))
            else:
                self._download_prd(prd_prefix, out_dirpath, bucket_name )
        else:
            prd_prefix = '/'.join(['products',
                                    str(s2_prd_info.datatake_sensing_start_time.date().year),
                                    str(s2_prd_info.datatake_sensing_start_time.date().month),
                                    str(s2_prd_info.datatake_sensing_start_time.date().day),
                                    prd_id.split('.')[0]]) + '/'
            logger.info('prd_prefix: %s', prd_prefix)

            prefix_components.insert(0,'tiles')
            prefix_components.append(str(s2_prd_info.datatake_sensing_start_time.date().day))
            prefix_components.append('0')
            tile_prefix = '/'.join(prefix_components) + '/'
            logger.info('tile_prefix: %s', tile_prefix)

            if s2_prd_info.product_level == 'L2A':
                bucket_name = "sentinel-s2-l2a"

                if l2_mask_only:
                    mask_filename='SCL.jp2'
                    logging.info('Try to download %s to %s', prd_prefix + mask_filename, out_dirpath / mask_filename)
                    self._s3_client.download_file(Bucket=bucket_name,
                                            Key=tile_prefix + 'R20m/' + mask_filename,
                                            Filename=str(out_dirpath / mask_filename),
                                            ExtraArgs=dict(RequestPayer='requester'))
                else:
                    self._download_prd(prd_prefix, out_dirpath, bucket_name, request_payer=True)
                    self._download_prd(tile_prefix, out_dirpath, bucket_name, request_payer=True)
            else:
                bucket_name = "sentinel-s2-l1c"
                self._download_prd(prd_prefix, out_dirpath, bucket_name, request_payer=True)
                self._download_prd(tile_prefix, out_dirpath, bucket_name, request_payer=True)


    def download_copdem_tiles(self, copdem_tile_ids:List[str], out_dirpath:Path,
                              resolution:int=30):
        if resolution == 30:
            bucket_name = 'copernicus-dem-30m'
        elif resolution == 90:
            bucket_name = 'copernicus-dem-90m'
        else:
            logger.error('todo')
            return None

        for copdem_tile_id in copdem_tile_ids:
            copdem_tile_id_filename = copdem_tile_id + '.tif'
            copdem_tile_id_filepath = out_dirpath / copdem_tile_id_filename
            copdem_object_key = copdem_tile_id + '/' + copdem_tile_id_filename
            logging.info('Try to download %s to %s', copdem_object_key, copdem_tile_id_filename)
            self._s3_client.download_file(Bucket=bucket_name,
                                          Key=copdem_object_key,
                                          Filename=str(copdem_tile_id_filepath))

if __name__ == "__main__":
    import sys
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=logging.INFO, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )
    aws_data_provider = AWSDataProvider()
    # TODO: add the possibility to filter the file downloaded: full or subset
    # aws_data_provider.download_s2_prd('S2B_MSIL1C_20210714T235249_N0301_R130_T57KUR_20210715T005654.SAFE',
    #                                    Path('/tmp'))
    # aws_data_provider.download_s2_prd('S2B_MSIL2A_20210714T131719_N0301_R124_T28WDB_20210714T160455.SAFE',
    #                               Path('/tmp'))
    # aws_data_provider.download_s2_prd('S2B_MSIL2A_20210714T131719_N0301_R124_T28WDB_20210714T160455.SAFE',
    #                              Path('/tmp'), l2a_cogs=True)
    #aws_data_provider.download_s2_prd('S2B_MSIL2A_20210714T131719_N0301_R124_T28WDB_20210714T160455.SAFE',
    #                              Path('/tmp'), l2a_cogs=True, l2_mask_only=True)
    # aws_data_provider.download_s2_prd('S2B_MSIL2A_20210714T131719_N0301_R124_T28WDB_20210714T160455.SAFE',
    #                              Path('/tmp'), l2_mask_only=True)
    # aws_data_provider.download_s1_prd('S1B_IW_GRDH_1SSH_20210714T083244_20210714T083309_027787_0350EB_E62C.SAFE',
    # #                              Path('/tmp'))
    # aws_data_provider.download_copdem_tiles(['Copernicus_DSM_COG_10_S90_00_W157_00_DEM', 'Copernicus_DSM_COG_10_S90_00_W156_00_DEM'],
    #                               Path('/tmp'))
    aws_data_provider.download_l8_c2_prd('LC08_L2SP_227099_20211017_20211026_02_T2',
                                        Path('/tmp'))
