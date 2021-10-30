import logging
import os
from pathlib import Path
from typing import List

from ewoc_dag.provider.aws import download_l8_prd_from_aws
from ewoc_dag.utils import get_product_by_id


logger = logging.getLogger(__name__)

# def get_l8_vsi_path(prd_id:str, l2_mask_only:bool=False, bands_ids: List=None)

def get_l8_product(prd_id:str, out_root_dirpath:Path, source:str='aws:s3:usgs-landsat-c02',
                   eodag_config_file=None, l2_mask_only:bool=False):
    """
    Retrieve Landsat 8 data via eodag or directly from a object storage
    :param product_id: Landsat 8 product id
    :param out_dir: Ouptut directory
    :param provider: Data provider (creodias)
    :param config_file: eodag config file, if None the creds will be selected from env vars
    """
    if source is None:
        source = os.getenv('EWOC_L8_DATA_SOURCE')

    if source == 'usgs:https':
        logging.info('Use USGS EE https API!',)
        if l2_mask_only:
            logger.warning('https API does not support to retrieve l2 mask only')
        get_product_by_id(
            prd_id,
            out_root_dirpath,
            provider='usgs',
            config_file=eodag_config_file,
            product_type="TODO"
        )
    elif source == 'aws:s3:usgs-landsat-c02':
        logging.info('Use usgs aws object storage!')
        download_l8_prd_from_aws(prd_id, out_root_dirpath, l2_mask_only)
 