import logging
import os
from pathlib import Path

from ewoc_dag.provider.aws import AWSDataProvider
from ewoc_dag.provider.eodag_utils import get_product_by_id


logger = logging.getLogger(__name__)


def get_l8_product(prd_id:str, out_root_dirpath:Path, source:str='aws',
                   eodag_config_file=None, l2_mask_only:bool=False)->None:
    """
    Retrieve Landsat 8 L2 C2 product via eodag or directly from a object storage
    :param product_id: Landsat 8 L2 C2 product id
    :param out_dir: Ouptut directory
    :param provider: Data provider:
    :param config_file: eodag config file, if None the creds will be selected from env vars
    """
    if source is None:
        source = os.getenv('EWOC_L8_DATA_SOURCE')

    if source == 'eodag':
        logging.info('Use EODAG to retrieve Landsat 8 L2 C2 product!')
        if l2_mask_only:
            logger.error('EODAG does not support to retrieve l2 mask only')
        get_product_by_id(
            prd_id,
            out_root_dirpath,
            provider='usgs', # Keep manage EODAG
            config_file=eodag_config_file,
            product_type="TODO"
        )
    elif source == 'aws':
        logging.info('Use AWS to retrieve Landsat 8 L2 C2 product!')
        aws_s3 = AWSDataProvider()
        aws_s3.download_l8_c2_prd(prd_id, out_root_dirpath,
                                  l2_mask_only=l2_mask_only)
    else:
        raise ValueError(f'Source {source} is not supported')
