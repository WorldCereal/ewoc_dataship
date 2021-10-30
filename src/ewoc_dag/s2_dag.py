import logging
import os
from pathlib import Path

from ewoc_dag.eo_prd_id.s2_prd_id import S2PrdIdInfo
from ewoc_dag.provider.aws import AWSDataProvider
from ewoc_dag.provider.creodias import CREODIASDataProvider
from ewoc_dag.utils import get_product_by_id


logger = logging.getLogger(__name__)


def get_s2_product(prd_id:str, out_root_dirpath:Path, source:str='creodias',
                   eodag_config_file=None, l2_mask_only:bool=False,
                   aws_l2a_cogs=True):
    """
    Retrieve Sentinel-2 data via eodag or directly from a object storage
    :param product_id: Sentinel product id
    :param out_dir: Ouptut directory
    :param provider: Data provider (creodias)
    :param config_file: eodag config file, if None the creds will be selected from env vars
    """

    if source is None:
        source = os.getenv('EWOC_S2_DATA_SOURCE')

    if source == 'eodag':
        logging.info('Use EODAG to retrieve Sentinel-2 product!')
        if S2PrdIdInfo.is_l1c(prd_id):
            product_type = 'S2_MSI_L1C'
        else:
            if l2_mask_only:
                logger.error('EODAG does not support to retrieve l2a mask only')

            product_type= 'S2_MSI_L2A'
        get_product_by_id(
            prd_id,
            out_root_dirpath,
            provider='creodias', # Keep EODAG manage
            config_file=eodag_config_file,
            product_type=product_type
        )
    elif source == 'creodias':
        logging.info('Use CREODIAS object storage to retrieve Sentinel-2 product!')
        CREODIASDataProvider().download_s2_prd(prd_id, out_root_dirpath,
                                            l2_mask_only=l2_mask_only)
    elif source == 'aws':
        logging.info('Use AWS object storage to retrieve Sentinel-2 product!')
        AWSDataProvider().download_s2_prd(prd_id, out_root_dirpath,
                                        l2_mask_only=l2_mask_only,
                                        l2a_cogs=aws_l2a_cogs)
    else:
        raise ValueError(f'Source {source} is not supported')
