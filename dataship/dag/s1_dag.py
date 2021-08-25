import logging
import os
from pathlib import Path

from dataship.dag.s3man import download_s1_prd_from_creodias
from dataship.dag.utils import get_product_by_id

logger = logging.getLogger(__name__)


def get_s1_product(prd_id:str, out_root_dirpath:Path, source:str='creodias_eodata', eodag_config_file=None):
    """
    Retrieve Sentinel-1 data via eodag or directly from a object storage
    """
    if source is None:
        source = os.getenv('EWOC_DATA_SOURCE')

    if source == 'creodias_finder':
        get_s1_product_by_id(prd_id, out_root_dirpath, provider='creodias', config_file=eodag_config_file)
    elif source == 'creodias_eodata':
        logging.info('Use creodias EODATA object storage!',)
        download_s1_prd_from_creodias(prd_id, out_root_dirpath)
    elif source == 'aws_s3':
        raise NotImplementedError('Get S1 product from AWS bucket is not currently implemented!')
    else:
        if eodag_config_file is not None:
            data_provider=os.getenv('EWOC_DATA_PROVIDER')
            logging.info('Use EODAG to retrieve the Sentinel-1 product with the following data provider %s.', data_provider)
            get_product_by_id(prd_id, out_root_dirpath, provider=data_provider)
        else:
            raise NotImplementedError

def get_s1_product_by_id(product_id, out_dir, provider=None, config_file=None):
    """
    Wrapper around get_product_by_id adapted for Sentinel-1 on creodias
    :param product_id: something like S1B_IW_GRDH_1SDV_20200510T092220_20200510T092245_021517_028DAB_A416
    :param out_dir: Ouptut directory
    :param provider: Data provider (creodias)
    :param config_file: eodag config file, if None the creds will be selected from env vars
    """
    if provider is None:
        provider = os.getenv("EWOC_DATA_PROVIDER")

    if provider == "creodias":
        get_product_by_id(
            product_id,
            out_dir,
            provider=provider,
            config_file=config_file,
            product_type="S1_SAR_GRD",
        )
    else:
        get_product_by_id(
            product_id, out_dir, provider=provider, config_file=config_file
        )
