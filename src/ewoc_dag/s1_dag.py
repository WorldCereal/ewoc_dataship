import logging
import os
from pathlib import Path

from ewoc_dag.provider.aws import AWSDataProvider
from ewoc_dag.provider.creodias import CREODIASDataProvider
from ewoc_dag.utils import get_product_by_id

logger = logging.getLogger(__name__)


def get_s1_product(prd_id:str, out_root_dirpath:Path,
                   source:str='creodias', eodag_config_file=None):
    """
    Retrieve Sentinel-1 data via eodag or directly from a object storage
    :param prd_id: ex. S1B_IW_GRDH_1SDV_20200510T092220_20200510T092245_021517_028DAB_A416
    :param out_root_dirpath: Ouptut directory
    :param source: Data provider: 
    :param eodag_config_file: eodag config file, if None the creds will be selected from env vars
    """
    if source is None:
        source = os.getenv('EWOC_S1_DATA_SOURCE')

    if source == 'eodag':
        logging.info('Use EODAG to retrieve S1 product!',)
        get_product_by_id(
            prd_id,
            out_root_dirpath,
            provider='creodias', # TODO Keep eodag manage
            config_file=eodag_config_file,
            product_type="S1_SAR_GRD",
        )
    elif source == 'creodias':
        logging.info('Use CREODIAS object storage to retrieve S1 product!')
        creodias_s3=CREODIASDataProvider()
        creodias_s3.download_s1_prd(prd_id, out_root_dirpath)
    elif source == 'aws':
        logging.info('Use AWS object storage to retrieve S1 product!')
        aws_s3=AWSDataProvider()
        aws_s3.download_s1_prd(prd_id, out_root_dirpath)
    else:
        raise ValueError(f'Source {source} is not supported')
