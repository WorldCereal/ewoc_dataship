import logging
import os
from pathlib import Path

from ewoc_dag.provider.aws import download_s1_prd_from_aws
from ewoc_dag.provider.creodias import download_s1_prd_from_creodias
from ewoc_dag.utils import get_product_by_id

logger = logging.getLogger(__name__)


def get_s1_product(prd_id:str, out_root_dirpath:Path,
                   source:str='creodias:s3', eodag_config_file=None):
    """
    Retrieve Sentinel-1 data via eodag or directly from a object storage
    :param prd_id: ex. S1B_IW_GRDH_1SDV_20200510T092220_20200510T092245_021517_028DAB_A416
    :param out_root_dirpath: Ouptut directory
    :param source: Data provider (creodias)
    :param eodag_config_file: eodag config file, if None the creds will be selected from env vars
    """
    if source is None:
        source = os.getenv('EWOC_S1_DATA_SOURCE')

    if source == 'creodias:https':
        logging.info('Use creodias EODATA https API!',)
        get_product_by_id(
            prd_id,
            out_root_dirpath,
            provider='creodias',
            config_file=eodag_config_file,
            product_type="S1_SAR_GRD",
        )
    elif source == 'creodias:s3':
        logging.info('Use creodias EODATA object storage API!')
        download_s1_prd_from_creodias(prd_id, out_root_dirpath)
    elif source == 'aws:sentinel-s1-l1c':
        download_s1_prd_from_aws(prd_id, out_root_dirpath)
        raise NotImplementedError('Get S1 product from AWS bucket is not currently implemented!')
    else:
        raise ValueError(f'Source {source} is not supported')
