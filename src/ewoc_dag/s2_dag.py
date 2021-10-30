import logging
import os
from pathlib import Path
import re

from ewoc_dag.provider.aws import AWSDataProvider
from ewoc_dag.provider.creodias import CREODIASDataProvider
from ewoc_dag.remote.sentinel_cloud_mask import Sentinel_Cloud_Mask
from ewoc_dag.utils import get_product_by_id


logger = logging.getLogger(__name__)


def get_s2_product(prd_id:str, out_root_dirpath:Path, source:str='creodias:s3:l1c',
                   eodag_config_file=None, l2_mask_only:bool=False):
    """
    Retrieve Sentinel-2 data via eodag or directly from a object storage
    :param product_id: Sentinel product id
    :param out_dir: Ouptut directory
    :param provider: Data provider (creodias)
    :param config_file: eodag config file, if None the creds will be selected from env vars
    """

    if source is None:
        source = os.getenv('EWOC_S2_DATA_SOURCE')

    if source == 'creodias:https:l1c':
        logging.info('Use creodias EODATA https API!',)
        if l2_mask_only:
            logger.warning('https API does not support to retrieve l2a mask only')
        get_product_by_id(
            prd_id,
            out_root_dirpath,
            provider='creodias',
            config_file=eodag_config_file,
            product_type="S2_MSI_L1C"
        )
    elif source == 'creodias:https:l2a':
        logging.info('Use creodias EODATA object storage!')
        get_product_by_id(
            prd_id,
            out_root_dirpath,
            provider='creodias',
            config_file=eodag_config_file,
            product_type="S2_MSI_L2A"
        )
    elif source == 'creodias:s3:l1c':
        logging.info('Use creodias EODATA object storage!')
        download_s2_prd(prd_id, out_root_dirpath)
    elif source == 'creodias:s3:l2a':
        logging.info('Use creodias EODATA object storage!')
        download_s2_prd(prd_id, out_root_dirpath,
                                      l2_mask_only=l2_mask_only)
    elif source == 'aws:sentinel-s2-l1c':
        download_s2_prd(prd_id, out_root_dirpath)
        raise NotImplementedError('Source not supported currently')
    elif source == 'aws:sentinel-s2-l2a':
        download_s2_prd(prd_id, out_root_dirpath,
                                 l2_mask_only=l2_mask_only)
        raise NotImplementedError('Source not supported currently')
    elif source == 'aws:sentinel-cogs':
        #download_s2_prd_from_aws(prd_id, out_root_dirpath,
        #                         l2a_mask_only=l2a_mask_only)
        if "L2A" in prd_id:
            raise NotImplementedError("A Level 2 product was given. This is not implemented yet")
        pattern_t = r"(?<=_T)(.*?)(?=\_)"
        pattern_d = r"(?<=C_)(.*?)(?=\_)"
        tile_id = re.findall(pattern_t, prd_id)[0]
        date = re.findall(pattern_d, prd_id)[0].split("T")[0]
        cm_s2 = Sentinel_Cloud_Mask(tile_id, date, bucket="sentinel-cogs",
                                    prefix="sentinel-s2-l2a-cogs/")
        if cm_s2.mask_exists():
            cm_s2.download(str(Path(out_root_dirpath)/(prd_id+".tif")))
        else:
            logger.error("Mask doesn't exist on the specified bucket")
    else:
        raise ValueError
