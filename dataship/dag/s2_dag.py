import logging
import os
from pathlib import Path

from dataship.remote.sentinel_cloud_mask import Sentinel_Cloud_Mask
from dataship.dag.s3man import download_s2_prd_from_creodias
from dataship.dag.utils import get_product_by_id
import re

logger = logging.getLogger(__name__)


def get_s2_product(prd_id:str, out_root_dirpath:Path, source:str='creodias_eodata', eodag_config_file=None):
    """
    Retrieve Sentinel-1 data via eodag or directly from a object storage
    """

    if source is None:
        source = os.getenv('EWOC_DATA_SOURCE')

    if source == 'creodias_finder':
        get_s2_product_by_id(prd_id, out_root_dirpath, provider='creodias', config_file=eodag_config_file)
    elif source == 'creodias_eodata':
        logging.info('Use creodias EODATA object storage!')
        download_s2_prd_from_creodias(prd_id, out_root_dirpath)
    elif source == 'aws_e84':
        pattern_t = r"(?<=_T)(.*?)(?=\_)"
        pattern_d = r"(?<=C_)(.*?)(?=\_)"
        tile_id = re.findall(pattern_t, prd_id)[0]
        date = re.findall(pattern_d, prd_id)[0].split("T")[0]
        cm_s2 = Sentinel_Cloud_Mask(tile_id, date, bucket="sentinel-cogs", prefix="sentinel-s2-l2a-cogs/")
        if cm_s2.mask_exists():
            cm_s2.download(str(Path(out_root_dirpath)/(prd_id+".tif")))  # TODO check this
        else:
            print("Mask doesn't exist on the specified bucket")
    else:
        # TODO: Implement the other two prodivers
        raise NotImplementedError


def get_s2_product_by_id(product_id, out_dir, provider=None, config_file=None):
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
            product_type=f"S2_MSI_L1C"
        )
    else:
        get_product_by_id(
            product_id, out_dir, provider=provider, config_file=config_file
        )
