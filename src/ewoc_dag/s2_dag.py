import logging
import os
from pathlib import Path
import re

from ewoc_dag.remote.sentinel_cloud_mask import Sentinel_Cloud_Mask
from ewoc_dag.s3man import download_s2_prd_from_creodias
from ewoc_dag.utils import get_product_by_id


logger = logging.getLogger(__name__)


def get_s2_product(prd_id:str, out_root_dirpath:Path, source:str='creodias_eodata',
                   eodag_config_file=None):
    """
    Retrieve Sentinel-2 data via eodag or directly from a object storage
    """

    if source is None:
        source = os.getenv('EWOC_DATA_SOURCE')

    if source == 'creodias_finder':
        get_s2_product_by_id(prd_id, out_root_dirpath, provider='creodias',
                             config_file=eodag_config_file)
    elif source == 'creodias_eodata':
        logging.info('Use creodias EODATA object storage!')
        download_s2_prd_from_creodias(prd_id, out_root_dirpath)
    elif source == 'aws_e84':
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
        # TODO: Implement the other two prodivers
        raise NotImplementedError


def get_s2_product_by_id(product_id, out_dir, provider=None, config_file=None):
    """
    Wrapper around get_product_by_id adapted for Sentinel-1 on creodias
    :param product_id: Sentinel product id
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
            product_type="S2_MSI_L1C"
        )
    else:
        get_product_by_id(
            product_id, out_dir, provider=provider, config_file=config_file
        )
