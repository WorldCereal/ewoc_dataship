# -*- coding: utf-8 -*-
""" DAG for Sentinel-2 L1C and L2A products
"""
import logging
import os
from pathlib import Path
from tempfile import gettempdir

from ewoc_dag.eo_prd_id.s2_prd_id import S2PrdIdInfo
from ewoc_dag.bucket.aws import AWSS2L1CBucket, AWSS2L2ABucket, AWSS2L2ACOGSBucket
from ewoc_dag.bucket.creodias import CreodiasBucket
from ewoc_dag.eodag_utils import get_product_by_id

logger = logging.getLogger(__name__)

_S2_SOURCES = ["eodag", "aws", "creodias"]


def get_s2_default_provider() -> str:
    """Return the default provider according the computation of two env variables:
        - EWOC_CLOUD_PROVIDER
        - EWOC_S2_PROVIDER
    The first superseed the second one

    Returns:
        str: s2 data provider
    """
    return os.getenv(
        "EWOC_CLOUD_PROVIDER", os.getenv("EWOC_S2_PROVIDER", _S2_SOURCES[0])
    )


def get_s2_product(
    prd_id: str,
    out_root_dirpath: Path = Path(gettempdir()),
    source: str = None,
    eodag_config_file: Path = None,
    l2_mask_only: bool = False,
    aws_l2a_cogs: bool = True,
):
    """
    Retrieve Sentinel-2 data via eodag or directly from a object storage
    :param product_id: Sentinel product id
    :param out_dir: Ouptut directory
    :param provider: Data provider (creodias)
    :param config_file: eodag config file, if None the creds will be selected from env vars
    """

    if source is None:
        s2_provider = get_s2_default_provider()
    else:
        s2_provider = source

    if s2_provider == "eodag":
        logging.info("Use EODAG to retrieve Sentinel-2 product!")
        if S2PrdIdInfo.is_l1c(prd_id):
            product_type = "S2_MSI_L1C"
        else:
            if l2_mask_only:
                logger.error("EODAG does not support to retrieve l2a mask only")

            product_type = "S2_MSI_L2A"
        get_product_by_id(
            prd_id,
            out_root_dirpath,
            provider="creodias",  # TODO keep eodag manage
            config_file=eodag_config_file,
            product_type=product_type,
        )
    elif s2_provider == "creodias":
        logging.info("Use CREODIAS object storage to retrieve Sentinel-2 product!")
        CreodiasBucket().download_s2_prd(
            prd_id, out_root_dirpath, l2_mask_only=l2_mask_only
        )
    elif s2_provider == "aws":
        logging.info("Use AWS object storage to retrieve Sentinel-2 product!")
        if S2PrdIdInfo.is_l1c(prd_id):
            AWSS2L1CBucket().download_prd(
                prd_id,
                out_root_dirpath,
            )
        else:
            if aws_l2a_cogs:
                AWSS2L2ACOGSBucket().download_prd(
                    prd_id,
                    out_dirpath_root=out_root_dirpath,
                    l2a_mask_only=l2_mask_only,
                )
            else:
                AWSS2L2ABucket().download_prd(
                    prd_id,
                    out_dirpath_root=out_root_dirpath,
                    l2a_mask_only=l2_mask_only,
                )

    else:
        raise ValueError(f"Source {s2_provider} is not supported")


if __name__ == "__main__":
    import sys

    LOG_FORMAT = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format=LOG_FORMAT,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    _L1C_PRD_ID = "S2B_MSIL1C_20210714T235249_N0301_R130_T57KUR_20210715T005654.SAFE"
    _L2A_PRD_ID = "S2B_MSIL2A_20210714T131719_N0301_R124_T28WDB_20210714T160455.SAFE"
    get_s2_product(_L1C_PRD_ID, source="aws")
    get_s2_product(_L2A_PRD_ID, source="aws")
    get_s2_product(_L2A_PRD_ID, source="aws", l2_mask_only=True)
    get_s2_product(_L2A_PRD_ID, source="aws", aws_l2a_cogs=True)
    get_s2_product(_L2A_PRD_ID, source="aws", aws_l2a_cogs=True, l2_mask_only=True)
