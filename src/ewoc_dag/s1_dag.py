# -*- coding: utf-8 -*-
""" DAG for Sentinel-1 GRD products
"""
import logging
import os
from pathlib import Path
from tempfile import gettempdir

from ewoc_dag.bucket.aws import AWSS1Bucket
from ewoc_dag.bucket.creodias import CreodiasBucket
from ewoc_dag.eodag_utils import get_product_by_id

logger = logging.getLogger(__name__)

_S1_SOURCES = ["eodag", "aws", "creodias"]


def get_s1_default_provider() -> str:
    """Return the default provider according the computation of two env variables:
        - EWOC_CLOUD_PROVIDER
        - EWOC_S1_PROVIDER
    The first superseed the second one

    Returns:
        str: s1 data provider
    """
    return os.getenv(
        "EWOC_CLOUD_PROVIDER", os.getenv("EWOC_S1_PROVIDER", _S1_SOURCES[0])
    )


def get_s1_product(
    prd_id: str,
    out_root_dirpath: Path = Path(gettempdir()),
    source: str = None,
    eodag_config_file=None,
):
    """
    Retrieve Sentinel-1 data via eodag or directly from a object storage
    :param prd_id: ex. S1B_IW_GRDH_1SDV_20200510T092220_20200510T092245_021517_028DAB_A416
    :param out_root_dirpath: Ouptut directory
    :param source: Data provider:
    :param eodag_config_file: eodag config file, if None the creds will be selected from env vars
    """

    if source is None:
        s1_provider = get_s1_default_provider()
    else:
        s1_provider = source

    if s1_provider == "eodag":
        logging.info(
            "Use EODAG to retrieve S1 product!",
        )
        get_product_by_id(
            prd_id,
            out_root_dirpath,
            provider="creodias",  # TODO Keep eodag manage
            config_file=eodag_config_file,
            product_type="S1_SAR_GRD",
        )
    elif s1_provider == "creodias":
        logging.info("Use CREODIAS object storage to retrieve S1 product!")
        CreodiasBucket().download_s1_prd(prd_id, out_root_dirpath)
    elif s1_provider == "aws":
        logging.info("Use AWS object storage to retrieve S1 product!")
        AWSS1Bucket().download_prd(prd_id, out_root_dirpath)
    else:
        raise ValueError(f"Source {s1_provider} is not supported")
