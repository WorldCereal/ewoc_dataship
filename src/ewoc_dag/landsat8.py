# -*- coding: utf-8 -*-
""" DAG for Landsat8 Collection 2 products
"""
import logging
import os
from pathlib import Path
from tempfile import gettempdir


from ewoc_dag.bucket.aws import AWSS2L8C2Bucket
from ewoc_dag.eodag_utils import get_product_by_id


logger = logging.getLogger(__name__)

_L8C2_SOURCES = ["eodag", "aws"]


def get_l8_product(
    prd_id: str,
    out_root_dirpath: Path = Path(gettempdir()),
    source: str = "aws",
    eodag_config_file: Path = None,
    l2_mask_only: bool = False,
) -> None:
    """
    Retrieve Landsat 8 L2 C2 product via eodag or directly from a object storage
    :param product_id: Landsat 8 L2 C2 product id
    :param out_dir: Ouptut directory
    :param provider: Data provider:
    :param config_file: eodag config file, if None the creds will be selected from env vars
    """

    if source is None:
        source = os.getenv("EWOC_L8C2_PROVIDER", "eodag")

    if source == "eodag":
        logging.info("Use EODAG to retrieve Landsat 8 L2 C2 product!")
        if l2_mask_only:
            logger.warning("EODAG does not support to retrieve l2 mask only!")
        get_product_by_id(
            prd_id,
            out_root_dirpath,
            provider="usgs",  # TODO Keep manage EODAG
            config_file=eodag_config_file,
            product_type="TODO",
        )
    elif source == "aws":
        logging.info("Use AWS to retrieve Landsat 8 L2 C2 product!")
        AWSS2L8C2Bucket().download_prd(prd_id, out_root_dirpath)
    else:
        raise ValueError(f"Source {source} is not supported")


if __name__ == "__main__":
    import sys

    LOG_FORMAT = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format=LOG_FORMAT,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    _PRD_ID = "LC08_L2SP_227099_20211017_20211026_02_T2"
    get_l8_product(_PRD_ID)
