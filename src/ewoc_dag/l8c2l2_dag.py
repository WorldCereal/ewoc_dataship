# -*- coding: utf-8 -*-
""" DAG for Landsat8 Collection 2 products
"""
import logging
from pathlib import Path
from tempfile import gettempdir
from typing import Optional, List


from ewoc_dag.bucket.aws import AWSL8C2L2Bucket
from ewoc_dag.eodag_utils import get_product_by_id


_logger = logging.getLogger(__name__)

_L8C2_SOURCES = ["eodag", "aws"]


def get_l8c2l2_product(
    prd_id: str,
    out_root_dirpath: Path = Path(gettempdir()),
    source: str = _L8C2_SOURCES[1],
    eodag_config_file: Optional[Path] = None,
    prd_items: Optional[List[str]] = None,
) -> Path:
    """Retrieve Landsat8 Collection 2 Level 2 product according to
     the product id and the source

    Args:
        prd_id (str): landsat8 C2 L2 product ID
        out_root_dirpath (Path, optional):  Path where to write the L8 product.
         Defaults to Path(gettempdir()).
        source (str, optional): Source used to retrieve the L8 product.
         Defaults to _L8C2_SOURCES[1].
        eodag_config_file (Path, optional): Path to the EODAG config file.
         Defaults to None.
        prd_items (List[str], optional): Items of the product to download.
         Defaults to None.

    Raises:
        ValueError: if the source is not supported

    Returns:
        Path: Path to the L8 product
    """
    if source == _L8C2_SOURCES[0]:
        _logger.info("Use EODAG to retrieve Landsat 8 L2 C2 product!")
        if prd_items is not None:
            _logger.warning(
                "EODAG does not support to retrieve element of the product!"
            )
        out_prd_path = get_product_by_id(
            prd_id,
            out_root_dirpath,
            provider="usgs",  # TODO Keep manage EODAG
            config_file=eodag_config_file,
            product_type="TODO",
        )
    elif source == _L8C2_SOURCES[1]:
        _logger.info("Use AWS to retrieve Landsat 8 L2 C2 product!")
        out_prd_path = AWSL8C2L2Bucket().download_prd(
            prd_id, out_root_dirpath, prd_items=prd_items
        )
    else:
        raise ValueError(f"Source {source} is not supported: not in {_L8C2_SOURCES}")

    return out_prd_path


def get_l8c2l2_gdal_path(prd_id: str, prd_item: str) -> str:
    """_summary_

    Args:
        prd_id (str): landsat8 C2 L2 product ID
        prd_item (str): Item of the landsat8 C2 L2 product

    Returns:
        str: the gdal vsi path to the item in the AWS USGS bucket
    """
    return AWSL8C2L2Bucket().to_gdal_path(prd_id, prd_item)


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
    get_l8c2l2_product(_PRD_ID)
    get_l8c2l2_product(_PRD_ID, prd_items=["ST_TRAD", "QA_PIXEL"])
    _logger.info(get_l8c2l2_gdal_path(_PRD_ID, "ST_B10"))
