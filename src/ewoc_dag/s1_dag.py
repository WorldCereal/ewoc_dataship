# -*- coding: utf-8 -*-
""" DAG for Sentinel-1 GRD products
"""
import logging
import os
from pathlib import Path
from tempfile import gettempdir

from ewoc_dag.bucket.aws import AWSDownloadError, AWSS1Bucket
from ewoc_dag.bucket.creodias import CreodiasBucket
from ewoc_dag.eodag_utils import get_product_by_id
from typing import Optional

logger = logging.getLogger(__name__)

_S1_SOURCES = ["eodag", "aws", "creodias"]


class S1DagError(Exception):
    """Exception raised for errors in the S1 SAFE conversion format on AWS."""

    def __init__(self, error=None):
        self._error = error
        self.message = "Error while S1 downloading:"
        super().__init__(self.message)

    def __str__(self):
        return f"{self.message} {self._error}"


def get_s1_default_provider() -> str:
    """Return the default provider according the computation of two env variables:
        - EWOC_CLOUD_PROVIDER
        - EWOC_S1_PROVIDER
    The first superseed the second one. By default the source used is EODAG.

    Returns:
        str: s1 data provider
    """
    return os.getenv(
        "EWOC_CLOUD_PROVIDER", os.getenv("EWOC_S1_PROVIDER", _S1_SOURCES[0])
    )


def get_s1_product(
    prd_id: str,
    out_root_dirpath: Path = Path(gettempdir()),
    source: Optional[str] = None,
    eodag_config_file: Optional[Path] = None,
    safe_format: bool = False,
) -> Path:
    """Retrieve Sentinel-1 product according to the product id and the source

    Args:
        prd_id (str): Sentinel-1 product ID
        out_root_dirpath (Path, optional): Path where to write the S1 product.
         Defaults to Path(gettempdir()).
        source (str, optional): Source used to retrieve the S1 product.
         Defaults to None.
         If None, the source is computed thanks to get_s1_default_provider method
        eodag_config_file (Path, optional): Path to the EODAG config file.
         Defaults to None.
        safe_format (bool, optional): Translate from format used in AWS bucket to SAFE format.
         Used only with AWS source.
         Defaults to False.

    Returns:
        Path: Path to the S1 product

    Raises:
        ValueError: if the source is not supported
    """

    if source is None:
        s1_provider = get_s1_default_provider()
    else:
        s1_provider = source

    if s1_provider == "eodag":
        logging.info(
            "Use EODAG to retrieve S1 product!",
        )
        s1_prd_path = get_product_by_id(
            prd_id,
            out_root_dirpath,
            provider="creodias",  # TODO Keep eodag manage
            config_file=eodag_config_file,
            product_type="S1_SAR_GRD",
        )
    elif s1_provider == "creodias":
        logging.info("Use CREODIAS object storage to retrieve S1 product!")
        s1_prd_path = CreodiasBucket().download_s1_prd(prd_id, out_root_dirpath)
    elif s1_provider == "aws":
        logging.info("Use AWS object storage to retrieve S1 product!")
        try:
            s1_prd_path = AWSS1Bucket().download_prd(
                prd_id, out_root_dirpath, safe_format=safe_format
            )
        except AWSDownloadError as exc:
            logger.error(exc)
            raise S1DagError(exc) from exc
    else:
        raise ValueError(f"Source {s1_provider} is not supported")

    return s1_prd_path
