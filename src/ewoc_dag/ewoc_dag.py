# -*- coding: utf-8 -*-
""" DAG for Sentinel-1 GRD products
"""
import logging
from pathlib import Path
from tempfile import gettempdir

from ewoc_dag.bucket.ewoc import EWOCPRDBucket

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


def get_bucket_prefix(
    bucket_prefix: str, out_dirpath_root: Path = Path(gettempdir())
) -> None:
    """Retrieve Sentinel-1 product according to the product id and the source

    Args:
        prd_id (str): Sentinel-1 product ID
        out_root_dirpath (Path, optional): Path where to write the S1 product.
         Defaults to Path(gettempdir()).

    Returns:
        Path: Path to the S1 product
    """

    ewoc_prd_bucket = EWOCPRDBucket()
    ewoc_prd_bucket.download_bucket_prefix(bucket_prefix,
                                           out_dirpath=out_dirpath_root)

def get_blocks(production_id:str,
               tile_id:str,
               season:str,
               year:str,
               out_dirpath_root: Path = Path(gettempdir())
               )->Path:
    """Retrieve blocks files from the production id

    Args:
        production_id (str): root bucket prefix where are produced the tiles
        tile_id (str): S2 MGRS tile id
        season (str): EWoC season
        year (str): production reference year
        out_root_dirpath (Path, optional): Path where to write the block files.
         Defaults to Path(gettempdir()).

    Returns:
        Path: Path to dir where the blocks files are written
    """
    #bucket_prefix= 'c728b264-5c97-4f4c-81fe-1500d4c4dfbd_26178_20221025141020/blocks/50QLL/2021_annual/annualcropland/classification/'
    bucket_prefix = f'{production_id}/blocks/{tile_id}/{year}_{season}'
    out_dirpath = out_dirpath_root / 'blocks' / tile_id
    out_dirpath.mkdir(exist_ok=True, parents=True)
    logger.info("Trying to download blocks: {bucket_prefix} to {out_dirpath} ")
    EWOCPRDBucket().download_bucket_prefix(bucket_prefix,
                                           out_dirpath=out_dirpath)

    return out_dirpath
