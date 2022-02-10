# -*- coding: utf-8 -*-
""" DAG for SRTM 1s and 3s tiles
"""
import os
import logging
import zipfile
from pathlib import Path
from typing import List

import requests
from eotile.eotile_module import main

from ewoc_dag.bucket.creodias import CreodiasBucket
from ewoc_dag.bucket.ewoc import EWOCAuxDataBucket

logger = logging.getLogger(__name__)

_SRTM_1S_SOURCES = ["esa", "creodias"]


def get_srtm_1s_default_provider() -> str:
    """Return the default provider according the computation of two env variables:
        - EWOC_CLOUD_PROVIDER
        - EWOC_SRTM_1S_PROVIDER
    The first superseed the second one. By default the source used is ESA.

    Returns:
        str: srtm1s data provider
    """
    return os.getenv(
        "EWOC_CLOUD_PROVIDER", os.getenv("EWOC_SRTM_1S_PROVIDER", _SRTM_1S_SOURCES[0])
    )


def get_srtm_from_s2_tile_id(
    s2_tile_id: str, out_dirpath: Path, source: str = None, resolution: str = "1s"
) -> None:
    """
    Retrieve srtm 1s data for a Sentinel-2 tile id from the source into the output dir
    :param s2_tile_ids: Sentinel-2 tile id
    :param out_dirpath: Output directory where the srtm data is downloaded
    :param source: Source where to retrieve the srtm 1s data
    :param resolution: 1s or 3s for respectively 30m and 90m srtm
    """
    if resolution == "1s":
        srtm_tile_ids = get_srtm1s_ids(s2_tile_id)
    elif resolution == "3s":
        srtm_tile_ids = get_srtm3s_ids(s2_tile_id)
    get_srtm_tiles(srtm_tile_ids, out_dirpath, source=source, resolution=resolution)


def get_srtm_tiles(
    srtm_tile_ids: List[str], out_dir: Path, source: str = None, resolution: str = "1s"
) -> None:
    """
    Retrieve srtm 1s data from the source into the output dir
    :param srtm_tile_ids: List of srtm tile ids
    :param out_dirpath: Output directory where the srtm data is downloaded
    :param source: Source where to retrieve the srtm 1s data
    """
    if source is None:
        if resolution == "1s":
            srtm_provider = get_srtm_1s_default_provider()
        elif resolution == "3s":
            srtm_provider = "ewoc"
    else:
        srtm_provider = source

    if srtm_provider == "esa":
        if resolution == "1s":
            logger.info("Use ESA website to retrieve the srtm 1s data!")
            get_srtm_from_esa(srtm_tile_ids, out_dir)
        else:
            raise ValueError(f"Source SRTM{resolution} not available on ESA website!")
    elif srtm_provider == "creodias":
        if resolution == "1s":
            logger.info("Use creodias bucket to retrieve srtm 1s data!")
            CreodiasBucket().download_srtm1s_tiles(srtm_tile_ids, out_dir)
        else:
            raise ValueError(
                f"Source SRTM{resolution} not available on CREODIAS bucket!"
            )
    elif srtm_provider == "ewoc":
        if resolution == "3s":
            logger.info("Use EWoC bucket to retrieve srtm data!")
            EWOCAuxDataBucket().download_srtm3s_tiles(srtm_tile_ids, out_dir)
        else:
            raise ValueError(
                f"Source SRTM{resolution} not available on EWoC Aux Data bucket!"
            )
    elif srtm_provider == "usgs":
        logger.info("Use usgs EE to retrieve srtm 1s data!")
        raise NotImplementedError
    else:
        raise ValueError(f"Source {srtm_provider} not supported for srtm!")


def get_srtm_from_esa(srtm_tile_ids: List[str], out_dirpath: Path) -> None:
    """
    Retrieve srtm 1s data from ESA website into the output dir
    :param srtm_tile_ids: List of srtm tile ids
    :param out_dirpath: Output directory where the srtm data is downloaded
    """
    esa_website_root = "http://step.esa.int/auxdata/dem/SRTMGL1/"
    for srtm_tile_id in srtm_tile_ids:
        srtm_tile_id_filename = srtm_tile_id + ".SRTMGL1.hgt.zip"
        srtm_tile_id_url = esa_website_root + srtm_tile_id_filename

        response = requests.get(srtm_tile_id_url)
        # pylint: disable=no-member
        if response.status_code != requests.codes.ok:
            logger.error(
                "%s not dwnloaded (error_code: %s) from %s!",
                srtm_tile_id_filename,
                response.status_code,
                srtm_tile_id_url,
            )
            continue

        logger.info("%s downloaded!", srtm_tile_id_filename)

        srtm_tile_id_filepath = out_dirpath / srtm_tile_id_filename
        with open(srtm_tile_id_filepath, "wb") as srtm_file:
            srtm_file.write(response.content)

        with zipfile.ZipFile(srtm_tile_id_filepath, "r") as srtm_zipfile:
            srtm_zipfile.extractall(out_dirpath)

        srtm_tile_id_filepath.unlink()


def get_srtm1s_ids(s2_tile_id: str) -> List[str]:
    """
    Get srtm 1s id for an S2 tile
    :param s2 tile_id:
    :return: List of srtm ids
    """
    res = main(s2_tile_id, dem=True, overlap=True, no_l8=True, no_s2=True)
    return list(res[2].id)


def get_srtm3s_ids(s2_tile_id: str) -> List[str]:
    """
    Get srtm 3s id for an S2 tile
    :param s2 tile_id:
    :return: List of srtm ids
    """
    res = main(s2_tile_id, srtm5x5=True, overlap=True, no_l8=True, no_s2=True)
    return list(res[3]["id"].values)
