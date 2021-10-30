import logging
from pathlib import Path
from typing import List
import zipfile

import requests
from eotile.eotile_module import main

from ewoc_dag.provider.creodias import CREODIASDataProvider
from ewoc_dag.provider.ewoc import EWOCDataProvider


logger = logging.getLogger(__name__)


def get_srtm_from_s2_tile_id(s2_tile_id: str, out_dirpath: Path,
                             source: str = "esa", resolution='1s') -> None:
    """
    Retrieve srtm 1s data for a Sentinel-2 tile id from the source into the output dir
    :param s2_tile_ids: Sentinel-2 tile id
    :param out_dirpath: Output directory where the srtm data is downloaded
    :param source: Source where to retrieve the srtm 1s data
    :param resolution: 1s or 3s for respectively 30m and 90m srtm
    """
    get_srtm_tiles(get_srtm1s_ids(s2_tile_id), out_dirpath,
                   source=source, resolution=resolution)


def get_srtm_tiles(srtm_tile_ids: List[str], out_dir: Path,
                    source: str = "esa", resolution='1s') -> None:
    """
    Retrieve srtm 1s data from the source into the output dir
    :param srtm_tile_ids: List of srtm tile ids
    :param out_dirpath: Output directory where the srtm data is downloaded
    :param source: Source where to retrieve the srtm 1s data
    """
    if source == "esa":
        if resolution != "1s":
            logger.info("Use ESA website to retrieve the srtm 1s data!")
            get_srtm_from_esa(srtm_tile_ids, out_dir)
        else:
            raise ValueError(f"Source SRTM{resolution} not available on ESA website!")
    elif source == "creodias":
        if resolution != "1s":
            logger.info("Use creodias bucket to retrieve srtm 1s data!")
            CREODIASDataProvider().download_srtm1s_tiles(srtm_tile_ids, out_dir)
        else:
            raise ValueError(f"Source SRTM{resolution} not available on CREODIAS bucket!")
    elif source == "ewoc":
        logger.info("Use EWoC bucket to retrieve srtm data!")
        EWOCDataProvider().download_srtm_tiles(srtm_tile_ids, out_dir,
                                               resolution=resolution)
    elif source == "usgs":
        logger.info("Use usgs EE to retrieve srtm 1s data!")
        raise NotImplementedError
    else:
        raise ValueError(f"Source {source} not supported!")

def get_srtm_from_esa(srtm_tile_ids: List[str], out_dirpath: Path) -> None:
    """
    Retrieve srtm 1s data from ESA website into the output dir
    :param srtm_tile_ids: List of srtm tile ids
    :param out_dirpath: Output directory where the srtm data is downloaded
    """
    ESA_WEBSITE_ROOT = "http://step.esa.int/auxdata/dem/SRTMGL1/"
    for srtm_tile_id in srtm_tile_ids:
        srtm_tile_id_filename = srtm_tile_id + ".SRTMGL1.hgt.zip"
        srtm_tile_id_url = ESA_WEBSITE_ROOT + srtm_tile_id_filename

        response = requests.get(srtm_tile_id_url)
        if response.status_code != requests.codes.ok:
            logger.error(
                "%s not dwnloaded (error_code: %s) from %s!",
                srtm_tile_id_filename,
                response.status_code,
                srtm_tile_id_url,
            )
            continue
        else:
            logger.info("%s downloaded!", srtm_tile_id_filename)

        srtm_tile_id_filepath = out_dirpath / srtm_tile_id_filename
        with open(srtm_tile_id_filepath, "wb") as srtm_file:
            srtm_file.write(response.content)

        with zipfile.ZipFile(srtm_tile_id_filepath, "r") as srtm_zipfile:
            srtm_zipfile.extractall(out_dirpath)

        srtm_tile_id_filepath.unlink()

def get_srtm1s_ids(s2_tile_id: str) -> None:
    """
    Get srtm 1s id for an S2 tile
    :param s2 tile_id:
    :return: List of srtm ids
    """
    res = main(s2_tile_id, dem=True, overlap=True, no_l8=True, no_s2=True)
    return list(res[2].id)
