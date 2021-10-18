import logging
from pathlib import Path
from typing import List
import zipfile

import requests
from eotile.eotile_module import main

from ewoc_dag.s3man import download_s3file as dwnld_s3file
from ewoc_dag.s3man import download_srtm_tiles_from_ewoc, download_srtm_tiles_from_creodias

logger = logging.getLogger(__name__)

def get_srtm(tile_id, full_name=False):
    """
    Get srtm hgt files id for an S2 tile
    :param tile_id:
    :return: List of hgt files ids
    """
    res = main(tile_id, dem=True, overlap=True, no_l8=True, no_s2=True)
    srtm_df = res[2]
    list_ids = list(srtm_df.id)
    if full_name:
        out = [f"{tile}.SRTMGL1.hgt.zip" for tile in list_ids]
        return out
    else:
        return list_ids


def get_srtm1s(s2_tile_id: str, out_dirpath: Path, source: str = "esa") -> None:
    """
    Retrieve srtm 1s data for a Sentinel-2 tile id from the source into the output dir
    :param s2_tile_ids: Sentinel-2 tile id
    :param out_dirpath: Output directory where the srtm data is downloaded
    :param source: Source where to retrieve the srtm 1s data
    """
    get_srtm1s_from_ids(get_srtm1s_ids(s2_tile_id), out_dirpath, source=source)


def get_srtm1s_from_ids(
    srtm_tile_ids: List[str], out_dir: Path, source: str = "esa"
) -> None:
    """
    Retrieve srtm 1s data from the source into the output dir
    :param srtm_tile_ids: List of srtm tile ids
    :param out_dirpath: Output directory where the srtm data is downloaded
    :param source: Source where to retrieve the srtm 1s data
    """
    if source == "esa":
        logger.info("Use ESA website to retrieve the srtm 1s data!")
        get_srtm_from_esa(srtm_tile_ids, out_dir)
    elif source == "creodias_eodata":
        logger.info("Use creodias bucket to retrieve srtm 1s data!")
        get_srtm_from_creodias(srtm_tile_ids, out_dir)
    elif source == "ewoc":
        logger.info("Use EWoC bucket to retrieve srtm 1s data!")
        get_srtm_from_ewoc(srtm_tile_ids, out_dir)
    elif source == "usgs":
        logger.info("Use usgs EE to retrieve srtm 1s data!")
        raise NotImplementedError
    else:
        logger.error("Source %s not supported!", source)
        raise ValueError


def get_srtm_from_local_bucket(srtm_tile_ids: List[str], out_dirpath: Path) -> None:
    """
    Retrieve srtm 1s data from local bucket into the output dir
    :param srtm_tile_ids: List of srtm tile ids
    :param out_dirpath: Output directory where the srtm data is downloaded
    """
    local_bucket_name = "world-cereal"
    for srtm_tile_id in srtm_tile_ids:
        srtm_tile_id_filename = srtm_tile_id + ".SRTMGL1.hgt.zip"
        srtm_tile_id_filepath = out_dirpath / srtm_tile_id_filename
        dwnld_s3file("srtm30/" + srtm_tile_id_filename,
                     str(srtm_tile_id_filepath),
                     local_bucket_name)
        logger.debug("%s downloaded!", srtm_tile_id_filename)

        with zipfile.ZipFile(srtm_tile_id_filepath, "r") as srtm_zipfile:
            srtm_zipfile.extractall(out_dirpath)

        srtm_tile_id_filepath.unlink()

def get_srtm_from_ewoc(srtm_tile_ids: List[str], out_dirpath: Path) -> None:
    """
    Retrieve srtm 1s data from ewoc object storage into the output dir
    :param srtm_tile_ids: List of srtm tile ids
    :param out_dirpath: Output directory where the srtm data is downloaded
    """
    download_srtm_tiles_from_ewoc(srtm_tile_ids, out_dirpath)

def get_srtm_from_creodias(srtm_tile_ids: List[str], out_dirpath: Path) -> None:
    """
    Retrieve srtm 1s data from creodias eodata object storage into the output dir
    :param srtm_tile_ids: List of srtm tile ids
    :param out_dirpath: Output directory where the srtm data is downloaded
    """
    download_srtm_tiles_from_creodias(srtm_tile_ids, out_dirpath)

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

        r = requests.get(srtm_tile_id_url)
        if r.status_code != requests.codes.ok:
            logger.error(
                "%s not dwnloaded (error_code: %s) from %s!",
                srtm_tile_id_filename,
                r.status_code,
                srtm_tile_id_url,
            )
            continue
        else:
            logger.info("%s downloaded!", srtm_tile_id_filename)

        srtm_tile_id_filepath = out_dirpath / srtm_tile_id_filename
        with open(srtm_tile_id_filepath, "wb") as srtm_file:
            srtm_file.write(r.content)

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
