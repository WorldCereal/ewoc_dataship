# -*- coding: utf-8 -*-
""" DAG for Copernicus DEM tiles
"""
import logging
import os
from pathlib import Path
from tempfile import gettempdir
from typing import List

from eotile.eotile_module import main

from ewoc_dag.bucket.creodias import CreodiasBucket
from ewoc_dag.bucket.aws import AWSCopDEMBucket


logger = logging.getLogger(__name__)

_COPDEM_SOURCES = ["aws", "creodias"]
_COPDEM_RESOLUTIONS = ["1s", "3s"]


def get_copdem_default_provider() -> str:
    """Return the default provider according the computation of two env variables:
        - EWOC_CLOUD_PROVIDER
        - EWOC_COPDEM_PROVIDER
    The first superseed the second one

    Returns:
        str: the cop dem provider
    """

    return os.getenv(
        "EWOC_CLOUD_PROVIDER", os.getenv("EWOC_COPDEM_SOURCE", _COPDEM_SOURCES[0])
    )


def get_copdem_from_s2_tile_id(
    s2_tile_id: str,
    out_dirpath: Path = Path(gettempdir()),
    source: str = None,
    resolution: str = _COPDEM_RESOLUTIONS[0],
) -> None:
    """
    Retrieve copdem data for a Sentinel-2 tile id from the source into the output dir
    :param s2_tile_ids: Sentinel-2 tile id
    :param out_dirpath: Output directory where the cop dem data is downloaded
    :param source: Source where to retrieve the srtm 1s data
    :param resolution: 30 or 90 for respectively 30m and 90m cop dem
    """
    if source is None:
        copdem_provider = get_copdem_default_provider()
        logger.info("Use %s as copdem provider")
    else:
        copdem_provider = source
    get_copdem_tiles(
        get_copdem_ids(s2_tile_id),
        out_dirpath,
        source=copdem_provider,
        resolution=resolution,
    )


def get_copdem_tiles(
    srtm_tile_ids: List[str],
    out_dir: Path = Path(gettempdir()),
    source: str = None,
    resolution: str = _COPDEM_RESOLUTIONS[0],
) -> None:
    """
    Retrieve cop dem data from the source into the output dir
    :param srtm_tile_ids: List of srtm tile ids
    :param out_dirpath: Output directory where the srtm data is downloaded
    :param source: Source where to retrieve the cop dem data
    """
    if source is None:
        copdem_provider = get_copdem_default_provider()
        logger.info("Use %s as copdem provider")
    else:
        copdem_provider = source

    if copdem_provider == "creodias":
        logger.info("Use creodias bucket to retrieve cop dem data!")
        CreodiasBucket().download_copdem_tiles(
            srtm_tile_ids, out_dirpath=out_dir, resolution=resolution
        )
    elif copdem_provider == "aws":
        logger.info("Use AWS bucket to retrieve cop dem data!")
        AWSCopDEMBucket(resolution=resolution).download_tiles(srtm_tile_ids, out_dir)
    else:
        raise ValueError(f"Source {copdem_provider} not supported {_COPDEM_SOURCES}!")


def get_copdem_ids(s2_tile_id: str) -> List[str]:
    """
    Get coptem id for an S2 tile
    :param s2 tile_id:
    :return: List of srtm ids
    """
    res = main(s2_tile_id, dem=True, overlap=True, no_l8=True, no_s2=True)
    return list(res[2].id)


def get_gdal_vrt_files(
    copdem_tile_ids: List[str],
    source: str = None,
    resolution: str = _COPDEM_RESOLUTIONS[0],
) -> List[str]:
    """Compute vsis3 files for the COP DEM files from a bucket

    Args:
        copdem_tile_ids (List[str]): List of COP DEM tile ID
        source (str, optional): Source of COP DEM. Defaults to _COPDEM_DEFAULT_SOURCE.
        resolution (str, optional): Resolution of the COP DEM. Defaults to _COPDEM_RESOLUTIONS[0].

    Raises:
        ValueError: if the source is not in _COPDEM_SOURCES

    Returns:
        List[str]: List of vsis3 file for gdal commands
    """
    if source is None:
        copdem_provider = get_copdem_default_provider()
        logger.info("Use %s as copdem provider")
    else:
        copdem_provider = source

    copdem_gdal_paths = []
    if copdem_provider == "aws":
        copdem_bucket = AWSCopDEMBucket(resolution=resolution)
        for copdem_tile_id in copdem_tile_ids:
            copdem_gdal_paths.append(copdem_bucket.to_gdal_path(copdem_tile_id))
    elif copdem_provider == "creodias":
        raise NotImplementedError("Currently not implemented")
    else:
        raise ValueError(f"Source {copdem_provider} not supported {_COPDEM_SOURCES}!")

    return copdem_gdal_paths


def to_gdal_vrt_input_file_list(
    copdem_tile_ids: List[str],
    source: str = None,
    resolution: str = _COPDEM_RESOLUTIONS[0],
    filepath: Path = Path(gettempdir()) / "copdem_list.txt",
) -> None:
    """Write gdalbuildvrt file with vsis3 file according to the COP DEM tile id

    Args:
        copdem_tile_ids (List[str]): List of COP DEM tile ID
        source (str, optional): Source of COP DEM. Defaults to _COPDEM_DEFAULT_SOURCE.
        resolution (str, optional):  Resolution of the COP DEM. Defaults to _COPDEM_RESOLUTIONS[0].
        filepath (Path, optional): Filepath where to write the vsis3 file.
            Defaults to Path(gettempdir())/"copdem_list.txt".
    """
    if source is None:
        copdem_provider = get_copdem_default_provider()
        logger.info("Use %s as copdem provider")
    else:
        copdem_provider = source
    with open(filepath, "wt", encoding="UTF8") as out_txt_file:
        out_txt_file.write(
            "\n".join(
                get_gdal_vrt_files(
                    copdem_tile_ids, source=copdem_provider, resolution=resolution
                )
            )
        )


if __name__ == "__main__":
    import sys

    LOG_FORMAT = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format=LOG_FORMAT,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    _TILE_ID = "31TCJ"
    logger.info(get_copdem_ids(_TILE_ID))
    logger.info(get_gdal_vrt_files([_TILE_ID]))
    to_gdal_vrt_input_file_list(get_copdem_ids(_TILE_ID))
    get_copdem_from_s2_tile_id(_TILE_ID, source="aws")
    get_copdem_from_s2_tile_id(
        _TILE_ID, source="aws", resolution=_COPDEM_RESOLUTIONS[1]
    )
