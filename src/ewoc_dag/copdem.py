import logging
from pathlib import Path
from typing import List

from eotile.eotile_module import main

from ewoc_dag.provider.creodias import CREODIASDataProvider
from ewoc_dag.provider.aws import AWSDataProvider


logger = logging.getLogger(__name__)

def get_copdem_from_s2_tile_id(s2_tile_id: str, out_dirpath: Path,
                               source: str = "creodias", resolution='30') -> None:
    """
    Retrieve copdem data for a Sentinel-2 tile id from the source into the output dir
    :param s2_tile_ids: Sentinel-2 tile id
    :param out_dirpath: Output directory where the cop dem data is downloaded
    :param source: Source where to retrieve the srtm 1s data
    :param resolution: 30 or 90 for respectively 30m and 90m cop dem
    """
    get_copdem_tiles(get_copdem_ids(s2_tile_id), out_dirpath,
                    source=source, resolution=resolution)

def get_copdem_tiles(srtm_tile_ids: List[str], out_dir: Path,
                     source: str = "creodias", resolution='30') -> None:
    """
    Retrieve cop dem data from the source into the output dir
    :param srtm_tile_ids: List of srtm tile ids
    :param out_dirpath: Output directory where the srtm data is downloaded
    :param source: Source where to retrieve the cop dem data
    """
    if source == "creodias":
            logger.info("Use creodias bucket to retrieve cop dem data!")
            CREODIASDataProvider().download_copdem_tiles(srtm_tile_ids, out_dir)
    elif source == "aws":
        logger.info("Use AWS bucket to retrieve cop dem data!")
        AWSDataProvider().download_copdem_tiles(srtm_tile_ids, out_dir,
                                               resolution=resolution)
    else:
        raise ValueError(f"Source {source} not supported!")

def get_copdem_ids(s2_tile_id: str) -> None:
    """
    Get coptem id for an S2 tile
    :param s2 tile_id:
    :return: List of srtm ids
    """
    res = main(s2_tile_id, dem=True, overlap=True, no_l8=True, no_s2=True)
    return list(res[2].id)
