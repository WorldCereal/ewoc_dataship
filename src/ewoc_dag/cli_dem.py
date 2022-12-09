""" CLI to retrieve DEM data identify by their product ID to EO data provider.
"""
import argparse
import logging
from pathlib import Path
import sys
import tempfile

from ewoc_dag import __version__
from ewoc_dag.copdem_dag import get_copdem_from_s2_tile_id
from ewoc_dag.srtm_dag import get_srtm_from_s2_tile_id


__author__ = "Mickael Savinaud"
__copyright__ = "Mickael Savinaud"
__license__ = "MIT"

logger = logging.getLogger(__name__)


def get_dem_data(
    s2_tile_id: str,
    out_dirpath: Path = Path(tempfile.gettempdir()),
    dem_source: str = "esa",
    dem_type: str = "srtm",
    dem_resolution: str = "1s",
    to_sen2cor: bool = False,
) -> None:
    """Retrieve the dem data from the S2 tile ID

    Args:
        s2_tile_id (str): S2 MGRS tile id
        out_dirpath (Path, optional): Output directory where the dem tile will be write.
            Defaults to Path(tempfile.gettempdir()).
        dem_source (str, optional): Source where to retrieve data. Defaults to "esa".
        dem_type (str, optional): Currently support COP DEM and SRTM. Defaults to "srtm".
        dem_resolution (str, optional): Resolution to retrieve. Defaults to "1s".
        to_sen2cor (bool, optional): If true, rename copdem files to match Sen2Cor expectations.

    Raises:
        ValueError: Raise if the source is not supported
    """
    if dem_type == "srtm":
        get_srtm_from_s2_tile_id(
            s2_tile_id, out_dirpath, source=dem_source, resolution=dem_resolution
        )
    elif dem_type == "copdem":
        get_copdem_from_s2_tile_id(
            s2_tile_id,
            out_dirpath,
            source=dem_source,
            resolution=dem_resolution,
            to_sen2cor=to_sen2cor,
        )
    else:
        raise ValueError("DEM type different than srtm, copdem is not possible!")


# ---- CLI ----
# The functions defined in this section are wrappers around the main Python
# API allowing them to be called directly from the terminal as a CLI
# executable/script.


def parse_args(args):
    """Parse command line parameters

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--help"]``).

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(description="Get EO data for EWoC")
    parser.add_argument(
        "--version",
        action="version",
        version=f"ewoc_dag {__version__}",
    )
    parser.add_argument(dest="s2_tile_id", help="Sentinel-2 tile ID")
    parser.add_argument(
        "-o",
        dest="out_dirpath",
        help="Output Dirpath",
        type=Path,
        default=Path(tempfile.gettempdir()),
    )
    parser.add_argument(
        "--dem-source",
        dest="dem_source",
        help="Source of the EO data",
        type=str,
        default="esa",
    )
    parser.add_argument(
        "--dem-type",
        dest="dem_type",
        help="Type of the DEM data",
        type=str,
        default="srtm",
    )
    parser.add_argument(
        "--resolution",
        dest="resolution",
        help="Resolution of the DEM data",
        type=str,
        default="1s",
    )
    parser.add_argument(
        "--to-sen2cor",
        action="store_true",
        help="Rename copdem files for Sen2Cor",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO,
    )
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG,
    )
    return parser.parse_args(args)


def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )


def main(args):
    """Wrapper allowing :func:`generate_s1_ard` to be called with string arguments in a CLI fashion

    Instead of returning the value from :func:`fib`, it prints the result to the
    ``stdout`` in a nicely formatted message.

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--verbose", "42"]``).
    """
    args = parse_args(args)
    setup_logging(args.loglevel)
    # logger.info("Start retrieve %s from %s to %s !",
    #            args.prd_ids, args.data_source, args.out_dirpath)
    get_dem_data(
        args.s2_tile_id,
        args.out_dirpath,
        dem_source=args.dem_source,
        dem_type=args.dem_type,
        dem_resolution=args.resolution,
        to_sen2cor=args.to_sen2cor,
    )
    # logger.info("Data are available at %s!", args.out_dirpath)


def run():
    """Calls :func:`main` passing the CLI arguments extracted from :obj:`sys.argv`

    This function can be used as entry point to create console scripts with setuptools.
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
