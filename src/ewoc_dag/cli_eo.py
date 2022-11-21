# -*- coding: utf-8 -*-
""" CLI to retrieve EO data identifiy by their product ID to EO data provider.
"""
import argparse
import logging
from pathlib import Path
import sys
from tempfile import gettempdir

from ewoc_dag import __version__
from ewoc_dag.eo_prd_id.l8_prd_id import L8C2PrdIdInfo
from ewoc_dag.eo_prd_id.s1_prd_id import S1PrdIdInfo
from ewoc_dag.eo_prd_id.s2_prd_id import S2PrdIdInfo
from ewoc_dag.l8c2l2_dag import get_l8c2l2_product, _L8C2_SOURCES
from ewoc_dag.s1_dag import S1DagError, get_s1_product
from ewoc_dag.s2_dag import get_s2_product
from typing import Optional


__author__ = "Mickael Savinaud"
__copyright__ = "Mickael Savinaud"
__license__ = "MIT"

logger = logging.getLogger(__name__)


class EwocEODagException(Exception):
    """Base Class for ewoc_dag package"""

    def __init__(self, error=None):
        self._error = error
        self._message = "EwoC EO DAG error:"
        super().__init__(self._message)

    def __str__(self):
        return f"{self._message} {self._error}"


def get_eo_data(
    prd_id: str,
    out_dirpath: Path = Path(gettempdir()),
    eo_data_source: str = "eodag",
    eodata_config_filepath: Optional[Path] = None,
    only_l2a_mask: bool = False,
    use_s2_cogs: bool = False,
    to_safe: bool = False,
) -> None:
    """Retrieve EO data from the product ID

    Args:
        prd_id (str): Product ID
        out_dirpath (Path, optional): Directory to write the product. Defaults to
            Path(gettempdir()).
        eo_data_source (str, optional): Data provider. Defaults to "creodias".
        eodata_config_filepath (Path, optional): EODAG configuration file. Defaults to None.
        only_l2a_mask (bool, optional): For S2 L2A products retrieve only mask . Defaults to False.
        use_s2_cogs (bool, optional): Force to use Sentinel-2 L2A COGS bucket. Defaults to False.
    """
    if L8C2PrdIdInfo.is_valid(prd_id):
        get_l8c2l2_product(
            prd_id,
            out_dirpath,
            source=_L8C2_SOURCES[1],
            eodag_config_file=eodata_config_filepath,
        )
    elif S1PrdIdInfo.is_valid(prd_id):
        try:
            get_s1_product(
                prd_id,
                out_dirpath,
                source=eo_data_source,
                eodag_config_file=eodata_config_filepath,
                safe_format=to_safe,
            )
        except S1DagError as exc:
            logger.error(exc)
            raise EwocEODagException(exc) from exc
    elif S2PrdIdInfo.is_valid(prd_id):
        get_s2_product(
            prd_id,
            out_dirpath,
            source=eo_data_source,
            eodag_config_file=eodata_config_filepath,
            l2_mask_only=only_l2a_mask,
            aws_l2a_cogs=use_s2_cogs,
        )


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
    parser.add_argument(dest="prd_ids", help="EO product ID")
    parser.add_argument(
        "-o",
        dest="out_dirpath",
        help="Output Dirpath",
        type=Path,
        default=Path(gettempdir()),
    )
    parser.add_argument(
        "--data-source",
        dest="data_source",
        help="Source of the EO data",
        type=str,
        default="eodag",
    )
    parser.add_argument(
        "--to-safe",
        dest="to_safe",
        help="Convert to SAFE format (for AWS S2 L1C and S1 L1 GRD data)",
        action="store_true",
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
    logger.info(
        "Start retrieve %s from %s to %s !",
        args.prd_ids,
        args.data_source,
        args.out_dirpath,
    )
    try:
        get_eo_data(
            args.prd_ids,
            args.out_dirpath,
            eo_data_source=args.data_source,
            to_safe=args.to_safe,
        )
    except EwocEODagException as exc:
        logger.error(exc)
        sys.exit(2)
    except BaseException as err:
        logger.error(f"Unexpected {err=}, {type(err)=}")
        sys.exit(1)
    else:
        logger.info("Data %s are available at %s!", args.prd_ids, args.out_dirpath)


def run():
    """Calls :func:`main` passing the CLI arguments extracted from :obj:`sys.argv`

    This function can be used as entry point to create console scripts with setuptools.
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
