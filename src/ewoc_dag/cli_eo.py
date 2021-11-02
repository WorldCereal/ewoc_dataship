
import argparse
import logging
from pathlib import Path
import sys
import tempfile

from ewoc_dag import __version__
from ewoc_dag.eo_prd_id.l8_prd_id import L8C2PrdIdInfo
from ewoc_dag.eo_prd_id.s1_prd_id import S1PrdIdInfo
from ewoc_dag.eo_prd_id.s2_prd_id import S2PrdIdInfo
from ewoc_dag.landsat8 import get_l8_product
from ewoc_dag.s1_dag import get_s1_product
from ewoc_dag.s2_dag import get_s2_product


__author__ = "Mickael Savinaud"
__copyright__ = "Mickael Savinaud"
__license__ = "MIT"

logger = logging.getLogger(__name__)

def get_eo_data(prd_id:str, out_dirpath:Path=Path(tempfile.gettempdir()),
             eo_data_source:str='creodias',
             eodata_config_filepath=None,
             only_l2a_mask=False,
             use_s2_cogs=False)-> None:

    if L8C2PrdIdInfo.is_valid(prd_id):
        get_l8_product(prd_id, out_dirpath,
                       source=eo_data_source,
                       eodag_config_file=eodata_config_filepath,
                       l2_mask_only=only_l2a_mask)
    elif S1PrdIdInfo.is_valid(prd_id):
        get_s1_product(prd_id, out_dirpath,
                       source=eo_data_source,
                       eodag_config_file=eodata_config_filepath)
    elif S2PrdIdInfo.is_valid(prd_id):
        get_s2_product(prd_id, out_dirpath,
                       source=eo_data_source,
                       eodag_config_file=eodata_config_filepath,
                       l2_mask_only=only_l2a_mask,
                       aws_l2a_cogs=use_s2_cogs)


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
    parser.add_argument("-o",dest="out_dirpath", help="Output Dirpath",
                        type=Path,
                        default=Path(tempfile.gettempdir()))
    parser.add_argument("--data-source", dest="data_source",
                        help= 'Source of the EO data',
                        type=str,
                        default='creodias')
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
    logger.info("Start retrieve %s from %s to %s !",
                args.prd_ids, args.data_source, args.out_dirpath)
    get_eo_data(args.prd_ids, args.out_dirpath,
                eo_data_source=args.data_source)
    logger.info("Data are available at %s!", args.out_dirpath)


def run():
    """Calls :func:`main` passing the CLI arguments extracted from :obj:`sys.argv`

    This function can be used as entry point to create console scripts with setuptools.
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()