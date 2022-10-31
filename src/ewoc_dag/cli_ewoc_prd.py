# -*- coding: utf-8 -*-
""" CLI to retrieve EO data identifiy by their product ID to EO data provider.
"""
import argparse
import logging
from pathlib import Path
import sys
from tempfile import gettempdir

from ewoc_dag import __version__
from ewoc_dag.bucket.ewoc import EWOCPRDBucket


__author__ = "Mickael Savinaud"
__copyright__ = "Mickael Savinaud"
__license__ = "MIT"

logger = logging.getLogger(__name__)


def get_ewoc_prd(
    bucket_prefix: str,
    out_dirpath: Path = Path(gettempdir()),
) -> None:
    """Get EWoC product from an bucket prefix
    todo
    """

    ewoc_prd_bucket = EWOCPRDBucket()
    ewoc_prd_bucket.download_prd(bucket_prefix, out_dirpath=out_dirpath)


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
    parser.add_argument(dest="bucket_prefix", help="EO product ID", type=str)

    parser.add_argument(
        "-o",
        dest="out_dirpath",
        help="Output Dirpath",
        type=Path,
        default=Path(gettempdir()),
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
        args.bucket_prefix,
        args.out_dirpath,
    )
    get_ewoc_prd(args.bucket_prefix, out_dirpath=args.out_dirpath)
    logger.info("Data are available at %s!", args.out_dirpath)


def run():
    """Calls :func:`main` passing the CLI arguments extracted from :obj:`sys.argv`

    This function can be used as entry point to create console scripts with setuptools.
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
