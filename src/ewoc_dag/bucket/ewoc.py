# -*- coding: utf-8 -*-
""" EWoC private data bucket management module
"""
import logging
import os
import zipfile
from datetime import datetime
from distutils.util import strtobool
from pathlib import Path
from tempfile import gettempdir
from typing import List, Tuple

import pandas as pd

from ewoc_dag.bucket.eobucket import EOBucket
from ewoc_dag.eo_prd_id.ewoc_prd_id import (
    EwocArdPrdIdInfo,
    EwocS1ArdPrdIdInfo,
    EwocTirArdPrdIdInfo,
)

_logger = logging.getLogger(__name__)


def split_tile_id(tile_id: str) -> Tuple[str, str, str]:
    """Split the S2 Tile ID into MGRS parts

    Args:
        tile_id (str): S2 MGRS tile ID

    Raises:
        ValueError: if the tile ID is not correct

    Returns:
        Tuple[str]: Part of the tile ID (exemple for 31TCJ: {31, TC, J})
    """
    if len(tile_id) == 5:
        return tile_id[:2], tile_id[2], tile_id[3:]

    if len(tile_id) == 4:
        return tile_id[0], tile_id[1], tile_id[2:]

    raise ValueError(f"Tile ID {tile_id} is not valid !")


def tileid_to_ard_path_component(tile_id: str) -> str:
    """Part of the ARD key related to S2 Tile ID

    Args:
        tile_id (str): S2 MGRS tile ID

    Returns:
        str: tile ID converted to ARD parth component (exemple for 31TCJ: "31/TC/J")
    """
    part1, part2, part3 = split_tile_id(tile_id)
    return f"{part1}/{part2}/{part3}"


class EWOCBucket(EOBucket):
    """Base class for EWoC data buckets"""

    _CREODIAS_EWOC_ENDPOINT_URL = "https://s3.waw2-1.cloudferro.com"

    def __init__(self, bucket_name: str) -> None:

        ewoc_access_key_id = os.getenv("EWOC_S3_ACCESS_KEY_ID")
        ewoc_secret_access_key_id = os.getenv("EWOC_S3_SECRET_ACCESS_KEY")

        ewoc_cloud_provider = os.getenv("EWOC_CLOUD_PROVIDER", "CREODIAS")
        if ewoc_cloud_provider == "creodias":
            ewoc_endpoint_url = self._CREODIAS_EWOC_ENDPOINT_URL
        elif ewoc_cloud_provider == "aws":
            ewoc_endpoint_url = None
        else:
            raise ValueError(f"Cloud provider {ewoc_cloud_provider} not supported!")

        super().__init__(
            bucket_name,
            s3_access_key_id=ewoc_access_key_id,
            s3_secret_access_key=ewoc_secret_access_key_id,
            endpoint_url=ewoc_endpoint_url,
        )

        if not self._check_bucket():
            raise ValueError(f"EWoC {bucket_name} not correctly intialized!")

        _logger.info(
            "EWoC bucket %s is hosted on %s and functional",
            bucket_name,
            ewoc_cloud_provider,
        )

    def _list_prds_key(self, tile_prefix):
        prds_key = set()

        kwargs = {"Bucket": self._bucket_name, "Prefix": tile_prefix, "MaxKeys": 1000}
        while True:
            resp = self._s3_client.list_objects_v2(**kwargs)

            for obj in resp.get("Contents"):
                _logger.debug("obj.key: %s", obj["Key"])
                prd_path = Path("/" + obj["Key"])
                _logger.debug(prd_path)
                prds_key.add(str(prd_path.parent))

            try:
                kwargs["ContinuationToken"] = resp["NextContinuationToken"]
            except KeyError:
                break

        return prds_key


class EWOCAuxDataBucket(EWOCBucket):
    """Class to handle access to EWoC Auxiliary datas"""

    def __init__(self) -> None:
        super().__init__("ewoc-aux-data")

    def download_srtm3s_tiles(
        self, srtm_tile_ids: List[str], out_dirpath: Path = Path(gettempdir())
    ) -> None:
        """Download srtm 3s (90m) tiles according a S2 tile ID

        Args:
            srtm_tile_ids (List[str]): List of S2 MGRS tile ID
            out_dirpath (Path, optional): Output directry to write SRTM tiles.
                Defaults to Path(gettempdir()).
        """

        for srtm_tile_id in srtm_tile_ids:
            srtm_tile_id_filename = srtm_tile_id + ".zip"
            srtm_tile_id_filepath = out_dirpath / srtm_tile_id_filename
            srtm_object_key = "srtm90/" + srtm_tile_id_filename
            _logger.info(
                "Try to download %s to %s", srtm_object_key, srtm_tile_id_filepath
            )
            self._s3_client.download_file(
                Bucket=self._bucket_name,
                Key=srtm_object_key,
                Filename=str(srtm_tile_id_filepath),
            )

            with zipfile.ZipFile(srtm_tile_id_filepath, "r") as srtm_zipfile:
                srtm_zipfile.extractall(out_dirpath / "srtm3s")

            srtm_tile_id_filepath.unlink()

    def _list_agera5_prd(self) -> List[str]:
        """list all AgERA5 prodcuts inside the AUX data bucket

        Returns:
            List[str]: list of AgERA5 products in the bucket
        """
        return self._list_prds_key("AgERA5/")

    def agera5_to_satio_csv(
        self, filepath: Path = Path(gettempdir()) / "satio_agera5.csv"
    ) -> None:
        """Write SatIO Collection file for AgERA5

        Args:
            filepath (Path, optional): SatIO Collection output filepath.
                Defaults to Path(gettempdir())/"satio_agera5.csv".
        """

        agera5_paths = []
        agera5_dates = []
        agera5_products = []
        for agera5_dir in self._list_agera5_prd():
            agera5_date = agera5_dir.split("/")[3]
            agera5_path = f"{self._s3_basepath()}{agera5_dir}"
            agera5_paths.append(agera5_path)
            agera5_dates.append(
                datetime.strptime(agera5_date, "%Y%m%d").strftime("%Y-%m-%d")
            )
            agera5_products.append("AgERA5_" + agera5_date)

        pd.DataFrame(
            {
                "product": agera5_products,
                "date": agera5_dates,
                "path": agera5_paths,
                "tile": "global",
                "epsg": "4326",
            }
        ).to_csv(filepath)

    def upload_agera5_prd(self):
        raise NotImplementedError("Currently not implemented")


class EWOCARDBucket(EWOCBucket):
    """Class to handle access of EWoC ARD data bucket"""

    def __init__(self, ewoc_dev_mode=None) -> None:
        if ewoc_dev_mode is None:
            ewoc_dev_mode = strtobool(os.getenv("EWOC_DEV_MODE", "False"))
        if not ewoc_dev_mode:
            super().__init__("ewoc-ard")
        elif ewoc_dev_mode:
            super().__init__("ewoc-ard-dev")

    def sar_to_satio_csv(
        self,
        tile_id: str,
        production_id: str,
        filepath: Path = Path(gettempdir()) / "satio_sar.csv",
    ) -> None:
        """Generate SatIO Collection file for SAR products

        Args:
            tile_id (str): S2 MGRS tile id
            production_id (str): production ID related to Workplan
            filepath (Path, optional): Filepath of the output file for satio.
                Defaults to Path(gettempdir())/"satio_tir.csv".
        """
        prds_path = []
        prds_datetime = []

        for prd_key in self._list_prds_key(
            f"{production_id}/SAR/{tileid_to_ard_path_component(tile_id)}"
        ):
            prd_info = EwocS1ArdPrdIdInfo(prd_key.split("/")[8])
            prds_path.append(f"{self._s3_basepath()}{prd_key}")
            prds_datetime.append(prd_info.acquisition_datetime)

        pd.DataFrame(
            {
                "date": prds_datetime,
                "tile": tile_id,
                "level": "SIGMA0",
                "path": prds_path,
            }
        ).to_csv(filepath)

    def optical_to_satio_csv(
        self,
        tile_id: str,
        production_id: str,
        filepath: Path = Path(gettempdir()) / "satio_optical.csv",
    ) -> None:
        """Generate SatIO Collection file for Optical products

        Args:
            tile_id (str): S2 MGRS tile id
            production_id (str): production ID related to Workplan
            filepath (Path, optional): Filepath of the output file for satio.
                Defaults to Path(gettempdir())/"satio_tir.csv".
        """
        prds_path = []
        prds_datetime = []

        for prd_key in self._list_prds_key(
            f"{production_id}/OPTICAL/{tileid_to_ard_path_component(tile_id)}"
        ):
            prd_info = EwocArdPrdIdInfo(prd_key.split("/")[8])
            prds_path.append(f"{self._s3_basepath()}{prd_key}")
            prds_datetime.append(prd_info.acquisition_datetime)

        pd.DataFrame(
            {"date": prds_datetime, "tile": tile_id, "level": "SMAC", "path": prds_path}
        ).to_csv(filepath)

    def tir_to_satio_csv(
        self,
        tile_id: str,
        production_id: str,
        filepath: Path = Path(gettempdir()) / "satio_tir.csv",
    ) -> None:
        """Generate SatIO Collection file for TIR products

        Args:
            tile_id (str): S2 MGRS tile id
            production_id (str): production ID related to Workplan
            filepath (Path, optional): Filepath of the output file for satio.
                Defaults to Path(gettempdir())/"satio_tir.csv".
        """
        prds_path = []
        prds_datetime = []

        for prd_key in self._list_prds_key(
            f"{production_id}/TIR/{tileid_to_ard_path_component(tile_id)}"
        ):
            prd_info = EwocTirArdPrdIdInfo(prd_key.split("/")[8])
            prds_path.append(f"{self._s3_basepath()}{prd_key}")
            prds_datetime.append(prd_info.acquisition_datetime)

        pd.DataFrame(
            {"date": prds_datetime, "tile": tile_id, "level": "L2SP", "path": prds_path}
        ).to_csv(filepath)

    def upload_ard_prd(
        self, ard_prd_path: Path, ard_prd_prefix: str
    ) -> Tuple[int, float]:
        """Upload EWoC ARD tif files to EWoC ARD bucket

        Args:
            ard_prd_path (Path): Path to the directory which contain ARD data
            ard_prd_prefix (str): Bucket prefix where store data
        """
        return super()._upload_prd(ard_prd_path, ard_prd_prefix)

    def upload_ard_raster(self, ard_raster_path: Path, ard_raster_key: str) -> int:
        """Upload EWoC ARD raster individually to EWoC ARD bucket

        Args:
            ard_raster_path (Path): Path to the ard raster to upload
            ard_raster_key (str): Key where to upload the ard raster
        """
        return super()._upload_file(ard_raster_path, ard_raster_key)


class EWOCPRDBucket(EWOCBucket):
    """Class to handle access of EWoC final products"""

    def __init__(self, ewoc_dev_mode=None) -> None:

        if ewoc_dev_mode is None:
            ewoc_dev_mode = strtobool(os.getenv("EWOC_DEV_MODE", "False"))
        if not ewoc_dev_mode:
            super().__init__("ewoc-prd")
        elif ewoc_dev_mode:
            super().__init__("ewoc-prd-dev")

    def upload_ewoc_prd(self, prd_path: Path, prd_prefix: str) -> None:
        """Upload EWoC product

        Args:
            prd_path (Path): Path to the product to upload
            prd_prefix (str): Product prefix where to put the product
        """
        super()._upload_prd(prd_path, prd_prefix, file_suffix=None)


if __name__ == "__main__":
    import sys

    _LOGFORMAT = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format=_LOGFORMAT,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ewoc_auxdata_bucket = EWOCAuxDataBucket()
    ewoc_auxdata_bucket.download_srtm3s_tiles(["srtm_01_16", "srtm_01_21"])

    ewoc_auxdata_bucket.agera5_to_satio_csv()

    ewoc_ard_bucket = EWOCARDBucket(ewoc_dev_mode=True)
    _logger.info(
        ewoc_ard_bucket.upload_ard_raster(Path("/tmp/upload.file"), "test.file")
    )

    ewoc_ard_bucket.upload_ard_prd(Path("/tmp/upload_test_dir"), "test_up_dir")

    ewoc_ard_bucket.sar_to_satio_csv("31TCJ", "0000_0_09112021223005")
    ewoc_ard_bucket.optical_to_satio_csv("31TCJ", "0000_0_09112021223005")
    ewoc_ard_bucket.tir_to_satio_csv("31TCJ", "0000_0_09112021223005")
