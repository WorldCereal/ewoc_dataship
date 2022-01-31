# -*- coding: utf-8 -*-
""" Creodias DIAS bucket management module
"""
import logging
import zipfile
from pathlib import Path
from tempfile import gettempdir
from typing import List

from ewoc_dag.bucket.eobucket import EOBucket
from ewoc_dag.eo_prd_id.s1_prd_id import S1PrdIdInfo
from ewoc_dag.eo_prd_id.s2_prd_id import S2PrdIdInfo

logger = logging.getLogger(__name__)


class CreodiasBucket(EOBucket):
    """Class to handle access to Sentinel-1 data from Creodias DIAS bucket.
    cf https://creodias.eu/faq-s3 for more information"""

    _CREODIAS_BUCKET_FORMAT_PREFIX = "/%Y/%m/%d/"

    def __init__(self) -> None:
        """Constructor of the DIAS bucket manager on Creodias

        Raises:
            ValueError: if the DIAS bucket is not accessible and functional
        """
        super().__init__(
            "DIAS",
            s3_access_key_id="anystring",
            s3_secret_access_key="anystring",
            endpoint_url="http://data.cloudferro.com",
        )

        if not self._check_bucket():
            raise ValueError("Creodias DIAS bucket not correctly intialized!")

        logger.info("Creodias DIAS bucket ready to use!")

    def download_s1_prd(
        self, prd_id: str, out_dirpath: Path = Path(gettempdir())
    ) -> Path:
        """Download Sentinel-1 product from DIAS bucket

        Args:
            prd_id (str): Sentinel-1 product id
            out_dirpath (Path, optional): Path where to write the product

        Returns:
            Path: Path to the S1 product
        """
        out_dirpath = out_dirpath / prd_id.split(".")[0]
        out_dirpath.mkdir(exist_ok=True)
        s1_prd_info = S1PrdIdInfo(prd_id)
        s1_bucket_prefix = "Sentinel-1/SAR/"
        prd_prefix = (
            s1_bucket_prefix
            + s1_prd_info.product_type
            + s1_prd_info.start_time.date().strftime(
                self._CREODIAS_BUCKET_FORMAT_PREFIX
            )
            + prd_id
            + "/"
        )
        self._download_prd(prd_prefix, out_dirpath)
        return out_dirpath

    def download_s2_prd(
        self,
        prd_id: str,
        out_dirpath: Path = Path(gettempdir()),
        l2_mask_only: bool = False,
    ) -> Path:
        """Download Sentinel-2 product from DIAS bucket

        Args:
            prd_id (str): Sentinel-2 product id
            out_dirpath (Path, optional): Path where to write the product.
             Defaults to Path(gettempdir()).
            l2_mask_only (bool, optional): Retrieve only the mask from the product.
             Defaults to False.
             Used only for L2 product
        Returns:
            Path: Path to the S2 product
        """
        out_dirpath = out_dirpath / prd_id
        out_dirpath.mkdir(exist_ok=True)
        s2_prd_info = S2PrdIdInfo(prd_id)
        s2_bucket_prefix = "Sentinel-2/MSI/"
        prd_prefix = (
            s2_bucket_prefix
            + s2_prd_info.product_level
            + s2_prd_info.datatake_sensing_start_time.date().strftime(
                self._CREODIAS_BUCKET_FORMAT_PREFIX
            )
            + prd_id
            + "/"
        )
        if not l2_mask_only:
            self._download_prd(prd_prefix, out_dirpath)
        else:
            if s2_prd_info.product_level == "L2A":
                # TODO support mask only mode: issue with one item of the mask key #51
                logger.error(
                    "Download S2 L2A SCL mask from Creodias bucket is not currently supported!"
                )
                # /GRANULE/L2A_T28WDB_A022744_20210714T131716/IMG_DATA/R20m/T28WDB_20210714T131719_SCL_20m.jp2
                mask_key_filename = f"T{s2_prd_info.tile_id}_{s2_prd_info.datatake_sensing_start_time}_SCL_20m.jp2"
                orbit_direction = "A"  # always ascending
                orbit_number = (
                    "todo"  # could be found in manifest.safe key= safe:orbitNumber
                )
                mask_key = f"{prd_prefix}GRANULE/L2A_T{s2_prd_info.tile_id}_{orbit_direction}{orbit_number}_{s2_prd_info.datatake_sensing_start_time}/IMG_DATA/R20m/{mask_key_filename}"
                mask_filepath = out_dirpath / mask_key_filename
                logging.info(
                    "Try to download %s to %s",
                    f"{mask_key}",
                    out_dirpath / mask_key_filename,
                )
                self._s3_client.download_file(
                    Bucket=self._bucket_name, Key=mask_key, Filename=str(mask_filepath)
                )
            else:
                logger.error("Not possible to download L2A mask from a L1C product ID!")

        return out_dirpath

    def download_srtm1s_tiles(
        self, srtm_tile_ids: List[str], out_dirpath: Path = Path(gettempdir())
    ):
        """Download srtm 1s tiles from DIAS bucket

        Args:
            srtm_tile_ids (List[str]): list of srtm 1s tile id
            out_dirpath (Path, optional): Directory where to write the srtm 1s tiles.
                Defaults to Path(gettempdir()).
        """

        srtm_prefix = "auxdata/SRTMGL1/dem/"
        for srtm_tile_id in srtm_tile_ids:
            srtm_tile_id_filename = srtm_tile_id + ".SRTMGL1.hgt.zip"
            srtm_tile_id_filepath = out_dirpath / srtm_tile_id_filename
            srtm_object_key = srtm_prefix + srtm_tile_id_filename
            logger.info(srtm_object_key)
            self._s3_client.download_file(
                Bucket=self._bucket_name,
                Key=srtm_object_key,
                Filename=str(srtm_tile_id_filepath),
            )

            with zipfile.ZipFile(srtm_tile_id_filepath, "r") as srtm_zipfile:
                srtm_zipfile.extractall(out_dirpath)

            srtm_tile_id_filepath.unlink()

    def download_copdem_tiles(
        self,
        copdem_tiles_id: List[str],
        out_dirpath: Path = Path(gettempdir()),
        resolution="1s",
    ) -> None:
        """Download copdem tiles from DIAS bucket

        Args:
            copdem_tiles_id (List[str]): [description]
            out_dirpath (Path, optional): [description]. Defaults to Path(gettempdir()).
            resolution (str, optional): [description]. Defaults to "1s".
        """
        # TODO support copdem retrieval #52
        raise NotImplementedError("Currently not supported!")


if __name__ == "__main__":
    creo_bucket = CreodiasBucket()
    creo_bucket.download_s2_prd(
        "S2B_MSIL1C_20210714T235249_N0301_R130_T57KUR_20210715T005654.SAFE"
    )
    creo_bucket.download_s2_prd(
        "S2B_MSIL2A_20210714T131719_N0301_R124_T28WDB_20210714T160455.SAFE"
    )
    creo_bucket.download_s2_prd(
        "S2B_MSIL2A_20210714T131719_N0301_R124_T28WDB_20210714T160455.SAFE",
        l2_mask_only=True,
    )
    creo_bucket.download_s1_prd(
        "S1B_IW_GRDH_1SSH_20210714T083244_20210714T083309_027787_0350EB_E62C.SAFE"
    )
