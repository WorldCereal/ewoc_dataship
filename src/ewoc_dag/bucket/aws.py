# -*- coding: utf-8 -*-
""" AWS pulic EO data bucket management module
"""
import logging
import os
from pathlib import Path
from tempfile import gettempdir
from typing import List

from ewoc_dag.bucket.eobucket import EOBucket
from ewoc_dag.eo_prd_id.l8_prd_id import L8C2PrdIdInfo
from ewoc_dag.eo_prd_id.s1_prd_id import S1PrdIdInfo
from ewoc_dag.eo_prd_id.s2_prd_id import S2PrdIdInfo


logger = logging.getLogger(__name__)


class AWSEOBucket(EOBucket):
    """Base class for EO public data bucket access on AWS"""

    _SUPPORTED_BUCKET = [
        "sentinel-s1-l1c",
        "sentinel-s2-l1c",
        "sentinel-s2-l2a",
        "sentinel-cogs",
        "usgs-landsat",
        "copernicus-dem-30m",
        "copernicus-dem-90m",
    ]

    def __init__(self, arn_suffix: str) -> None:
        """Base contructor for bucket of EO public data on AWS

            To set you AWS crendentials to access to the bucket please check the boto3 documentation

            The more interesting method in the scope of EWoC is to use AWS_ACCESS_KEY_ID and
            AWS_SECRET_ACCESS_KEY env variables.

        Args:
            arn_suffix (str): last part of the arn name of public bucket hosted on AWS
        """

        if (
            os.getenv("AWS_ACCESS_KEY_ID") is None
            or os.getenv("AWS_SECRET_ACCESS_KEY") is None
        ):
            logger.warning(
                "AWS S3 crendentials not set by env variables, \
try to continue with other authentifcation methods!"
            )
        if arn_suffix in self._SUPPORTED_BUCKET:
            super().__init__(arn_suffix)
        else:
            raise ValueError("Bucket is not supported!")


class AWSS1Bucket(AWSEOBucket):
    """Class to handle access to Sentinel-1 data"""

    def __init__(self) -> None:
        super().__init__("sentinel-s1-l1c")

    def download_prd(
        self, prd_id: str, out_dirpath_root: Path = Path(gettempdir())
    ) -> None:
        out_dirpath = out_dirpath_root / prd_id.split(".")[0]
        out_dirpath.mkdir(exist_ok=True)

        s1_prd_info = S1PrdIdInfo(prd_id)

        prd_prefix = (
            "/".join(
                [
                    s1_prd_info.product_type,
                    str(s1_prd_info.start_time.date().year),
                    str(s1_prd_info.start_time.date().month),
                    str(s1_prd_info.start_time.date().day),
                    s1_prd_info.beam_mode,
                    s1_prd_info.polarisation,
                    prd_id.split(".")[0],
                ]
            )
            + "/"
        )
        logger.debug("prd_prefix: %s", prd_prefix)
        return super()._download_prd(prd_prefix, out_dirpath, request_payer=True)


class AWSS2Bucket(AWSEOBucket):
    """Base class to handle access to Sentinel-2 data"""

    def _download_s2_prd(
        self,
        prd_id: str,
        out_dirpath_root: Path,
        l2_mask_only: bool = False,
        l2a_cogs: bool = False,
    ) -> None:
        out_dirpath = out_dirpath_root / prd_id.split(".")[0]
        out_dirpath.mkdir(exist_ok=True)

        s2_prd_info = S2PrdIdInfo(prd_id)
        prefix_components = [
            s2_prd_info.tile_id[0:2],
            s2_prd_info.tile_id[2],
            s2_prd_info.tile_id[3:5],
            str(s2_prd_info.datatake_sensing_start_time.date().year),
            str(s2_prd_info.datatake_sensing_start_time.date().month),
        ]
        if l2a_cogs:
            prefix_components.insert(0, "sentinel-s2-l2a-cogs")
            product_name = "_".join(
                [
                    s2_prd_info.mission_id,
                    s2_prd_info.tile_id,
                    s2_prd_info.datatake_sensing_start_time.date().strftime("%Y%m%d"),
                    "0",
                    "L2A",
                ]
            )
            (out_dirpath / product_name).mkdir(exist_ok=True)
            prefix_components.append(product_name)
            prd_prefix = "/".join(prefix_components) + "/"
            logger.debug("prd_prefix: %s", prd_prefix)

            if l2_mask_only:
                mask_filename = "SCL.tif"
                logging.info(
                    "Try to download %s to %s",
                    prd_prefix + mask_filename,
                    out_dirpath / mask_filename,
                )
                self._s3_client.download_file(
                    Bucket=self._bucket_name,
                    Key=prd_prefix + mask_filename,
                    Filename=str(out_dirpath / mask_filename),
                )
            else:
                self._download_prd(prd_prefix, out_dirpath)
        else:
            prd_prefix = (
                "/".join(
                    [
                        "products",
                        str(s2_prd_info.datatake_sensing_start_time.date().year),
                        str(s2_prd_info.datatake_sensing_start_time.date().month),
                        str(s2_prd_info.datatake_sensing_start_time.date().day),
                        prd_id.split(".")[0],
                    ]
                )
                + "/"
            )
            logger.info("prd_prefix: %s", prd_prefix)

            prefix_components.insert(0, "tiles")
            prefix_components.append(
                str(s2_prd_info.datatake_sensing_start_time.date().day)
            )
            prefix_components.append("0")
            tile_prefix = "/".join(prefix_components) + "/"
            logger.info("tile_prefix: %s", tile_prefix)

            if s2_prd_info.product_level == "L2A":
                bucket_name = "sentinel-s2-l2a"

                if l2_mask_only:
                    mask_filename = "SCL.jp2"
                    logging.info(
                        "Try to download %s to %s",
                        prd_prefix + mask_filename,
                        out_dirpath / mask_filename,
                    )
                    self._s3_client.download_file(
                        Bucket=bucket_name,
                        Key=tile_prefix + "R20m/" + mask_filename,
                        Filename=str(out_dirpath / mask_filename),
                        ExtraArgs=dict(RequestPayer="requester"),
                    )
                else:
                    super()._download_prd(prd_prefix, out_dirpath, request_payer=True)
                    super()._download_prd(tile_prefix, out_dirpath, request_payer=True)
            else:
                super()._download_prd(prd_prefix, out_dirpath, request_payer=True)
                super()._download_prd(tile_prefix, out_dirpath, request_payer=True)


class AWSS2L1CBucket(AWSS2Bucket):
    """Class to handle access to Sentinel-2 L1C data"""

    def __init__(self) -> None:
        super().__init__("sentinel-s2-l1c")

    def download_prd(
        self, prd_id: str, out_dirpath_root: Path = Path(gettempdir())
    ) -> None:
        return super()._download_s2_prd(prd_id, out_dirpath_root)


class AWSS2L2ABucket(AWSS2Bucket):
    """Class to handle access to Sentinel-2 L2A data"""

    def __init__(self) -> None:
        super().__init__("sentinel-s2-l2a")

    def download_prd(
        self,
        prd_id: str,
        out_dirpath_root: Path = Path(gettempdir()),
        l2a_mask_only: bool = False,
    ) -> None:
        return super()._download_s2_prd(
            prd_id, out_dirpath_root, l2_mask_only=l2a_mask_only
        )


class AWSS2L2ACOGSBucket(AWSS2Bucket):
    """Class to handle access to Sentinel-2 L2A COG data"""

    def __init__(self) -> None:
        super().__init__("sentinel-cogs")

    def download_prd(
        self,
        prd_id: str,
        out_dirpath_root: Path = Path(gettempdir()),
        l2a_mask_only: bool = False,
    ) -> None:
        return super()._download_s2_prd(
            prd_id, out_dirpath_root, l2_mask_only=l2a_mask_only, l2a_cogs=True
        )


class AWSS2L8C2Bucket(AWSEOBucket):
    """Class to handle access to Landsatdata"""
    def __init__(self) -> None:
        super().__init__("usgs-landsat")

    def download_prd(
        self, prd_id: str, out_dirpath_root: Path = Path(gettempdir()), filter_bands: list = None
    ) -> None:
        # TODO add mask only mode
        # TODO support level 1? support other collections?
        out_dirpath = out_dirpath_root / prd_id.split(".")[0]
        out_dirpath.mkdir(exist_ok=True)

        l8_prd_info = L8C2PrdIdInfo(prd_id)

        prd_prefix = (
            "/".join(
                [
                    "collection02",
                    "level-2",
                    "standard",
                    "oli-tirs",
                    str(l8_prd_info.acquisition_date.year),
                    l8_prd_info.wrs2_path,
                    l8_prd_info.wrs2_row,
                    prd_id,
                ]
            )
            + "/"
        )
        logger.debug("prd_prefix: %s", prd_prefix)
        return super()._download_prd(prd_prefix, out_dirpath, request_payer=True, filter_bands=filter_bands)


class AWSCopDEMBucket(AWSEOBucket):
    def __init__(self, resolution="1s") -> None:
        if resolution == "1s":
            super().__init__("copernicus-dem-30m")
            self._copdem_prefix = "Copernicus_DSM_COG_10_"
        elif resolution == "3s":
            self._copdem_prefix = "Copernicus_DSM_COG_30_"
            super().__init__("copernicus-dem-90m")
        else:
            ValueError("Resolution of Copernicus DEM is 1s or 3s!")

        self._copdem_suffix = "_00_DEM"

    def download_tiles(
        self,
        copdem_tile_ids: List[str],
        out_dirpath: Path = Path(gettempdir()),
    ) -> None:

        for copdem_tile_id in copdem_tile_ids:

            copdem_tile_id_aws = (
                self._copdem_prefix
                + copdem_tile_id[:3]
                + "_00_"
                + copdem_tile_id[3:]
                + self._copdem_suffix
            )
            copdem_tile_id_filename = copdem_tile_id_aws + ".tif"
            copdem_tile_id_filepath = out_dirpath / copdem_tile_id_filename
            copdem_object_key = copdem_tile_id_aws + "/" + copdem_tile_id_filename
            logger.info(
                "Try to download %s to %s", copdem_object_key, copdem_tile_id_filepath
            )
            self._s3_client.download_file(
                Bucket=self._bucket_name,
                Key=copdem_object_key,
                Filename=str(copdem_tile_id_filepath),
            )

    def _compute_key(self, copdem_tile_id: str) -> str:
        copdem_tile_id_aws = f"{self._copdem_prefix}{copdem_tile_id[:3]}\
_00_{copdem_tile_id[3:]}{self._copdem_suffix}"
        copdem_tile_id_filename = f"{copdem_tile_id_aws}.tif"
        return f"{copdem_tile_id_aws}/{copdem_tile_id_filename}"

    def to_gdal_path(self, copdem_tile_id: str) -> str:
        return f"/vsis3/{self._bucket_name}/{self._compute_key(copdem_tile_id)}"


if __name__ == "__main__":
    import sys

    LOG_FORMAT = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format=LOG_FORMAT,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # TODO: add the possibility to filter the file downloaded: full or subset

    # AWSS1Bucket().download_prd(
    #     "S1B_IW_GRDH_1SSH_20210714T083244_20210714T083309_027787_0350EB_E62C.SAFE"
    # )
    # AWSS2L1CBucket().download_prd(
    #     "S2B_MSIL1C_20210714T235249_N0301_R130_T57KUR_20210715T005654.SAFE"
    # )
    # AWSS2L2ABucket().download_prd(
    #     "S2B_MSIL2A_20210714T131719_N0301_R124_T28WDB_20210714T160455.SAFE"
    # )
    # AWSS2L2ABucket().download_prd(
    #     "S2B_MSIL2A_20210714T131719_N0301_R124_T28WDB_20210714T160455.SAFE",
    #     l2a_mask_only=True,
    # )
    # AWSS2L2ACOGSBucket().download_prd(
    #     "S2B_MSIL2A_20210714T131719_N0301_R124_T28WDB_20210714T160455.SAFE"
    # )
    # AWSS2L2ACOGSBucket().download_prd(
    #     "S2B_MSIL2A_20210714T131719_N0301_R124_T28WDB_20210714T160455.SAFE",
    #     l2a_mask_only=True,
    # )
    # AWSCopDEMBucket().download_tiles(
    #     [
    #         "Copernicus_DSM_COG_10_S90_00_W157_00_DEM",
    #         "Copernicus_DSM_COG_10_S90_00_W156_00_DEM",
    #     ]
    # )
    # AWSS2L8C2Bucket().download_prd("LC08_L2SP_227099_20211017_20211026_02_T2")
