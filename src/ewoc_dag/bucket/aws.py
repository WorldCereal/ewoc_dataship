# -*- coding: utf-8 -*-
""" AWS pulic EO data bucket management module
"""
import logging
import os
from pathlib import Path
import shutil
from tempfile import gettempdir
from typing import List

from ewoc_dag.bucket.eobucket import EOBucket, EOBucketException
from ewoc_dag.eo_prd_id.l8_prd_id import L8C2PrdIdInfo
from ewoc_dag.eo_prd_id.s1_prd_id import S1PrdIdInfo
from ewoc_dag.eo_prd_id.s2_prd_id import S2PrdIdInfo
from ewoc_dag.safe_format import S1SafeConversionError, aws_to_safe

logger = logging.getLogger(__name__)


class AWSDownloadError(Exception):
    """Exception raised for errors in the S1 SAFE conversion format on AWS."""

    def __init__(self, error=None):
        self._error = error
        self._message = "Error while downloading from AWS:"
        super().__init__(self._message)

    def __str__(self):
        return f"{self._message} {self._error}"


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
    """Class to handle access to Sentinel-1 data from AWS open data bucket:
    https://registry.opendata.aws/sentinel-1/
    """

    def __init__(self) -> None:
        super().__init__("sentinel-s1-l1c")

    def download_prd(
        self,
        prd_id: str,
        out_dirpath_root: Path = Path(gettempdir()),
        safe_format: bool = False,
    ) -> Path:
        """Download S1 products from AWS open data bucket: https://registry.opendata.aws/sentinel-1/

        Args:
            prd_id (str): Sentinel-1 product ID (with or whitout SAFE extension)
            out_dirpath_root (Path, optional): Directory where to write the product.\
                 Defaults to Path(gettempdir()).
            safe_format (bool, optional): Convert to ESA SAFE format. Defaults to False.

        Returns:
            Path: Directory where product is written. The directory is suffixed by .SAFE\
                when the safe format option is set to True
        """
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
        try:
            super()._download_prd(prd_prefix, out_dirpath, request_payer=True)
        except EOBucketException as exc:
            logger.error(exc)
            out_dirpath.rmdir()
            raise AWSDownloadError(exc) from exc

        if safe_format:
            try:
                return aws_to_safe(out_dirpath, prd_id)
            except S1SafeConversionError as exc:
                shutil.rmtree(out_dirpath)
                logger.error(exc)
                raise AWSDownloadError(exc) from exc

        return out_dirpath


class AWSS2Bucket(AWSEOBucket):
    """Class to handle access to Sentinel-2 data from AWS open data buckets:
    - https://registry.opendata.aws/sentinel-2/ for L1C and L2A data
    - https://registry.opendata.aws/sentinel-2-l2a-cogs/ for L2A data
    """

    def _download_s2_prd(
        self,
        prd_id: str,
        out_dirpath_root: Path = Path(gettempdir()),
        l2_mask_only: bool = False,
        l2a_cogs: bool = False,
        prd_items: List[str] = None,
    ) -> Path:
        """Download S2 product according to the product ID from the AWS buckets:
            - https://registry.opendata.aws/sentinel-2/ for L1C and L2A data
            - https://registry.opendata.aws/sentinel-2-l2a-cogs/ for L2A data

        Args:
            prd_id (str): Sentinel-2 product ID
            out_dirpath_root (Path, optional): Path where to write the product.
             Defaults to Path(gettempdir()).
            l2_mask_only (bool, optional): Retrieve only the mask from the product.
             Defaults to False.
             Used only for L2 product
            l2a_cogs (bool, optional): Use AWS bucket which provided COG products.
             Defaults to False.
            prd_items (List[str], optional): Applies a filter on which bands to download

        Returns:
            Path: Path to the S2 product
        """
        out_dirpath = out_dirpath_root / prd_id.split(".")[0]
        out_prod = out_dirpath / "product"
        out_tile = out_dirpath / "tile"
        out_dirpath.mkdir(exist_ok=True)
        out_prod.mkdir(exist_ok=True)
        out_tile.mkdir(exist_ok=True)

        s2_prd_info = S2PrdIdInfo(prd_id)
        prefix_components = [
            s2_prd_info.tile_id[0:2],
            s2_prd_info.tile_id[2],
            s2_prd_info.tile_id[3:5],
            str(s2_prd_info.datatake_sensing_start_time.date().year),
            str(s2_prd_info.datatake_sensing_start_time.date().month),
        ]
        # Remove leading zero from tile id
        prefix_components[0] = prefix_components[0].lstrip("0")
        if l2a_cogs:
            prefix_components.insert(0, "sentinel-s2-l2a-cogs")
            products_path = "/".join(prefix_components) + "/"
            product_date = s2_prd_info.datatake_sensing_start_time.date().strftime(
                "%Y%m%d"
            )
            folder_list = self._list_folders(products_path, request_payer=False)
            product_name = self._find_product(folder_list, product_date)
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

            products_path = "/".join(prefix_components) + "/"
            folder_list = self._list_folders(products_path, request_payer=True)
            folder_number = self._find_aws_folder_number(folder_list)
            prefix_components.append(folder_number)

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
                    super()._download_prd(prd_prefix, out_prod, request_payer=True, prd_items=prd_items)
                    super()._download_prd(tile_prefix, out_tile, request_payer=True, prd_items=prd_items)
            else:
                super()._download_prd(prd_prefix, out_tile, request_payer=True)
                super()._download_prd(tile_prefix, out_prod, request_payer=True)
        return out_dirpath


class AWSS2L1CBucket(AWSS2Bucket):
    """Class to handle access to Sentinel-2 L1C data from AWS open data
    bucket: https://registry.opendata.aws/sentinel-2/"""

    def __init__(self) -> None:
        super().__init__("sentinel-s2-l1c")

    def download_prd(
        self,
        prd_id: str,
        out_dirpath_root: Path = Path(gettempdir()),
        safe_format=False,
    ) -> Path:
        """Download S2 L1C product according to the product ID from the
         bucket: https://registry.opendata.aws/sentinel-2/

        Args:
            prd_id (str): Sentinel-2 L1C product ID
            out_dirpath_root (Path, optional): Path where to write the product.
             Defaults to Path(gettempdir()).
            safe_format (bool, optional): Translate to SAFE format. Defaults to False.

        Returns:
            Path: Path to the Sentinel-2 L1C product
        """

        out_dirpath = super()._download_s2_prd(prd_id, out_dirpath_root)

        safe_format = True
        if safe_format:
            return aws_to_safe(out_dirpath, prd_id)

        return out_dirpath


class AWSS2L2ABucket(AWSS2Bucket):
    """Class to handle access to Sentinel-2 L2A data from AWS open data
    bucket: https://registry.opendata.aws/sentinel-2/"""

    def __init__(self) -> None:
        super().__init__("sentinel-s2-l2a")

    def download_prd(
        self,
        prd_id: str,
        out_dirpath_root: Path = Path(gettempdir()),
        l2a_mask_only: bool = False,
        prd_items: List[str] = None,
    ) -> Path:
        """Download S2 L2A product according to the product ID from the
         bucket: https://registry.opendata.aws/sentinel-2/

        Args:
            prd_id (str): Sentinel-2 L2A product ID
            out_dirpath_root (Path, optional): Path where to write the product.
             Defaults to Path(gettempdir()).
            l2a_mask_only (bool, optional): Retrieve only the L2A mask. Defaults to False.
            prd_items (List[str], optional): Applies a filter on which bands to download

        Returns:
            Path:  Path to the Sentinel-2 L2A product
        """
        return super()._download_s2_prd(
            prd_id, out_dirpath_root, l2_mask_only=l2a_mask_only, prd_items=prd_items
        )


class AWSS2L2ACOGSBucket(AWSS2Bucket):
    """Class to handle access to Sentinel-2 L2A data from AWS open data
    bucket: https://registry.opendata.aws/sentinel-2-l2a-cogs/"""

    def __init__(self) -> None:
        super().__init__("sentinel-cogs")

    def download_prd(
        self,
        prd_id: str,
        out_dirpath_root: Path = Path(gettempdir()),
        l2a_mask_only: bool = False,
    ) -> Path:
        """Download S2 L2A product according to the product ID from the
         bucket: https://registry.opendata.aws/sentinel-2-l2a-cogs/

        Args:
            prd_id (str): Sentinel-2 L2A product ID
            out_dirpath_root (Path, optional): Path where to write the product.
             Defaults to Path(gettempdir()).
            l2a_mask_only (bool, optional): Retrieve only the L2A mask. Defaults to False.

        Returns:
            Path: Path to the Sentinel-2 L2A product
        """
        return super()._download_s2_prd(
            prd_id, out_dirpath_root, l2_mask_only=l2a_mask_only, l2a_cogs=True
        )


class AWSL8C2L2Bucket(AWSEOBucket):
    """Class to handle access to Landsat 8 Collection 2 Level 2 data
    from AWS open data bucket: https://registry.opendata.aws/usgs-landsat/"""

    def __init__(self) -> None:
        super().__init__("usgs-landsat")

    def download_prd(
        self,
        prd_id: str,
        out_dirpath_root: Path = Path(gettempdir()),
        prd_items: List[str] = None,
    ) -> Path:
        """Download Landsat 8 Collection 2 Level 2 product according to the product ID
         from the bucket: https://registry.opendata.aws/usgs-landsat/

        Args:
            prd_id (str): Landsat 8 Collection 2 Level 2 product ID
            out_dirpath_root (Path, optional): Path where to write the product.
             Defaults to Path(gettempdir()).
            prd_items (List[str], optional): List of product items to download. Defaults to None.

        Returns:
            Path: Path to the Landsat 8 Collection 2 Level 2 product
        """

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
        super()._download_prd(
            prd_prefix, out_dirpath, request_payer=True, prd_items=prd_items
        )

        return out_dirpath

    @staticmethod
    def compute_prd_key(prd_id: str):
        l8_prd_info = L8C2PrdIdInfo(prd_id)
        prd_key = "/".join(
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

        return prd_key

    @staticmethod
    def compute_prd_item_key(prd_id: str, prd_item: str):
        prd_key = AWSL8C2L2Bucket.compute_prd_key(prd_id)
        from ewoc_dag.eo_prd_id.l8_prd_id import L8C2Prd

        return prd_key + "/" + L8C2Prd(prd_id).get_prd_item(prd_item)

    def to_gdal_path(self, prd_id: str, prd_item: str) -> str:
        """Compute the gdal path to the L8 C2 L2 prd item

            To use the path, don't forget export AWS_REQUEST_PAYER=requester
        Args:
            prd_id (str): L8C2L2 product ID
            prd_item (str): L8C2L2 product item

        Returns:
            str: vsis3 path for gdal command
        """
        return f"/vsis3/{self._bucket_name}/{AWSL8C2L2Bucket.compute_prd_item_key(prd_id, prd_item)}"


class AWSCopDEMBucket(AWSEOBucket):
    """Class to handle access to Copdem data
    from AWS open data bucket: https://registry.opendata.aws/copernicus-dem/"""

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
        to_sen2cor: bool = False,
    ) -> None:
        """
        Download copdem tiles from AWS bucket
        :param copdem_tile_ids: List of copdem tile ids
        :param out_dirpath: Output directory
        :param to_sen2cor: If True, rename copdem files to match sen2cor expectations
        :return: None
        """

        for copdem_tile_id in copdem_tile_ids:

            copdem_tile_id_aws = (
                self._copdem_prefix
                + copdem_tile_id[:3]
                + "_00_"
                + copdem_tile_id[3:]
                + self._copdem_suffix
            )
            copdem_tile_id_filename = copdem_tile_id_aws + ".tif"
            if to_sen2cor:
                copdem_tile_id_filepath = out_dirpath / copdem_tile_id_filename.replace(
                    "_COG_", "_"
                )
            else:
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

    # logger.info(
    #     AWSS1Bucket().download_prd(
    #         "S1B_IW_GRDH_1SSH_20210714T083244_20210714T083309_027787_0350EB_E62C.SAFE",
    #         safe_format=True,
    #     )
    # )
    # AWSS2L1CBucket().download_prd(
    #    "S2B_MSIL1C_20210714T235249_N0301_R130_T57KUR_20210715T005654.SAFE"
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
    _L8C2L2_PRD_ID = "LC08_L2SP_227099_20211017_20211026_02_T2"
    # AWSL8C2L2Bucket().download_prd(_L8C2L2_PRD_ID)
    logger.info(AWSL8C2L2Bucket.compute_prd_item_key(_L8C2L2_PRD_ID, "ST_QA"))
    logger.info(AWSL8C2L2Bucket().to_gdal_path(_L8C2L2_PRD_ID, "ST_QA"))
