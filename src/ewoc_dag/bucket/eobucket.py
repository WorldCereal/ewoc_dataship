# -*- coding: utf-8 -*-
""" EO bucket management base module
"""
import logging
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


class UploadFileError(Exception):
    """Exception raised when file upload failed"""

    def __init__(self, client_error: ClientError, filepath, bucket_name, key):
        self.filepath = filepath
        self.bucket_name = bucket_name
        self.key = key
        super().__init__(client_error.response["Error"]["Message"])

    def __str__(self):
        return f"{self.filepath} is not uploaded to {self.bucket_name} at {self.key}!"


class UploadProductError(Exception):
    """Exception raised when product upload failed"""

    def __init__(self, upload_file_error: UploadFileError, prd_dirpath, object_prefix):
        self.prd_dirpath = prd_dirpath
        self.object_prefix = object_prefix
        super().__init__(upload_file_error.message)

    def __str__(self):
        return f"{self.prd_dirpath} fail to be uploaded to {self.object_prefix}!"


class DownloadFileError(Exception):
    """Exception raised when file download failed"""

    def __init__(self, client_error: ClientError, filepath, bucket_name, key):
        self.filepath = filepath
        self.bucket_name = bucket_name
        self.key = key
        super().__init__(client_error.response["Error"]["Message"])

    def __str__(self):
        return f"{self.key} is not download from {self.bucket_name} to {self.filepath}!"


class EOBucket:
    """Base class to describe the access (download, upload and list)
    to a bucket which contains EO data."""

    def __init__(
        self,
        bucket_name: str,
        s3_access_key_id: str = None,
        s3_secret_access_key: str = None,
        endpoint_url: str = None,
    ) -> None:
        """EO Bucket constructor

        Args:
            bucket_name (str): Name of the bucket
            s3_access_key_id (str, optional): Access key id of the bucket. Defaults to None.
            s3_secret_access_key (str, optional): Secret Access_key of the bucket. Defaults to None.
            endpoint_url (str, optional): Bucket endpoint URL. Defaults to None.
        """
        if (
            s3_access_key_id is None
            and s3_secret_access_key is None
            and endpoint_url is None
        ):
            self._s3_client = boto3.client("s3")
        else:
            self._s3_client = boto3.client(
                "s3",
                aws_access_key_id=s3_access_key_id,
                aws_secret_access_key=s3_secret_access_key,
                endpoint_url=endpoint_url,
            )

        self._bucket_name = bucket_name

    @property
    def bucket_name(self) -> str:
        """Returns the bucket name

        Returns:
            str: bucket name
        """
        return self._bucket_name

    def _check_bucket(self) -> bool:
        """Check if the bucket is usable

        Returns:
            bool: return True if the bucket is accessible and False otherwise
        """
        try:
            self._s3_client.head_bucket(Bucket=self._bucket_name)
        except ClientError as err:
            error_code = err.response["Error"]["Code"]
            if error_code == "404":
                logger.critical("Bucket %s does not exist!", self._bucket_name)
            elif error_code == "403":
                logger.critical("Acces forbidden to %s bucket!", self._bucket_name)
            return False

        return True

    def _s3_basepath(self) -> str:
        """Compute the basepath of the bucket s3://bucket_name

        Returns:
            str: basepath ex. s3://bucket_name
        """
        return f"s3://{self._bucket_name}"

    def _download_prd(
        self,
        prd_prefix: str,
        out_dirpath: Path,
        request_payer: bool = False,
        filter_band: str = None,
    ) -> None:
        """Download product from object storage

        Args:
            prd_prefix (str): prd key prefix
            out_dirpath (Path): directory where to write the objects of the product
            request_payer (bool): requester activation
        """
        extra_args = None
        request_payer_arg = str()
        if request_payer is True:
            extra_args = dict(RequestPayer="requester")
            request_payer_arg = "requester"

        logger.debug("Product prefix: %s", prd_prefix)
        response = self._s3_client.list_objects_v2(
            Bucket=self._bucket_name, Prefix=prd_prefix, RequestPayer=request_payer_arg
        )

        for obj in response["Contents"]:
            if filter_band is None or filter_band in obj["Key"]:
                logger.debug("obj.key: %s", obj["Key"])
                filename = obj["Key"].split(
                    sep="/", maxsplit=len(prd_prefix.split("/")) - 1
                )[-1]
                output_filepath = out_dirpath / filename
                (output_filepath.parent).mkdir(parents=True, exist_ok=True)
                logging.info("Try to download %s to %s", obj["Key"], output_filepath)
                self._s3_client.download_file(
                    Bucket=self._bucket_name,
                    Key=obj["Key"],
                    Filename=str(output_filepath),
                    ExtraArgs=extra_args,
                )

    def _upload_file(self, filepath: Path, key: str) -> bool:
        """Upload a object to a bucket

        Args:
            filepath (Path): Filepath of the object to upload
            key (str): key in the object storage of the object

        Returns:
            [int]: if upload succeed, return st_size of the uploaded file
        """
        if not filepath.exists():
            raise FileNotFoundError(f"{filepath} not exists")

        try:
            logger.info(
                "Try to upload %s to %s",
                filepath,
                "s3://" + self._bucket_name + "/" + key,
            )
            self._s3_client.upload_file(str(filepath), self._bucket_name, key)
        except ClientError as err:
            raise UploadFileError(err, filepath, self._bucket_name, key) from None

        logging.debug("Uploaded %s (%s) to %s", filepath, filepath.stat().st_size, key)
        return filepath.stat().st_size

    def _upload_prd(
        self, prd_dirpath: Path, object_prefix: str, file_suffix: str = ".tif"
    ):
        """Upload a set of objects from a directory to a bucket

        Args:
            dirpath (Path): Directory which contains the files to upload
            object_prefix (str): where to put the objects
            file_suffix (str, optional): extension use to filter the files in the directory.
                Defaults to '.tif'.

        Returns:
            [type]: [description]
        """
        if file_suffix is None:
            paths = sorted(prd_dirpath.rglob("*"))
        else:
            paths = sorted(prd_dirpath.rglob("*" + file_suffix))
        upload_object_size = 0
        nb_filepath = len(paths)
        for path in paths:
            if path.is_dir():
                nb_filepath -= 1
                continue
            filepath = path
            upload_object_size += filepath.stat().st_size
            key = object_prefix + "/" + str(filepath.relative_to(prd_dirpath))
            try:
                upload_object_size += self._upload_file(filepath, key)
            except UploadFileError as err:
                raise UploadProductError(err, prd_dirpath, object_prefix) from err

        logger.info(
            "Uploaded content of %s with %s suffix (%s file(s) with total size of %s) to %s",
            prd_dirpath,
            file_suffix,
            nb_filepath,
            upload_object_size,
            object_prefix,
        )

        return nb_filepath, upload_object_size
