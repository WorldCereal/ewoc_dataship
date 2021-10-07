import re
import boto3


class Sentinel_Cloud_Mask:
    """
    Sentinel cloud mask object, the main goal of this class is to check
    if a remote cloud mask is available and download it
    """

    def __init__(
        self,
        tile,
        date,
        bucket=None,
        prefix=None,
        provider="aws",
        payer=None,
    ):
        """

        :param tile: Sentinel-2 tile id ex 31TCJ or 31UFS
        :type tile: str
        :param date: Date of Sentinel-2 product in format YYYYmmdd
        :type date: str
        :param bucket: AWS bucket for Sentinel-2 level 2 products, Optional
        :type bucket: str
        :param prefix: AWS prefix for Sentinel-2 level 2 products, Optional
        :type prefix: str
        :param payer: Who is paying for the check and download
        :type payer: str
        """
        self.exists = False
        self.tile = tile
        self.key = None
        self.provider = provider
        self.date = date
        self.bucket = bucket
        self.prefix = prefix
        self.payer = payer

    def mask_exists(self):
        """
        Check if cloud mask exists for a given provider
        """
        if self.provider == "aws":
            if self.bucket is None:
                self.bucket = "sentinel-cogs"
            if self.prefix is None:
                self.prefix = "sentinel-s2-l2a-cogs/"

            file_keys = []
            year = self.date[:4]
            month = self.date[4:6]
            month = month.replace("0", "") if month[0] == "0" else month
            tile_digit = re.findall("\d+", self.tile)[0]
            tile_alpha = re.findall("[a-zA-Z]", self.tile)
            prod_dir = "{}/{}/{}/{}/{}/".format(
                tile_digit, tile_alpha[0], "".join(tile_alpha[1:]), year, month
            )
            response = {}
            if self.payer is "requester":
                s3 = boto3.client("s3")
                response = s3.list_objects_v2(
                    Bucket=self.bucket,
                    Prefix=self.prefix + prod_dir,
                    MaxKeys=100,
                    RequestPayer=self.payer,
                )
            elif self.payer is None:
                from botocore import UNSIGNED
                from botocore.config import Config

                s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
                response = s3.list_objects_v2(
                    Bucket=self.bucket, Prefix=self.prefix + prod_dir, MaxKeys=100,Delimiter="/",
                )
            resp = response["CommonPrefixes"]
            for prefix in resp:
                file_keys.append(prefix["Prefix"])
            cloud_mask = [
                file for file in file_keys if self.date in file
            ]
            if len(cloud_mask) == 1:
                self.key = cloud_mask[0]+'SCL.tif'
                self.exists = True
                return True
            else:
                return False
        else:
            # TODO add more providers or local folders
            raise NotImplementedError(f'Get S2 product from {self.provider} is not currently implemented!')

    def download_aws(self, out_file):
        """
        Download cloud mask to local storage
        :param out_file: Path where to copy the cloud mask
        :type out_file: str
        :return: True if success else return False
        :rtype: bool
        """

        if self.mask_exists():
            if self.payer == "requester":
                s3 = boto3.resource("s3")
                object = s3.Object(self.bucket, self.key)
                resp = object.get(RequestPayer="requester")
            elif self.payer is None:
                from botocore import UNSIGNED
                from botocore.config import Config

                s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
                object = s3.Object(self.bucket, self.key)
                resp = object.get()
            with open(out_file, "wb") as f:
                for chunk in iter(lambda: resp["Body"].read(4096), b""):
                    f.write(chunk)
            return True
        else:
            return False

    def download(self, out_file):
        """
        Download cloud mask
        :param out_file: Path where to copy the cloud mask
        :type out_file: str
        :return: True if success else return False
        :rtype: bool
        """
        if self.provider == "aws":
            self.download_aws(out_file)
            return True
        else:
            # Add more download methods for other providers or local folders
            # returns false for now
            return False
