import boto3


class Landsat_Cloud_Mask:
    """
    Landsat cloud mask object, the main goal of this class is to check
    if a remote cloud mask is available and download it
    """

    def __init__(
        self,
        path,
        row,
        date,
        bucket=None,
        prefix=None,
        provider="aws",
        payer="requester",
    ):
        """

        :param path: Landsat-8 path ex 198
        :type path: str
        :param row: Landsat-8 row ex 030 (don't forget the 0 for 2 digits rows)
        :type row: str
        :param date: Date of Landsat-8 scene in format YYYYmmdd
        :type date: str
        :param bucket: AWS bucket for Landsat-8 level 2 products, Optional
        :type bucket: str
        :param prefix: AWS prefix for Landsat-8 level 2 products, Optional
        :type prefix: str
        :param payer: Who is paying for the check and download
        :type payer: str
        """
        self.exists = False
        self.cloud_key = None
        self.tirs_10_key = None
        self.provider = provider
        self.path = path
        self.row = row
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
                self.bucket = "usgs-landsat"
            if self.prefix is None:
                self.prefix = "collection02/level-2/standard/oli-tirs/"
            s3 = boto3.client("s3")
            file_keys = []
            year = self.date[:4]
            prod_dir = "{}/{}/{}/".format(year, self.path, self.row)
            response = s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix=self.prefix + prod_dir,
                MaxKeys=1000,
                RequestPayer=self.payer,
                Delimiter='/',
            )
            resp = response["CommonPrefixes"]
            for prefix in resp:
                file_keys.append(prefix["Prefix"])
            cloud_mask = [
                file for file in file_keys if self.date in file.split('/')[7].split('_')[3]
            ]
            if len(cloud_mask) > 0:
                self.cloud_key = cloud_mask[0]+cloud_mask[0].split('/')[7]+'_SR_QA_AEROSOL.TIF'
                self.tirs_10_key = cloud_mask[0]+cloud_mask[0].split('/')[7]+'_ST_B10.TIF'
                self.exists = True
                return True
            else:
                return False
        else:
            # TODO add more providers or local folders
            # returns false for now
            return False

    def download_aws(self, out_file):
        """
        Download cloud mask to local storage
        :param out_file: Path where to copy the cloud mask
        :type out_file: str
        :return: True if success else return False
        :rtype: bool
        """
        s3 = boto3.resource("s3")
        if self.mask_exists():
            object = s3.Object(self.bucket, self.cloud_key)
            resp = object.get(RequestPayer="requester")
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
            self.download_aws(self, out_file)
            return True
        else:
            # Add more download methods for other providers or local folders
            # returns false for now
            return False
