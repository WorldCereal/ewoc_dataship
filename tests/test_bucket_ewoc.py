# -*- coding: utf-8 -*-
""" Test EWoC private data bucket management module
"""
from pathlib import Path
from tempfile import gettempdir
import unittest

from ewoc_dag.bucket.ewoc import (
    split_tile_id,
    tileid_to_ard_path_component,
    EWOCAuxDataBucket,
    EWOCARDBucket,
    EWOCPRDBucket,
)


class Test_ewoc(unittest.TestCase):
    def test_split_tile_id(self):
        self.assertEqual(split_tile_id("31TCJ"), ("31", "T", "CJ"))

    def test_tileid_to_ard_path(self):
        self.assertEqual(tileid_to_ard_path_component("31TCJ"), "31/T/CJ")

    def test_ewoc_aux_data(self):
        ewoc_auxdata_bucket = EWOCAuxDataBucket()
        self.assertEqual(ewoc_auxdata_bucket.bucket_name, "ewoc-aux-data")

        out_dirpath = Path(gettempdir())
        ewoc_auxdata_bucket.download_srtm3s_tiles(
            ["srtm_01_16", "srtm_01_21"], out_dirpath=out_dirpath
        )
        self.assertTrue((out_dirpath / "srtm3s" / "srtm_01_16.tif").exists())
        self.assertTrue((out_dirpath / "srtm3s" / "srtm_01_21.tif").exists())
        ewoc_auxdata_bucket.download_srtm1s_tiles(
            ["N53E031", "N53E032"], out_dirpath=out_dirpath
        )
        self.assertTrue((out_dirpath / "N53E031.hgt").exists())
        self.assertTrue((out_dirpath / "N53E032.hgt").exists())

        ewoc_auxdata_bucket.agera5_to_satio_csv()
        self.assertTrue((out_dirpath / "satio_agera5.csv").exists())
        ewoc_auxdata_bucket.close()

    def test_ewoc_ard(self):
        ewoc_ard_dev_bucket = EWOCARDBucket(ewoc_dev_mode=True)
        upload_dirpath = Path(gettempdir()) / "srtm3s"
        upload_dirpath.mkdir(exist_ok=True)
        upload_filepath = upload_dirpath / "readme.txt"
        upload_filepath.touch()
        ewoc_ard_dev_bucket.upload_ard_raster(upload_filepath, "test_upload.file")
        ewoc_ard_dev_bucket.upload_ard_prd(upload_dirpath, "test_upload_dir")

        # Tile not available
        with self.assertRaises(ValueError):
            ewoc_ard_dev_bucket.sar_to_satio_csv(
                "31TCJ", "c728b264-5c97-4f4c-81fe-1500d4c4dfbd_11106_20220809155141"
            )

        ewoc_ard_dev_bucket.sar_to_satio_csv(
            "18GWR", "c728b264-5c97-4f4c-81fe-1500d4c4dfbd_11106_20220809155141"
        )

        ewoc_ard_dev_bucket.close()

    def test_ewoc_prd(self):
        # TODO finalize the test
        ewoc_prd_dev_bucket = EWOCPRDBucket(ewoc_dev_mode=True)
        upload_dirpath = Path(gettempdir()) / "srtm3s"
        ewoc_prd_dev_bucket.upload_ewoc_prd(upload_dirpath, "test_upload_prd_dir")
        ewoc_prd_dev_bucket.close()

        ewoc_prd_bucket = EWOCPRDBucket()
        ewoc_prd_dev_bucket.download_bucket_prefix(
            "47QQG/2021_annual/", Path(gettempdir())
        )
        ewoc_prd_bucket.close()


if __name__ == "__main__":
    unittest.main()
