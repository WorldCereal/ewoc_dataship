# -*- coding: utf-8 -*-
""" Test copdem dag module
"""
from pathlib import Path
from tempfile import gettempdir
import unittest

from ewoc_dag.copdem_dag import (
    get_copdem_default_provider,
    get_copdem_from_s2_tile_id,
    get_copdem_tiles,
    get_copdem_ids,
    get_gdal_vrt_files,
    to_gdal_vrt_input_file_list,
)


class Test_copdem_dag(unittest.TestCase):
    _TEST_TILE_ID_1 = "31TCJ"
    _TEST_TILE_ID_2 = "33VVJ"

    def test_get_copdem_default_provider(self):
        self.assertEqual(get_copdem_default_provider(), "aws")

    def test_get_copdem_from_s2_tile_id(self):
        get_copdem_from_s2_tile_id(self._TEST_TILE_ID_1)
        self.assertTrue(
            (
                Path(gettempdir()) / "Copernicus_DSM_COG_10_N61_00_E013_00_DEM.tif"
            ).exists()
        )
        get_copdem_from_s2_tile_id(self._TEST_TILE_ID_2)
        self.assertTrue(
            (
                Path(gettempdir()) / "Copernicus_DSM_COG_10_N44_00_E001_00_DEM.tif"
            ).exists()
        )

    def test_get_copdem_tiles(self):
        get_copdem_tiles(get_copdem_ids(self._TEST_TILE_ID_1))
        get_copdem_tiles(get_copdem_ids(self._TEST_TILE_ID_2))

    def test_get_copdem_ids(self):
        self.assertEqual(
            get_copdem_ids(self._TEST_TILE_ID_1),
            ["N43E001", "N43E000", "N44E000", "N44E001"],
        )
        self.assertEqual(
            get_copdem_ids(self._TEST_TILE_ID_2),
            ["N62E015", "N62E014", "N62E013", "N61E015", "N61E014", "N61E013"],
        )

    def test_get_gdal_vrt_files(self):
        self.assertListEqual(
            get_gdal_vrt_files(sorted(get_copdem_ids(self._TEST_TILE_ID_1))),
            [
                "/vsis3/copernicus-dem-30m/Copernicus_DSM_COG_10_N43_00_E000_00_DEM/Copernicus_DSM_COG_10_N43_00_E000_00_DEM.tif",
                "/vsis3/copernicus-dem-30m/Copernicus_DSM_COG_10_N43_00_E001_00_DEM/Copernicus_DSM_COG_10_N43_00_E001_00_DEM.tif",
                "/vsis3/copernicus-dem-30m/Copernicus_DSM_COG_10_N44_00_E000_00_DEM/Copernicus_DSM_COG_10_N44_00_E000_00_DEM.tif",
                "/vsis3/copernicus-dem-30m/Copernicus_DSM_COG_10_N44_00_E001_00_DEM/Copernicus_DSM_COG_10_N44_00_E001_00_DEM.tif",
            ],
        )
        self.assertListEqual(
            get_gdal_vrt_files(sorted(get_copdem_ids(self._TEST_TILE_ID_2))),
            [
                "/vsis3/copernicus-dem-30m/Copernicus_DSM_COG_10_N61_00_E013_00_DEM/Copernicus_DSM_COG_10_N61_00_E013_00_DEM.tif",
                "/vsis3/copernicus-dem-30m/Copernicus_DSM_COG_10_N61_00_E014_00_DEM/Copernicus_DSM_COG_10_N61_00_E014_00_DEM.tif",
                "/vsis3/copernicus-dem-30m/Copernicus_DSM_COG_10_N61_00_E015_00_DEM/Copernicus_DSM_COG_10_N61_00_E015_00_DEM.tif",
                "/vsis3/copernicus-dem-30m/Copernicus_DSM_COG_10_N62_00_E013_00_DEM/Copernicus_DSM_COG_10_N62_00_E013_00_DEM.tif",
                "/vsis3/copernicus-dem-30m/Copernicus_DSM_COG_10_N62_00_E014_00_DEM/Copernicus_DSM_COG_10_N62_00_E014_00_DEM.tif",
                "/vsis3/copernicus-dem-30m/Copernicus_DSM_COG_10_N62_00_E015_00_DEM/Copernicus_DSM_COG_10_N62_00_E015_00_DEM.tif",
            ],
        )

    def test_to_gdal_vrt_input_file_list(self):
        to_gdal_vrt_input_file_list(get_copdem_ids(self._TEST_TILE_ID_1))
        self.assertTrue((Path(gettempdir()) / "copdem_list.txt").exists())
        to_gdal_vrt_input_file_list(get_copdem_ids(self._TEST_TILE_ID_2))
        self.assertTrue((Path(gettempdir()) / "copdem_list.txt").exists())


if __name__ == "__main__":
    unittest.main()
