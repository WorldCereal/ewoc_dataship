# -*- coding: utf-8 -*-
""" Test srtm dag module
"""
import logging
from pathlib import Path
import sys
from tempfile import gettempdir
import unittest

from ewoc_dag.l8c2l2_dag import (
    get_l8c2l2_product,
    get_l8c2l2_gdal_path
)

class Test_l8c2l2_dag(unittest.TestCase):
    _TEST_TILE_ID_1 = "31TCJ"
    _TEST_TILE_ID_2 = "33VVJ"
    
    def setUp(self) -> None:
        LOG_FORMAT = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
        logging.basicConfig(
            level=logging.INFO,
            stream=sys.stdout,
            format=LOG_FORMAT,
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self._PRD_ID = "LC08_L2SP_227099_20211017_20211026_02_T2"

    def test_get_l8c2l2_product(self):   
        get_l8c2l2_product(self._PRD_ID)
        get_l8c2l2_product(self._PRD_ID, prd_items=["ST_TRAD", "QA_PIXEL"])
    
    def test_get_l8c2l2_gdal_path(self):
        get_l8c2l2_gdal_path(self._PRD_ID, "ST_B10")