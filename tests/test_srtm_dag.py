# -*- coding: utf-8 -*-
""" Test srtm dag module
"""
import logging
from pathlib import Path
import sys
from tempfile import gettempdir
import unittest

from ewoc_dag.srtm_dag import (
    get_srtm_1s_default_provider,
    get_srtm_from_s2_tile_id,
    get_srtm_tiles,
    get_srtm_from_esa,
    get_srtm1s_ids,
    get_srtm3s_ids,
    get_srtm3s_ids_using_sen2cor_method
)

class Test_srtm_dag(unittest.TestCase):
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

    def test_get_srtm1s_default_provider(self) -> None:
        self.assertEqual(get_srtm_1s_default_provider(), "aws")

    def test_get_srtm1s_s2_tile_id(self):
        get_srtm_from_s2_tile_id(self._TEST_TILE_ID_1, source='ewoc')
        for srtm_1s_tile in ['N43E001', 'N43E000', 'N44E000', 'N44E001']:
            srtm_filepath=Path(gettempdir())/ f'{srtm_1s_tile}.hgt'
            self.assertTrue(srtm_filepath.exists())

    def test_get_srtm3s_s2_tile_id(self):
        get_srtm_from_s2_tile_id(self._TEST_TILE_ID_2,resolution='3s')


    def test_get_srtm1s_tiles(self):
        srtm_1s_tiles = ['N43E001', 'N43E000', 'N44E000', 'N44E001']
        get_srtm_tiles(srtm_1s_tiles, source='ewoc')
        for srtm_1s_tile in srtm_1s_tiles:
            srtm_filepath=Path(gettempdir())/ f'{srtm_1s_tile}.hgt'
            self.assertTrue(srtm_filepath.exists())

    def test_get_srtm3s_tiles(self):
        srtm_3s_tiles = ['srtm_39_00', 'srtm_40_00']
        get_srtm_tiles(srtm_3s_tiles, resolution='3s')
        for srtm_3s_tile in srtm_3s_tiles:
            srtm_filepath=Path(gettempdir())/ f'{srtm_3s_tile}.hgt'
            self.assertTrue(srtm_filepath.exists())


    def test_get_srtm_from_esa(self):
        srtm_1s_tiles = ['N43E001', 'N43E000', 'N44E000', 'N44E001']
        get_srtm_from_esa(srtm_1s_tiles)
        for srtm_1s_tile in srtm_1s_tiles:
            self.assertTrue((Path(gettempdir())/ f'{srtm_1s_tile}.hgt').exists())

    def test_get_srtm1s_ids(self):
        self.assertListEqual(get_srtm1s_ids(self._TEST_TILE_ID_1),
            ['N43E001', 'N43E000', 'N44E000', 'N44E001'])
        self.assertListEqual(get_srtm1s_ids(self._TEST_TILE_ID_2),
            ['N62E015', 'N62E014', 'N62E013', 'N61E015', 'N61E014', 'N61E013'])
    
    def test_get_srtm3s_ids(self):
        self.assertListEqual(get_srtm3s_ids(self._TEST_TILE_ID_1), ['srtm_37_04'])
        self.assertListEqual(sorted(get_srtm3s_ids(self._TEST_TILE_ID_2)), ['srtm_39_00', 'srtm_40_00'] )        