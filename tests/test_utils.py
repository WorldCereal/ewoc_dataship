from ewoc_dag.dag.utils import get_bounds, get_dates_from_prod_id
import unittest


class Test_dag(unittest.TestCase):
    def test_get_bounds(self):
        bnds = get_bounds('31TCJ')
        assert bnds == (300000.0, 4790220.0, 409800.0, 4900020.0)

    def test_get_dates_from_prod_id(self):
        res_sar = get_dates_from_prod_id("S1B_IW_GRDH_1SDV_20181101T061820_20181101T061845_013407_018CDF_B13F")
        res_optical_s2 = get_dates_from_prod_id("S2B_MSIL1C_20191021T110059_N0208_R094_T30SVG_20191021T130121")
        res_optical_l8 = get_dates_from_prod_id("LC08_L2SP_201033_20190718_20200827_02_T1_ST_B10")
        assert res_sar == ('2018-10-31', '2018-11-02', 'S1')
        assert res_optical_s2 == ('2019-10-20', '2019-10-22', 'S2')
        assert res_optical_l8 == ('2019-07-17', '2019-07-19', 'L8')

if __name__ == "__main__":
    unittest.main()