from datetime import datetime

from typing import List


class L8C2Prd:
    _PRD_ITEMS = {
        "ANG": "ANG.txt",
        "MTL_XML": "MTL.xml",
        "MTL_TXT": "MTL.txt",
        "MTL_JSON": "MTL.json",
        "QA_PIXEL": "QA_PIXEL.TIF",
        "QA_RADSAT": "QA_RADSAT.TIF",
        "SR_B1": "SR_B1.TIF",
        "SR_B2": "SR_B2.TIF",
        "SR_B3": "SR_B3.TIF",
        "SR_B4": "SR_B4.TIF",
        "SR_B5": "SR_B5.TIF",
        "SR_B6": "SR_B6.TIF",
        "SR_B7": "SR_B7.TIF",
        "SR_QA_AEROSOL": "SR_QA_AEROSOL.TIF",
        "SR_STAC": "SR_stac_.json",
        "ST_ATRAN": "ST_ATRAN.TIF",
        "ST_B10": "ST_B10.TIF",
        "ST_CDIST": "ST_CDIST.TIF",
        "ST_DRAD": "ST_DRAD.TIF",
        "ST_EMIS": "ST_EMIS.TIF",
        "ST_EMSD": "ST_EMSD.TIF",
        "ST_QA": "ST_QA.TIF",
        "ST_TRAD": "ST_TRAD.TIF",
        "ST_URAD": "ST_URAD.TIF",
        "ST_stac": "ST_stac.json",
        "thumb_large": "thumb_large.jpeg",
        "thumb_small": "thumb_small.jpeg",
    }

    def __init__(self, l8_c2_prd_id: str) -> None:
        self.prd_info = L8C2PrdIdInfo(l8_c2_prd_id)
        self._prd_id = l8_c2_prd_id

    def get_prd_item(self, prd_item) -> str:
        if prd_item in [*self._PRD_ITEMS]:
            return f"{self._prd_id}_{self._PRD_ITEMS[prd_item]}"
        raise ValueError(f"{prd_item} not available in {[*self._PRD_ITEMS]}!")

    def _get_prd_items(self, prd_item_type="SR") -> List[str]:
        prd_items = []
        for prd_item in [*self._PRD_ITEMS]:
            if prd_item_type in prd_item:
                prd_items.append(self.get_prd_item(prd_item))
        return prd_items

    def get_sr_items(self) -> List[str]:
        return self._get_prd_items()

    def get_st_items(self) -> List[str]:
        return self._get_prd_items(prd_item_type="ST")


class L8C2PrdIdInfo:

    _FORMAT_DATETIME = "%Y%m%d"

    def __init__(self, l8_c2_prd_id: str) -> None:
        # LXSS_LLLL_PPPRRR_YYYYMMDD_yyyymmdd_CC_TX
        # https://www.usgs.gov/media/files/landsat-8-9-olitirs-collection-2-level-2-data-format-control-book
        self._l8_c2_prd_id = l8_c2_prd_id
        elt_prd_id = self._l8_c2_prd_id.split("_")
        if len(elt_prd_id) == 7:
            self.platform_id = elt_prd_id[0][3:4]
            self.processing_level = elt_prd_id[1]
            self.wrs2_path = elt_prd_id[2][:3]
            self.wrs2_row = elt_prd_id[2][3:]
            self.acquisition_date = elt_prd_id[3]
            self.processing_date = elt_prd_id[4]
            self.collection = elt_prd_id[5]
            self.collection_category = elt_prd_id[6]
        else:
            raise ValueError(
                "Landsat 8-9 product id not provides the 9 keys values requested!"
            )

    @property
    def platform_id(self):
        return self._platform_id

    @platform_id.setter
    def platform_id(self, value):
        if value in ["8", "9"]:
            self._platform_id = value
        else:
            raise ValueError("Platform id is not possible!")

    @property
    def processing_level(self):
        return self._processing_level

    @processing_level.setter
    def processing_level(self, value):
        if value in ["L2SP", "L2SR"]:
            self._processing_level = value
        else:
            raise ValueError("Processing level is not possible!", value)

    @property
    def wrs2_path(self):
        return self._wrs2_path

    @wrs2_path.setter
    def wrs2_path(self, value):
        self._wrs2_path = value

    @property
    def wrs2_row(self):
        return self._wrs2_row

    @wrs2_row.setter
    def wrs2_row(self, value):
        self._wrs2_row = value

    @property
    def acquisition_date(self):
        return self._acquisition_date

    @acquisition_date.setter
    def acquisition_date(self, value):
        self._acquisition_date = datetime.strptime(value, self._FORMAT_DATETIME).date()

    @property
    def processing_date(self):
        return self._processing_date

    @processing_date.setter
    def processing_date(self, value):
        self._processing_date = datetime.strptime(value, self._FORMAT_DATETIME).date()

    @property
    def collection(self):
        return self._collection

    @collection.setter
    def collection(self, value):
        if value == "02":
            self._collection = value
        else:
            raise ValueError("Collection is not possible!")

    @property
    def collection_category(self):
        return self._collection_category

    @collection_category.setter
    def collection_category(self, value):
        allowed_values = ["T1", "T2"]
        if value in allowed_values:
            self._collection_category = value
        else:
            raise ValueError(
                "Collection category different than "
                + ", ".join(allowed_values)
                + " is not possible!"
            )

    def __str__(self):
        return f"Info provided by the Landsat 8 Collection 2 product id are: \
platform_id={self.platform_id}, \
processing_level={self.processing_level}, \
wrs2_path={self.wrs2_path}, wrs2_row={self.wrs2_row}, \
acquisition_date={self.acquisition_date}, processing_date={self.processing_date}, \
collection={self.collection}, collection_category={self.collection_category}"

    def __repr__(self):
        return f"L8C2PrdIdInfo(l8c2_prd_id={self._l8_c2_prd_id})"

    @staticmethod
    def is_valid(l8_c2_prd_id):
        try:
            L8C2PrdIdInfo(l8_c2_prd_id)
            return True
        except ValueError:
            return False


if __name__ == "__main__":
    print(L8C2PrdIdInfo("LC08_L2SP_227099_20211017_20211026_02_T2"))
    print(L8C2PrdIdInfo.is_valid("LC08_L2SP_227099_20211017_20211026_02_T2.SAFE"))
    print(L8C2PrdIdInfo.is_valid("LC08_L2SP_227099_20211017_20211026_02_T2"))
