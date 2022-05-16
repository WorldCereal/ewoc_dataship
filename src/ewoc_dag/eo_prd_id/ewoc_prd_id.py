# -*- coding: utf-8 -*-
""" EWoC ARD product ID information
"""

from datetime import datetime


class EwocArdPrdIdInfo:

    _FORMAT_DATETIME = "%Y%m%dT%H%M%S"

    def __init__(self, ewoc_prd_id: str) -> None:
        # S2A_MSIL2A_20181217T105441_N9999R051T31TCJ_31TCJ
        self._ewoc_prd_id = ewoc_prd_id
        elt_prd_id = self._ewoc_prd_id.split("_")
        if len(elt_prd_id) == 5:
            self.platform = elt_prd_id[0]
            self.acquisition_datetime = elt_prd_id[2]
            self.discriminatror_id = elt_prd_id[3]
            self.tile_id = elt_prd_id[4]
        else:
            raise ValueError(
                "EOWC ARD product id not provides the 5 keys values requested!"
            )

    @property
    def platform(self):
        return self._platform

    @platform.setter
    def platform(self, value):
        if value in ["S2A", "S2B", "LC08"]:
            self._platform = value
        else:
            raise ValueError("Platform is not possible!")

    @property
    def acquisition_datetime(self):
        return self._acquisition_datetime

    @acquisition_datetime.setter
    def acquisition_datetime(self, value):
        self._acquisition_datetime = datetime.strptime(value, self._FORMAT_DATETIME)

    @property
    def discriminatror_id(self):
        return self._discriminatror_id

    @discriminatror_id.setter
    def discriminatror_id(self, value):
        self._discriminatror_id = value

    @property
    def tile_id(self):
        return self._tile_id

    @tile_id.setter
    def tile_id(self, value):
        self._tile_id = value

    def __str__(self):
        return f"Info provided by the EWOC ARD product id are: \
platform={self.platform}, \
acquisition_datetime={self.acquisition_datetime}, \
discriminatror_id={self.discriminatror_id}, tile_id={self.tile_id}"

    def __repr__(self):
        return f"EwocArdPrdIdInfo(ewoc_prd_id={self._ewoc_prd_id})"

    @staticmethod
    def is_valid(ewoc_prd_id):
        try:
            EwocArdPrdIdInfo(ewoc_prd_id)
            return True
        except ValueError:
            return False


class EwocS1ArdPrdIdInfo:

    _FORMAT_DATETIME = "%Y%m%dT%H%M%S"

    def __init__(self, ewoc_prd_id: str) -> None:
        # S1A_20181208T060900_DES_TODO_02493002BF1C1710_31TCJ
        self._ewoc_prd_id = ewoc_prd_id
        elt_prd_id = self._ewoc_prd_id.split("_")
        if len(elt_prd_id) == 6:
            self.platform = elt_prd_id[0]
            self.acquisition_datetime = elt_prd_id[1]
            self.orbit_direction = elt_prd_id[2]
            self.discriminatror_id = elt_prd_id[4]
            self.tile_id = elt_prd_id[5]
        else:
            raise ValueError(
                "EOWC S1 ARD product id not provides the 6 keys values requested!"
            )

    @property
    def platform(self):
        return self._platform

    @platform.setter
    def platform(self, value):
        if value in ["S1A", "S1B"]:
            self._platform = value
        else:
            raise ValueError("Platform is not possible!")

    @property
    def acquisition_datetime(self):
        return self._acquisition_datetime

    @acquisition_datetime.setter
    def acquisition_datetime(self, value):
        self._acquisition_datetime = datetime.strptime(value, self._FORMAT_DATETIME)

    @property
    def orbit_direction(self):
        return self._orbit_direction

    @orbit_direction.setter
    def orbit_direction(self, value):
        if value in ["DES", "ASC"]:
            self._orbit_direction = value
        else:
            raise ValueError("Orbit direction is not possible!")

    @property
    def discriminatror_id(self):
        return self._discriminatror_id

    @discriminatror_id.setter
    def discriminatror_id(self, value):
        self._discriminatror_id = value

    @property
    def tile_id(self):
        return self._tile_id

    @tile_id.setter
    def tile_id(self, value):
        self._tile_id = value

    def __str__(self):
        return f"Info provided by the EWOC ARD product id are: \
platform={self.platform}, \
acquisition_datetime={self.acquisition_datetime}, \
discriminatror_id={self.discriminatror_id}, tile_id={self.tile_id}"

    def __repr__(self):
        return f"EwocArdPrdIdInfo(ewoc_prd_id={self._ewoc_prd_id})"

    @staticmethod
    def is_valid(ewoc_prd_id):
        try:
            EwocS1ArdPrdIdInfo(ewoc_prd_id)
            return True
        except ValueError:
            return False


class EwocTirArdPrdIdInfo:

    _FORMAT_DATETIME = "%Y%m%dT%H%M%S"

    def __init__(self, ewoc_prd_id: str) -> None:
        # LC08_L2SP_20190720_19902902T1_31TCJ
        self._ewoc_prd_id = ewoc_prd_id
        elt_prd_id = self._ewoc_prd_id.split("_")
        if len(elt_prd_id) == 5:
            self.platform = elt_prd_id[0]
            self.acquisition_datetime = elt_prd_id[2]
            self.discriminatror_id = elt_prd_id[3]
            self.tile_id = elt_prd_id[4]
        else:
            raise ValueError(
                "EOWC S1 ARD product id not provides the 5 keys values requested!"
            )

    @property
    def platform(self):
        return self._platform

    @platform.setter
    def platform(self, value):
        if value in ["LC08"]:
            self._platform = value
        else:
            raise ValueError("Platform is not possible!")

    @property
    def acquisition_datetime(self):
        return self._acquisition_datetime

    @acquisition_datetime.setter
    def acquisition_datetime(self, value):
        self._acquisition_datetime = datetime.strptime(value, self._FORMAT_DATETIME)

    @property
    def discriminatror_id(self):
        return self._discriminatror_id

    @discriminatror_id.setter
    def discriminatror_id(self, value):
        self._discriminatror_id = value

    @property
    def tile_id(self):
        return self._tile_id

    @tile_id.setter
    def tile_id(self, value):
        self._tile_id = value

    def __str__(self):
        return f"Info provided by the EWOC TIR ARD product id are: \
platform={self.platform}, \
acquisition_datetime={self.acquisition_datetime}, \
discriminatror_id={self.discriminatror_id}, tile_id={self.tile_id}"

    def __repr__(self):
        return f"EwocTirArdPrdIdInfo(ewoc_prd_id={self._ewoc_prd_id})"

    @staticmethod
    def is_valid(ewoc_prd_id):
        try:
            EwocTirArdPrdIdInfo(ewoc_prd_id)
            return True
        except ValueError:
            return False


if __name__ == "__main__":
    print(EwocArdPrdIdInfo("S2A_MSIL2A_20181217T105441_N9999R051T31TCJ_31TCJ"))
    print(EwocArdPrdIdInfo.is_valid("S2A_MSIL2A_20181217T105441_N9999R051T31TCJ_31TCJ"))
