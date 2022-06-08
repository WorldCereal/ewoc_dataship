from datetime import datetime

class S1PrdIdInfo:

    FORMAT_DATETIME='%Y%m%dT%H%M%S'

    def __init__(self, s1_prd_id:str) -> None:
        # S1A_IW_GRDH_1SDV_20210708T060105_20210708T060130_038682_04908E_8979.SAFE
        # https://sentinels.copernicus.eu/web/sentinel/user-guides/sentinel-1-sar/naming-conventions
        s1_prod_id_wsafe=s1_prd_id.split('.')[0]
        self._s1_prd_id=s1_prod_id_wsafe
        elt_prd_id = self._s1_prd_id.split('_')
        if len(elt_prd_id) == 9:
            self.mission_id = elt_prd_id[0]
            self.beam_mode = elt_prd_id[1]
            self.product_type = elt_prd_id[2][:3]
            self.resolution_class = elt_prd_id[2][3]
            self.processing_level = elt_prd_id[3][0]
            self.product_class = elt_prd_id[3][1]
            self.polarisation = elt_prd_id[3][2:]
            self.start_time = elt_prd_id[4]
            self.stop_time = elt_prd_id[5]
            self.absolute_orbit_number = elt_prd_id[6]
            self.mission_datatake_id = elt_prd_id[7]
            self.product_unique_id = elt_prd_id[8]
        else:
            raise ValueError('Sentinel 1 product id not provides the 9 keys values requested!')

    @property
    def product_unique_id(self):
        return self._product_unique_id

    @product_unique_id.setter
    def product_unique_id(self, value):
        if len(value) == 4:
            self._product_unique_id = value
        else:
            raise ValueError("Length of Product unique id different than 4 is not possible!", value)

    @property
    def mission_datatake_id(self):
        return self._mission_datatake_id

    @mission_datatake_id.setter
    def mission_datatake_id(self, value):
        if len(value) == 6:
            self._mission_datatake_id = value
        else:
            raise ValueError("Length of Mission datatake id different than 6 is not possible!", value)

    @property
    def absolute_orbit_number(self):
        return self._absolute_orbit_number

    @absolute_orbit_number.setter
    def absolute_orbit_number(self, value):
        if len(value) == 6:
            self._absolute_orbit_number = value
        else:
            raise ValueError("Length of Absolute orbit number different than 6 is not possible!", value)

    @property
    def start_time(self):
        return self._start_time

    @start_time.setter
    def start_time(self, value):
        self._start_time = datetime.strptime(value, self.FORMAT_DATETIME)

    @property
    def stop_time(self):
        return self._stop_time

    @stop_time.setter
    def stop_time(self, value):
        self._stop_time = datetime.strptime(value, self.FORMAT_DATETIME)
        
    @property
    def polarisation(self):
        return self._polarisation

    @polarisation.setter
    def polarisation(self, value):
        allowed_values= ['SH', 'SV', 'DH', 'DV']
        if value in allowed_values:
            self._polarisation = value
        else:
            raise ValueError("Polarisation (" + value + ") different than " + ', '.join(allowed_values) + " is not possible!")

    @property
    def product_class(self):
        return self._product_class

    @product_class.setter
    def product_class(self, value):
        allowed_values = ['S', 'A']
        if value in allowed_values:
            self._product_class = value
        else:
            raise ValueError("Product Class (" + value + ") different than " + ', '.join(allowed_values) + " is not possible!")

    @property
    def processing_level(self):
        return self._processing_level

    @processing_level.setter
    def processing_level(self, value):
        allowed_values = ['1', '2']
        if value in allowed_values:
            self._processing_level = value
        else:
            raise ValueError("Processing Level (" + value + ") different than " + ', '.join(allowed_values) + " is not possible!")

    @property
    def resolution_class(self):
        return self._resolution_class

    @resolution_class.setter
    def resolution_class(self, value):
        allowed_values = ['F', 'H', 'M'] 
        if value in allowed_values:
            self._resolution_class = value
        else:
            raise ValueError("Resolution class (" + value + ") different than " + ', '.join(allowed_values) + " is not possible!")

    @property
    def product_type(self):
        return self._product_type

    @product_type.setter
    def product_type(self, value):
        allowed_values = ['SLC', 'GRD', 'OCN'] 
        if value in allowed_values:
            self._product_type = value
        else:
            raise ValueError("Product type (" + value + ") different than " + ', '.join(allowed_values) + " is not possible!")

    @property
    def mission_id(self):
        return self._mission_id

    @mission_id.setter
    def mission_id(self, value):
        allowed_values= ['S1A', 'S1B']
        if value in allowed_values:
            self._mission_id = value
        else:
            raise ValueError("Mission ID (" + value + ") different than "+ ', '.join(allowed_values) + " is not possible!")

    @property
    def beam_mode(self):
        return self._beam_mode

    @beam_mode.setter
    def beam_mode(self, value):
        allowed_values = ['SM', 'IW', 'EW', 'WV']
        if value in allowed_values:
            self._beam_mode = value
        else:
            raise ValueError("Beam mode (" + value + ") different than " + ', '.join(allowed_values) + " is not possible!")

    def __str__(self):
        return f'Info provided by the S1 product id are: mission_id={self.mission_id}, beam_mode={self.beam_mode}, \
product_type={self.product_type}, resolution_class={self.resolution_class}, \
processing_level={self.processing_level}, product_class={self.product_class}, \
polarisation={self.polarisation}, start time={self.start_time}, stop time={self.stop_time}, \
absolute_orbit_number={self.absolute_orbit_number}, mission_datatake_id={self._mission_datatake_id}, \
product_unique_id={self._product_unique_id}'

    def __repr__(self):
        return f'S1PrdIdInfo(s1_prd_id={self._s1_prd_id})'

    @staticmethod
    def is_valid(s1_prd_id): 
        try:
            S1PrdIdInfo(s1_prd_id)
            return True
        except ValueError:
            return False
