from datetime import datetime

class S2PrdIdInfo:

    FORMAT_DATETIME='%Y%m%dT%H%M%S'

    def __init__(self, s2_prd_id) -> None:
        # S2A_MSIL1C_20170105T013442_N0204_R031_T53NMJ_20170105T013443.SAFE
        # https://sentinels.copernicus.eu/web/sentinel/user-guides/sentinel-2-msi/naming-convention
        s2_prod_id_wsafe=s2_prd_id.split('.')[0]
        self._s2_prd_id=s2_prod_id_wsafe
        elt_prd_id = self._s2_prd_id.split('_')
        if len(elt_prd_id) == 7:
            self.mission_id = elt_prd_id[0]
            self.product_level = elt_prd_id[1][3:]
            self.datatake_sensing_start_time = elt_prd_id[2]
            self.pdgs_processing_baseline_number = elt_prd_id[3][1:5]
            self.relative_orbit_number = elt_prd_id[4][1:4]
            self.tile_id = elt_prd_id[5][1:6]
            self.product_discriminator= elt_prd_id[6]
        else:
            raise ValueError('Sentinel 2 product id not provides the 7 keys values requested!')

    @property
    def product_discriminator(self):
        return self._product_discriminator

    @product_discriminator.setter
    def product_discriminator(self, value):
        if len(value) == 15:
            self._product_discriminator = value
        else:
            raise ValueError("Length of product discriminator different than 15 is not possible!")


    @property
    def tile_id(self):
        return self._tile_id

    # Check with the tile_id more precisely
    @tile_id.setter
    def tile_id(self, value):
        if len(value) == 5:
            self._tile_id = value
        else:
            raise ValueError("Length of tile id different than 5 is not possible!")

    @property
    def relative_orbit_number(self):
        return self._relative_orbit_number

    @relative_orbit_number.setter
    def relative_orbit_number(self, value):
        if len(value) == 3:
            if int(value) >= 1 and int(value) <=143:
                self._relative_orbit_number = value
            else:
                raise ValueError("Relative orbit number %s is not possible ([001, R143])!", value)
        else:
            raise ValueError("Length of relative orbit number different than 3 is not possible!", value)

    @property
    def pdgs_processing_baseline_number(self):
        return self._pdgs_processing_baseline_number

    @pdgs_processing_baseline_number.setter
    def pdgs_processing_baseline_number(self, value):
        self._pdgs_processing_baseline_number = value

    @property
    def datatake_sensing_start_time(self):
        return self._datatake_sensing_start_time

    @datatake_sensing_start_time.setter
    def datatake_sensing_start_time(self, value):
        self._datatake_sensing_start_time = datetime.strptime(value, self.FORMAT_DATETIME)
        
    @property
    def product_level(self):
        return self._product_level

    @product_level.setter
    def product_level(self, value):
        allowed_values= ['L1C', 'L2A']
        if value in allowed_values:
            self._product_level = value
        else:
            raise ValueError("Product level different than " + ', '.join(allowed_values) + " is not possible!")

    @property
    def mission_id(self):
        return self._mission_id

    @mission_id.setter
    def mission_id(self, value):
        allowed_values= ['S2A', 'S2B']
        if value in allowed_values:
            self._mission_id = value
        else:
            raise ValueError("Mission ID different than "+ ', '.join(allowed_values) + " is not possible!")

    def __str__(self):
        return f'Info provided by the S2 product id are: mission_id={self.mission_id}, \
product_level={self.product_level},  \
datatake_sensing_start_time={self.datatake_sensing_start_time}, \
pdgs_processing_baseline_number={self.pdgs_processing_baseline_number}, \
relative_orbit_number={self.relative_orbit_number}, tile_id={self.tile_id}, \
product_discriminator={self.product_discriminator}'

    def __repr__(self):
         return f'S2PrdIdInfo(s2_prd_id={self._s2_prd_id})'

    @staticmethod
    def is_valid(s2_prd_id): 
        try:
            S2PrdIdInfo(s2_prd_id)
            return True
        except ValueError:
            return False
      
if __name__ == "__main__":
    print(S2PrdIdInfo('S2B_MSIL1C_20210714T235249_N0301_R130_T57KUR_20210715T005654.SAFE'))
    print(S2PrdIdInfo('S2B_MSIL2A_20210714T131719_N0301_R124_T28WDB_20210714T160455.SAFE'))
    print(S2PrdIdInfo.is_valid('S2B_MSIL2_20210714T131719_N0301_R124_T28WDB_20210714T160455.SAFE'))
    print(S2PrdIdInfo.is_valid('S2B_MSIL1C_20210714T235249_N0301_R200_T57KUR_20210715T005654.SAFE'))