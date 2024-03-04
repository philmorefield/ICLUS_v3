'''
Use multiple data sources to generate rural residential pixels for EPA-NLUD.
'''
import os

import arcpy as ap

from arcpy.sa import Con


class Rural():
    '''
    Use multiple data sources to generate rural residential pixels for EPA-NLUD
    '''
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.huden = kwargs['huden']
        self.huden_bg = kwargs['huden_bg']
        self.nlcd_ras = kwargs['nlcd_ras']
        self.place = kwargs['place']

        self.raster_value = kwargs['RASTER_VALUES']['Rural']

        self.create_silvis_rural_raster()
        self.create_final_rural_raster()

    def create_silvis_rural_raster(self):
        '''
        Create a rural residential raster from the UW Silvis Census blocks
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating rural residential raster from UW Silvis blocks...")

        rural_residential = Con((self.huden > 0.05) & (self.huden <= 0.1), self.raster_value)
        rural_residential.save(os.path.join(self.INTERMEDIATE, 'SILVIS_RURAL'))

        rural_residential_bg = Con((self.huden_bg > 0.05) & (self.huden_bg <= 0.1), self.raster_value)
        rural_residential_bg.save(os.path.join(self.INTERMEDIATE, 'SILVIS_RURAL_BG'))

    def create_final_rural_raster(self):
        '''
        Create the final rural residential raster
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating final rural residential raster...")

        rural_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'SILVIS_RURAL'))
        rural_raster_bg = ap.Raster(os.path.join(self.INTERMEDIATE, 'SILVIS_RURAL_BG'))

        final_rural_raster = Con((self.nlcd_ras != 81) & (self.nlcd_ras != 82), rural_raster)
        final_rural_raster.save(os.path.join(self.OUTPUTS, 'FINAL_RURAL'))

        final_rural_raster_bg = Con((self.nlcd_ras != 81) & (self.nlcd_ras != 82), rural_raster_bg)
        final_rural_raster_bg.save(os.path.join(self.OUTPUTS, 'FINAL_RURAL_BG'))
