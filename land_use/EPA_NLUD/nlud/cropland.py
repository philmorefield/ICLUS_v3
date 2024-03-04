'''
Use multiple data sources to generate cropland pixels for EPA-NLUD.
'''
import os

import arcpy as ap

from arcpy.sa import Con, IsNull


class Cropland():
    '''
    Use multiple data sources to generate cropland pixels for EPA-NLUD
    '''
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.nlcd_ras = kwargs['nlcd_ras']

        self.raster_value = kwargs['RASTER_VALUES']['Cropland']

        self.create_nlcd_cropland_raster()
        self.create_final_cropland_raster()

    def create_nlcd_cropland_raster(self):
        '''
        Create a cropland raster from NLCD 2011
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating cropland from NLCD 2011...")

        nlcd_cropland = Con(self.nlcd_ras == 82, self.raster_value)
        out_ras = os.path.join(self.INTERMEDIATE, 'NLCD_CROPLAND')
        nlcd_cropland.save(out_ras)

    def create_final_cropland_raster(self):
        '''
        Create the final cropland raster
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating final private cropland raster...")

        cropland_ras = ap.Raster(os.path.join(self.INTERMEDIATE, 'NLCD_CROPLAND'))

        final_cropland_ras = Con(~IsNull(self.nlcd_ras), cropland_ras)
        final_cropland_ras.save(os.path.join(self.OUTPUTS, 'FINAL_CROPLAND'))
