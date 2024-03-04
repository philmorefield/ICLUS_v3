'''
Use multiple data sources to generate urban residential pixels for EPA-NLUD.
'''
import os

import arcpy as ap

from arcpy.sa import Con, IsNull


class Urban():
    '''
    Use multiple data sources to generate urban residential pixels for EPA-NLUD
    '''
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.huden = kwargs['huden']
        self.huden_bg = kwargs['huden_bg']
        self.nlcd_ras = kwargs['nlcd_ras']

        self.raster_value = kwargs['RASTER_VALUES']['Urban']
        self.create_silvis_urban_raster()
        self.create_final_urban_raster()

    def create_silvis_urban_raster(self):
        '''
        Create an urban residential raster from UW Silvis Census blocks
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating urban residential raster from UW Silvis blocks...")

        urban_residential = Con((self.huden > 4.0) & (self.huden <= 10.0), self.raster_value)
        urban_residential.save(os.path.join(self.INTERMEDIATE, 'SILVIS_URBAN'))

        urban_residential_bg = Con((self.huden_bg > 4.0) & (self.huden_bg <= 10.0), self.raster_value)
        urban_residential_bg.save(os.path.join(self.INTERMEDIATE, 'SILVIS_URBAN_BG'))

    def create_final_urban_raster(self):
        '''
        Create the final urban residential raster
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating final urban residential raster...")

        urban_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'SILVIS_URBAN'))
        urban_raster_bg = ap.Raster(os.path.join(self.INTERMEDIATE, 'SILVIS_URBAN_BG'))

        final_urban_raster = Con((~IsNull(self.nlcd_ras)) & (~IsNull(urban_raster)), urban_raster)
        final_urban_raster.save(os.path.join(self.OUTPUTS, 'FINAL_URBAN'))

        final_urban_raster_bg = Con((~IsNull(self.nlcd_ras)) & (~IsNull(urban_raster_bg)), urban_raster_bg)
        final_urban_raster_bg.save(os.path.join(self.OUTPUTS, 'FINAL_URBAN_BG'))
