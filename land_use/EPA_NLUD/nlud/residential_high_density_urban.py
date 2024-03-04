'''
Use multiple data sources to generate high-density urban residential pixels for
EPA-NLUD.
'''
import os

import arcpy as ap

from arcpy.sa import Con, IsNull


class HighDensityUrban():
    '''
    Use multiple data sources to generate high-density urban residential pixels
    for EPA-NLUD
    '''
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.huden = kwargs['huden']
        self.huden_bg = kwargs['huden_bg']
        self.nlcd_ras = kwargs['nlcd_ras']

        self.raster_value = kwargs['RASTER_VALUES']['HighDensityUrban']

        self.create_silvis_high_density_urban_raster()
        self.create_final_high_density_urban_raster()

    def create_silvis_high_density_urban_raster(self):
        '''
        Create a high-density residential raster from the UW Silvis Census blocks
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating high-density urban residential raster from UW Silvis blocks...")

        high_density_urban_residential = Con(self.huden > 10.0, self.raster_value)
        high_density_urban_residential.save(os.path.join(self.INTERMEDIATE, 'SILVIS_HIGH_DENSITY_URBAN'))

        high_density_urban_residential_bg = Con(self.huden_bg > 10.0, self.raster_value)
        high_density_urban_residential_bg.save(os.path.join(self.INTERMEDIATE, 'SILVIS_HIGH_DENSITY_URBAN_BG'))

    def create_final_high_density_urban_raster(self):
        '''
        Create the final high-density urban raster
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating final high-density urban residential raster...")
        high_density_urban_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'SILVIS_HIGH_DENSITY_URBAN'))
        high_density_urban_raster_bg = ap.Raster(os.path.join(self.INTERMEDIATE, 'SILVIS_HIGH_DENSITY_URBAN_BG'))

        final_high_density_urban_raster = Con((~IsNull(self.nlcd_ras)) & (~IsNull(high_density_urban_raster)), high_density_urban_raster)
        final_high_density_urban_raster.save(os.path.join(self.OUTPUTS, 'FINAL_HIGHDENSITYURBAN'))

        final_high_density_urban_raster_bg = Con((~IsNull(self.nlcd_ras)) & (~IsNull(high_density_urban_raster_bg)), high_density_urban_raster_bg)
        final_high_density_urban_raster_bg.save(os.path.join(self.OUTPUTS, 'FINAL_HIGHDENSITYURBAN_BG'))
