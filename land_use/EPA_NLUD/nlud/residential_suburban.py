'''
Use multiple data sources to generate suburban residential pixels for EPA-NLUD.
'''
import os

import arcpy as ap

from arcpy.sa import Con, IsNull


class Suburban():
    '''
    Use multiple data sources to generate suburban residential pixels for EPA-NLUD
    '''
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.huden = kwargs['huden']
        self.huden_bg = kwargs['huden_bg']
        self.nlcd_ras = kwargs['nlcd_ras']

        self.raster_value = kwargs['RASTER_VALUES']['Suburban']

        self.create_silvis_suburban_raster()
        self.create_final_suburban_raster()

    def create_silvis_suburban_raster(self):
        '''
        Create a residential suburban raster from UW Silvis Census blocks
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating suburban residential raster from UW Silvis blocks...")

        suburban_residential = Con((self.huden >= 0.5) & (self.huden <= 4.0), self.raster_value)
        suburban_residential.save(os.path.join(self.INTERMEDIATE, 'SILVIS_SUBURBAN'))

        suburban_residential_bg = Con((self.huden_bg >= 0.5) & (self.huden_bg <= 4.0), self.raster_value)
        suburban_residential_bg.save(os.path.join(self.INTERMEDIATE, 'SILVIS_SUBURBAN_BG'))

    def create_final_suburban_raster(self):
        '''
        Create the final suburban residential raster
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating final suburban residential raster...")
        suburban_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'SILVIS_SUBURBAN'))
        suburban_raster_bg = ap.Raster(os.path.join(self.INTERMEDIATE, 'SILVIS_SUBURBAN_BG'))

        final_suburban_raster = Con((~IsNull(self.nlcd_ras)) & (~IsNull(suburban_raster)), suburban_raster)
        final_suburban_raster.save(os.path.join(self.OUTPUTS, 'FINAL_SUBURBAN'))

        final_suburban_raster_bg = Con((~IsNull(self.nlcd_ras)) & (~IsNull(suburban_raster_bg)), suburban_raster)
        final_suburban_raster_bg.save(os.path.join(self.OUTPUTS, 'FINAL_SUBURBAN_BG'))
