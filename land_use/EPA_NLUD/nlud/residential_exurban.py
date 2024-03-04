'''
Use multiple data sources to generate exurban residential pixels for EPA-NLUD.
'''
import os

import arcpy as ap

from arcpy.sa import Con


class Exurban():
    '''
    Use multiple data sources to generate exurban residential pixels for EPA-NLUD
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

        self.raster_value = kwargs['RASTER_VALUES']['Exurban']

        self.create_silvis_exurban_raster()
        self.create_final_exurban_raster()

    def create_silvis_exurban_raster(self):
        '''
        Create an exurban residential raster from UW Silvis Censu blocks
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating exurban residential raster from UW Silvis blocks...")

        exurban_residential = Con((self.huden > 0.1) & (self.huden < 0.5), self.raster_value)
        exurban_residential.save(os.path.join(self.INTERMEDIATE, 'SILVIS_EXURBAN'))

        exurban_residential_bg = Con((self.huden_bg > 0.1) & (self.huden_bg < 0.5), self.raster_value)
        exurban_residential_bg.save(os.path.join(self.INTERMEDIATE, 'SILVIS_EXURBAN_BG'))

    def create_final_exurban_raster(self):
        '''
        Create the final exurban residential raster
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating final exurban residential raster...")
        exurban_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'SILVIS_EXURBAN'))
        exurban_raster_bg = ap.Raster(os.path.join(self.INTERMEDIATE, 'SILVIS_EXURBAN_BG'))

        final_exurban_raster = Con((self.nlcd_ras != 81) & (self.nlcd_ras != 82), exurban_raster)
        final_exurban_raster.save(os.path.join(self.OUTPUTS, 'FINAL_EXURBAN'))

        final_exurban_raster_bg = Con((self.nlcd_ras != 81) & (self.nlcd_ras != 82), exurban_raster_bg)
        final_exurban_raster_bg.save(os.path.join(self.OUTPUTS, 'FINAL_EXURBAN_BG'))
