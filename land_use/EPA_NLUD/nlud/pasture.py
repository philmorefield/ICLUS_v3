'''
Use multiple data sources to generate pasture pixels for EPA-NLUD.
'''
import os

import arcpy as ap

from arcpy.sa import Con, IsNull


class Pasture():
    '''
    Use multiple data sources to generate pasture pixels for EPA-NLUD
    '''
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.nlcd_ras = kwargs['nlcd_ras']

        self.raster_value = kwargs['RASTER_VALUES']['Pasture']

        self.create_nlcd_pasture_raster()
        self.create_final_pasture_raster()

    def create_nlcd_pasture_raster(self):
        '''
        Create a pasture raster from NLCD 2011
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating pasture from NLCD 2011...")

        nlcd_pasture = Con(self.nlcd_ras == 81, self.raster_value)
        out_ras = os.path.join(self.INTERMEDIATE, 'NLCD_PASTURE')
        nlcd_pasture.save(out_ras)

    def create_final_pasture_raster(self):
        '''
        Create the final pasture raster
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating pasture raster...")

        pasture_ras = ap.Raster(os.path.join(self.INTERMEDIATE, 'NLCD_PASTURE'))

        final_pasture_ras = Con(~IsNull(self.nlcd_ras), pasture_ras)
        final_pasture_ras.save(os.path.join(self.OUTPUTS, 'FINAL_PASTURE'))
