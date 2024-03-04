'''
Use multiple data sources to generate privately owned forest pixels for EPA-NLUD.
'''
import os

import arcpy as ap

from arcpy.sa import Con, IsNull


class PrivateForest():
    '''
    Use multiple data sources to generate privately owned forest pixels for EPA-NLUD
    '''
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.nlcd_ras = kwargs['nlcd_ras']

        self.raster_value = kwargs['RASTER_VALUES']['PrivateForest']

        self.create_nlcd_forest_raster()
        self.create_final_privateforest_raster()

    def create_nlcd_forest_raster(self):
        '''
        Create a privately owned forest raster from NLCD 2011
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating forest from from NLCD 2011...")

        nlcd_forest = Con((self.nlcd_ras == 41) | (self.nlcd_ras == 42) | (self.nlcd_ras == 43), self.raster_value)
        out_ras = os.path.join(self.INTERMEDIATE, 'NLCD_FOREST')
        nlcd_forest.save(out_ras)

    def create_final_privateforest_raster(self):
        '''
        Create the final privately owned forest raster
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating final private forest raster...")

        forest_ras = ap.Raster(os.path.join(self.INTERMEDIATE, 'NLCD_FOREST'))

        final_privateforest_ras = Con(~IsNull(self.nlcd_ras), forest_ras)
        final_privateforest_ras.save(os.path.join(self.OUTPUTS, 'FINAL_PRIVATEFOREST'))
