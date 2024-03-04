'''
Use multiple data sources to generate privately owned grass and shrub pixels
for EPA-NLUD.
'''
import os

import arcpy as ap

from arcpy.sa import Con, IsNull


class PrivateGrassShrub():
    '''
    Use multiple data sources to generate privately owned grass and shrub
    pixels for EPA-NLUD
    '''
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.nlcd_ras = kwargs['nlcd_ras']

        self.raster_value = kwargs['RASTER_VALUES']['PrivateGrassShrub']

        self.create_nlcd_grass_shrub_raster()
        self.create_final_privategrassshrub_raster()

    def create_nlcd_grass_shrub_raster(self):
        '''
        Create a privately owned grass and shrub raster from NLCD 2011
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating grass/shrub from NLCD 2011...")

        nlcd_grass_shrub = Con((self.nlcd_ras == 52) | (self.nlcd_ras == 71), self.raster_value)
        out_ras = os.path.join(self.INTERMEDIATE, 'NLCD_GRASS_SHRUB')
        nlcd_grass_shrub.save(out_ras)

    def create_final_privategrassshrub_raster(self):
        '''
        Create the final privately owned grass and shrub raster
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating final private grass/shrub raster...")

        grass_shrub_ras = ap.Raster(os.path.join(self.INTERMEDIATE, 'NLCD_GRASS_SHRUB'))

        final_privategrassshrub_ras = Con(~IsNull(self.nlcd_ras), grass_shrub_ras)
        final_privategrassshrub_ras.save(os.path.join(self.OUTPUTS, 'FINAL_PRIVATEGRASSSHRUB'))
