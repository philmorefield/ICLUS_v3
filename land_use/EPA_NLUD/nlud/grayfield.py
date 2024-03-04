'''
Use multiple data sources to generate grayfield pixels for EPA-NLUD.
'''
import os

import arcpy as ap

from arcpy.sa import Con, IsNull


class Grayfield():
    '''
    Use multiple data sources to generate grayfield pixels for EPA-NLUD.

    Calculated at Block Group level only.
    '''
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.huden = kwargs['huden']
        self.huden_bg = kwargs['huden_bg']

        self.jobden = kwargs['jobden']
        self.jobden_bg = kwargs['jobden_bg']

        self.popden = kwargs['popden']
        self.popden_bg = kwargs['popden_bg']

        self.nlcd_ras = kwargs['nlcd_ras']
        self.place = kwargs['place']
        self.urban_areas = kwargs['urban_areas']

        self.raster_value = kwargs['RASTER_VALUES']['Grayfield']

        self.create_silvis_grayfield_raster()
        self.create_final_grayfield_raster()

    def create_silvis_grayfield_raster(self):
        '''
        Create a raster of grayfield areas from UW Silvis Census blocks and LEHD
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating grayfield residential raster from UW Silvis blocks...")

        grayfield = Con(~IsNull(self.urban_areas), Con((self.huden > 0.0) & (self.popden == 0.0) & (self.jobden == 0.0), Con((self.nlcd_ras == 23) | (self.nlcd_ras == 24), self.raster_value)))
        grayfield.save(os.path.join(self.INTERMEDIATE, 'SILVIS_GRAYFIELD'))

        # grayfield = Con(~IsNull(self.urban_areas), Con((self.huden_bg > 0.5) & (self.popden_bg < 0.1), self.raster_value, Con((self.nlcd_ras == 23) | (self.nlcd_ras == 24), Con(self.jobden_bg < 0.01, self.raster_value))))
        # grayfield = Con(~IsNull(self.urban_areas), Con((self.huden_bg > 0.0) & (self.popden_bg == 0.0) & (self.jobden_bg == 0.0), Con((self.nlcd_ras == 23) | (self.nlcd_ras == 24), self.raster_value)))
        # grayfield.save(os.path.join(self.INTERMEDIATE, 'SILVIS_GRAYFIELD_BG'))

    def create_final_grayfield_raster(self):
        '''
        Create the final raster of grayfield areas
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating final grayfield residential raster...")
        grayfield_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'SILVIS_GRAYFIELD'))
        # grayfield_raster_bg = ap.Raster(os.path.join(self.INTERMEDIATE, 'SILVIS_GRAYFIELD_BG'))

        final_grayfield_raster = Con((~IsNull(self.nlcd_ras)) & (~IsNull(grayfield_raster)), grayfield_raster)
        final_grayfield_raster.save(os.path.join(self.OUTPUTS, 'FINAL_GRAYFIELD'))

        # final_grayfield_raster_bg = Con((~IsNull(self.nlcd_ras)) & (~IsNull(grayfield_raster_bg)), grayfield_raster_bg)
        # final_grayfield_raster_bg.save(os.path.join(self.OUTPUTS, 'FINAL_GRAYFIELD_BG'))
