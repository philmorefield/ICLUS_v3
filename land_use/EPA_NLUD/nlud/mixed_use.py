'''
Use multiple data sources to generate mixed use pixels for EPA-NLUD.
'''
import os

from arcpy.sa import Con, IsNull


class MixedUse():
    '''
    Use multiple data sources to generate mixed use pixels for EPA-NLUD.
    '''
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.comm = kwargs['comm']
        self.comm_bg = kwargs['comm_bg']

        self.huden = kwargs['huden']
        self.huden_bg = kwargs['huden_bg']

        self.nlcd_ras = kwargs['nlcd_ras']
        self.urban_areas = kwargs['urban_areas']

        self.raster_value = kwargs['RASTER_VALUES']['MixedUse']

        self.create_mixed_use_raster()

    def create_mixed_use_raster(self):
        '''
        Create a raster of mixed use (i.e., commercial and residential)
        '''
        final_mixed_use_raster = Con((~IsNull(self.nlcd_ras)) & (~IsNull(self.urban_areas)), Con((self.huden > 5.0) & (self.comm > 5.0) & (self.nlcd_ras >= 23) & (self.nlcd_ras <= 24), self.raster_value))
        out_ras = os.path.join(self.OUTPUTS, 'FINAL_MIXEDUSE')
        final_mixed_use_raster.save(out_ras)

        final_mixed_use_raster_bg = Con((~IsNull(self.nlcd_ras)) & (~IsNull(self.urban_areas)), Con((self.huden_bg > 5.0) & (self.comm_bg > 5.0) & (self.nlcd_ras >= 23) & (self.nlcd_ras <= 24), self.raster_value))
        out_ras = os.path.join(self.OUTPUTS, 'FINAL_MIXEDUSE_BG')
        final_mixed_use_raster_bg.save(out_ras)
