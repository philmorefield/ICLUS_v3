'''
Use multiple data sources to generate wetlands pixels for EPA NLUD.
'''
import os

import arcpy as ap

from arcpy.sa import CellStatistics, Con, IsNull


class Wetlands():
    '''
    Use multiple data sources to generate wetlands pixels for EPA NLUD
    '''
    def __init__(self, kwargs):
        self.region_number = kwargs['REGION_NUMBER']
        self.inputs = kwargs['INPUTS']
        self.outputs = kwargs['OUTPUTS']
        self.intermediate = kwargs['INTERMEDIATE']

        self.nlcd_ras = kwargs['nlcd_ras']

        self.raster_value = kwargs['RASTER_VALUES']['Wetlands']

        self.create_nhd_wetlands_raster()
        self.create_tiger_wetlands_raster()
        self.create_nlcd_wetlands_raster()
        self.create_final_wetlands_raster()

    def create_nhd_wetlands_raster(self):
        '''
        Create a wetlands raster from NHDPlusv2
        '''
        print(f"[Region {self.region_number}]   Creating wetlands raster from NHDPlusv2...")
        nhd = os.path.join(self.inputs, 'NHDWaterbody_proj')
        nhd_query = "FTYPE = 'SwampMarsh' Or \
                     FTYPE = 'InundationArea' Or \
                     FTYPE = 'Playa'"
        nhd_output_fc = 'in_memory\\nhd_wetlands'

        nhd_wetlands = ap.Select_analysis(in_features=nhd,
                                          out_feature_class=nhd_output_fc,
                                          where_clause=nhd_query)

        ap.AddField_management(in_table=nhd_wetlands,
                               field_name='WETLANDS_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=nhd_wetlands,
                                     field='WETLANDS_TRUE',
                                     expression=self.raster_value,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.intermediate, 'NHD_WETLANDS')
        ap.PolygonToRaster_conversion(in_features=nhd_wetlands,
                                      value_field='WETLANDS_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_tiger_wetlands_raster(self):
        '''
        Create a wetlands raster from TIGER2010
        '''
        print(f"[Region {self.region_number}]   Creating wetlands raster from TIGER2010...")
        tiger_wetlands = os.path.join(self.inputs, 'TIGER2010_areawater_proj')
        tiger_query = "MTFCC = 'H2025'"
        tiger_output_fc = 'in_memory\\tiger_wetlands'

        tiger_wetlands = ap.Select_analysis(in_features=tiger_wetlands,
                                            out_feature_class=tiger_output_fc,
                                            where_clause=tiger_query)

        ap.AddField_management(in_table=tiger_wetlands,
                               field_name='WETLANDS_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=tiger_wetlands,
                                     field='WETLANDS_TRUE',
                                     expression=self.raster_value,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.intermediate, 'TIGER_WETLANDS')
        ap.PolygonToRaster_conversion(in_features=tiger_wetlands,
                                      value_field='WETLANDS_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_nlcd_wetlands_raster(self):
        '''
        Create a wetlands raster from NLCD 2011
        '''
        print(f"[Region {self.region_number}]   Creating wetlands from from NLCD 2011...")

        nlcd_wetlands = Con((self.nlcd_ras == 90) | (self.nlcd_ras == 95), self.raster_value)
        out_ras = os.path.join(self.intermediate, 'NLCD_WETLANDS')
        nlcd_wetlands.save(out_ras)

    def create_final_wetlands_raster(self):
        '''
        Create the final wetlands raster
        '''
        print(f"[Region {self.region_number}]   Creating final wetlands raster...")
        nhd_wetlands = os.path.join(self.intermediate, 'NHD_WETLANDS')
        tiger_wetlands = os.path.join(self.intermediate, 'TIGER_WETLANDS')
        nlcd_wetlands = os.path.join(self.intermediate, 'NLCD_WETLANDS')

        wetlands_rasters = [nhd_wetlands,
                            tiger_wetlands,
                            nlcd_wetlands]

        sum_wetlands_rasters = CellStatistics(in_rasters_or_constants=wetlands_rasters,
                                              statistics_type='SUM',
                                              ignore_nodata='DATA')

        final_wetlands_raster = Con((~IsNull(self.nlcd_ras)) & (sum_wetlands_rasters >= (self.raster_value * 2)), self.raster_value)
        out_ras = os.path.join(self.outputs, 'FINAL_WETLANDS')
        final_wetlands_raster.save(out_ras)
