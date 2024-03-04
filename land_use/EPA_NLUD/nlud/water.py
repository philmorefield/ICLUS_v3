'''
Use multiple data sources to generate water pixels for EPA NLUD.
'''
import os

import arcpy as ap

from arcpy.sa import CellStatistics, Con, IsNull


class Water():
    '''
    Use multiple data sources to generate water pixels for EPA NLUD.
    '''
    def __init__(self, kwargs):
        self.region_number = kwargs['REGION_NUMBER']
        self.inputs = kwargs['INPUTS']
        self.outputs = kwargs['OUTPUTS']
        self.intermediate = kwargs['INTERMEDIATE']

        self.nlcd_ras = kwargs['nlcd_ras']

        self.raster_value = kwargs['RASTER_VALUES']['Water']

        self.create_nhd_water_raster()
        self.create_tiger_water_raster()
        self.create_nlcd_water_raster()
        self.create_padus_water_raster()
        self.create_final_water_raster()

    def create_nhd_water_raster(self):
        '''
        Create a water raster from NHDPlusv2
        '''
        print(f"[Region {self.region_number}]   Creating water raster from NHDPlusv2...")
        nhd = os.path.join(self.inputs, 'NHDWaterbody_proj')
        nhd_query = "FTYPE = 'LakePond' Or \
                     FTYPE = 'Reservoir' Or \
                     FTYPE = 'Estuary'"
        nhd_output_fc = 'in_memory\\nhd_water'

        nhd_water = ap.Select_analysis(in_features=nhd,
                                       out_feature_class=nhd_output_fc,
                                       where_clause=nhd_query)

        ap.AddField_management(in_table=nhd_water,
                               field_name='WATER_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=nhd_water,
                                     field='WATER_TRUE',
                                     expression=self.raster_value,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.intermediate, 'NHD_WATER')
        ap.PolygonToRaster_conversion(in_features=nhd_water,
                                      value_field='WATER_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_tiger_water_raster(self):
        '''
        Create a water raster from TIGER2010
        '''
        print(f"[Region {self.region_number}]   Creating water raster from TIGER2010...")
        tiger_water = os.path.join(self.inputs, 'TIGER2010_areawater_proj')
        tiger_query = "MTFCC = 'H2030' Or \
                       MTFCC = 'H2040' Or \
                       MTFCC = 'H2051' Or \
                       MTFCC = 'H2053' Or \
                       MTFCC = 'H2060' Or \
                       MTFCC = 'H3010' Or \
                       MTFCC = 'H3013' Or \
                       MTFCC = 'H3020'"
        tiger_output_fc = 'in_memory\\tiger_water'

        tiger_water = ap.Select_analysis(in_features=tiger_water,
                                         out_feature_class=tiger_output_fc,
                                         where_clause=tiger_query)

        ap.AddField_management(in_table=tiger_water,
                               field_name='WATER_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=tiger_water,
                                     field='WATER_TRUE',
                                     expression=self.raster_value,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.intermediate, 'TIGER_WATER')
        ap.PolygonToRaster_conversion(in_features=tiger_water,
                                      value_field='WATER_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_nlcd_water_raster(self):
        '''
        Create a water raster from NLCD 2011
        '''
        print(f"[Region {self.region_number}]   Creating water raster from NLCD 2011...")
        nlcd_water = Con(self.nlcd_ras == 11, 1)
        out_ras = os.path.join(self.intermediate, 'NLCD_WATER')
        nlcd_water.save(out_ras)

    def create_padus_water_raster(self):
        '''
        Create a water raster from PAD-US
        '''
        print(f"[Region {self.region_number}]   Creating water raster from PAD-US...")

        padus = os.path.join(self.inputs, 'PADUS_14_Combined_proj')
        padus_query = "Loc_Nm LIKE '%Lake%' Or Loc_Nm LIKE '%Reservoir%'"  # potential water areas
        padus_output_fc = 'in_memory\\water'

        padus_water = ap.Select_analysis(in_features=padus,
                                         out_feature_class=padus_output_fc,
                                         where_clause=padus_query)

        ap.AddField_management(in_table=padus_water,
                               field_name='WATER_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=padus_water,
                                     field='WATER_TRUE',
                                     expression=self.raster_value,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.intermediate, 'PADUS_WATER')
        ap.PolygonToRaster_conversion(in_features=padus_water,
                                      value_field='WATER_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_final_water_raster(self):
        '''
        Create the final water raster

        User's Accuracy of NLCD Water pixels ~90%. The Silvis water blocks are
        hand-digited. We use both of those sources to define water pixels
        without any qualification. We then add water pixels anywhere at least
        two of the other data sources (NHD, Tiger, PAD-US) indicate water.
        '''
        print(f"[Region {self.region_number}]   Creating final water raster...")

        nhd_water = os.path.join(self.intermediate, 'NHD_WATER')
        tiger_water = os.path.join(self.intermediate, 'TIGER_WATER')
        nlcd_water = os.path.join(self.intermediate, 'NLCD_WATER')
        padus_water = os.path.join(self.intermediate, 'PADUS_WATER')
        silvis_water = os.path.join(self.inputs, 'CONUS_blk10_Census_change_1990_2010_PLA2_WATER')

        water_rasters = [nhd_water,
                         tiger_water,
                         padus_water]

        sum_water_rasters = CellStatistics(in_rasters_or_constants=water_rasters,
                                           statistics_type='SUM',
                                           ignore_nodata='DATA')

        nhd_tiger_padus_water = Con(sum_water_rasters >= self.raster_value * 2, self.raster_value)
        del sum_water_rasters
        nhd_tiger_padus_water.save(os.path.join(self.intermediate, 'NHD_TIGER_PADUS_WATER'))

        final_water_raster = Con(~IsNull(self.nlcd_ras), Con((~IsNull(silvis_water)) | (~IsNull(nlcd_water)) | (~IsNull(nhd_tiger_padus_water)), self.raster_value))
        out_ras = os.path.join(self.outputs, 'FINAL_WATER')
        final_water_raster.save(out_ras)
