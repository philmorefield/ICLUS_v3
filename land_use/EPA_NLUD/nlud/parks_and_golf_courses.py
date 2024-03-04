'''
Use multiple data sources to generate parks and golf course pixels for EPA-NLUD.
'''
import os

import arcpy as ap

from arcpy.sa import Con, IsNull


class ParksGolf():
    '''
    Use multiple data sources to generate parks and golf course pixels for EPA-NLUD
    '''
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.nlcd_ras = kwargs['nlcd_ras']
        self.urban_areas = kwargs['urban_areas']

        self.raster_value = kwargs['RASTER_VALUES']['ParksGolf']

        self.create_padus_parksgolf_raster()
        self.create_tiger_parksgolf_raster()
        self.create_esri_parksgolf_raster()
        self.create_navteq_parksgolf_raster()
        self.create_final_parksgolf_raster()

    def create_padus_parksgolf_raster(self):
        '''
        Create a park and golf course raster from PAD-US
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating parks and golf course raster from PAD-US...")

        padus = os.path.join(self.INPUTS, 'PADUS_14_Combined_proj')
        padus_query = "Des_Tp = 'LP' Or Des_Tp = 'LREC'"  # Local Parks and Local Recreation Areas
        padus_output_fc = 'in_memory\\padus_parksgolf'

        padus_parksgolf = ap.Select_analysis(in_features=padus,
                                             out_feature_class=padus_output_fc,
                                             where_clause=padus_query)

        ap.AddField_management(in_table=padus_parksgolf,
                               field_name='PARKSGOLF_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=padus_parksgolf,
                                     field='PARKSGOLF_TRUE',
                                     expression=self.raster_value,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.INTERMEDIATE, 'PADUS_PARKSGOLF')
        ap.PolygonToRaster_conversion(in_features=padus_parksgolf,
                                      value_field='PARKSGOLF_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_tiger_parksgolf_raster(self):
        '''
        Create a parks and golf course raster from TIGER2010

        K1228 --> Campground
        K2180 --> Park
        K2181 --> National Park Service Land (only used in urban areas)
        K2561 --> Golf course
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating parks and golf course raster from TIGER2010...")

        # parks, camp grounds, and golf courses
        tiger_arealm = os.path.join(self.INPUTS, 'TIGER2010_arealm_proj')
        tiger_query = "MTFCC = 'K1228' Or \
                       MTFCC = 'K2180' Or \
                       MTFCC = 'K2561'"
        tiger_output_fc = 'in_memory\\tiger_parksgolf'

        tiger_parksgolf = ap.Select_analysis(in_features=tiger_arealm,
                                             out_feature_class=tiger_output_fc,
                                             where_clause=tiger_query)

        ap.AddField_management(in_table=tiger_parksgolf,
                               field_name='PARKS_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=tiger_parksgolf,
                                     field='PARKS_TRUE',
                                     expression=self.raster_value,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.INTERMEDIATE, 'TIGER_PARKSGOLF')
        ap.PolygonToRaster_conversion(in_features=tiger_parksgolf,
                                      value_field='PARKS_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

        # NPS lands in urban areas
        nps_query = "MTFCC = 'K2181'"
        nps_output_fc = 'in_memory\\nps_parks'

        nps_parks = ap.Select_analysis(in_features=tiger_arealm,
                                       out_feature_class=nps_output_fc,
                                       where_clause=nps_query)

        ap.AddField_management(in_table=nps_parks,
                               field_name='PARKS_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=nps_parks,
                                     field='PARKS_TRUE',
                                     expression=self.raster_value,
                                     expression_type='PYTHON3')

        out_rasterdataset = os.path.join(self.INTERMEDIATE, 'NPS_PARKS')
        ap.PolygonToRaster_conversion(in_features=nps_parks,
                                      value_field='PARKS_TRUE',
                                      out_rasterdataset=out_rasterdataset,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

        nps_parks_ras = ap.Raster(out_rasterdataset)
        urban_nps = ap.sa.Con(~IsNull(self.urban_areas), nps_parks_ras)

        ap.Mosaic_management(inputs=[urban_nps],
                             target=out_ras)

    def create_esri_parksgolf_raster(self):
        '''
        Create a parks and golf course raster from ESRI_2010_lalndmark

        D81 --> Golf course
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating parks and golf course raster from ESRI_2010_lalndmark...")

        esri_arealm = os.path.join(self.INPUTS, 'ESRI_2010_lalndmrk_proj')
        esri_query = "FCC = 'D81'"
        esri_output_fc = 'in_memory\\esri_parksgolf'

        esri_parksgolf = ap.Select_analysis(in_features=esri_arealm,
                                            out_feature_class=esri_output_fc,
                                            where_clause=esri_query)

        ap.AddField_management(in_table=esri_parksgolf,
                               field_name='PARKS_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=esri_parksgolf,
                                     field='PARKS_TRUE',
                                     expression=self.raster_value,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.INTERMEDIATE, 'ESRI_PARKSGOLF')
        ap.PolygonToRaster_conversion(in_features=esri_parksgolf,
                                      value_field='PARKS_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_navteq_parksgolf_raster(self):
        '''
        Create a parks and golf course raster from Navteq 2011
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating parks and golf course raster from Navteq 2011...")

        # Small city/county parks from LandUseA
        navteq_landuse_A = os.path.join(self.INPUTS, 'Navteq_2011_LandUseA_proj')
        navteq_query = "FEAT_TYPE = 'PARK (CITY/COUNTY)'"
        navteq_output_fc = 'in_memory\\navteq_A_parksgolf'
        navteq_parksgolf = ap.Select_analysis(in_features=navteq_landuse_A,
                                              out_feature_class=navteq_output_fc,
                                              where_clause=navteq_query)

        ap.AddField_management(in_table=navteq_parksgolf,
                               field_name='PARKSGOLF_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=navteq_parksgolf,
                                     field='PARKSGOLF_TRUE',
                                     expression=self.raster_value,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.INTERMEDIATE, 'NAVTEQ_A_PARKSGOLF')
        ap.PolygonToRaster_conversion(in_features=navteq_parksgolf,
                                      value_field='PARKSGOLF_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

        # Golf courses from LandUseB
        navteq_landuse_B = os.path.join(self.INPUTS, 'Navteq_2011_LandUseB_proj')
        navteq_query = "FEAT_TYPE = 'GOLF COURSE'"
        navteq_output_fc = 'in_memory\\navteq_B_parksgolf'
        navteq_parksgolf = ap.Select_analysis(in_features=navteq_landuse_B,
                                              out_feature_class=navteq_output_fc,
                                              where_clause=navteq_query)

        ap.AddField_management(in_table=navteq_parksgolf,
                               field_name='PARKSGOLF_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=navteq_parksgolf,
                                     field='PARKSGOLF_TRUE',
                                     expression=self.raster_value,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.INTERMEDIATE, 'NAVTEQ_B_PARKSGOLF')
        ap.PolygonToRaster_conversion(in_features=navteq_parksgolf,
                                      value_field='PARKSGOLF_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_final_parksgolf_raster(self):
        '''
        Create the final parks and golf course raster
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating final parks and golf raster...")

        padus_parks_ras = ap.Raster(os.path.join(self.INTERMEDIATE, 'PADUS_PARKSGOLF'))
        tiger_parks_ras = ap.Raster(os.path.join(self.INTERMEDIATE, 'TIGER_PARKSGOLF'))
        esri_parks_ras = ap.Raster(os.path.join(self.INTERMEDIATE, 'ESRI_PARKSGOLF'))
        navteq_A_parks_ras = ap.Raster(os.path.join(self.INTERMEDIATE, 'NAVTEQ_A_PARKSGOLF'))
        navteq_B_parks_ras = ap.Raster(os.path.join(self.INTERMEDIATE, 'NAVTEQ_B_PARKSGOLF'))

        final_parks_ras = Con(~IsNull(self.nlcd_ras), Con((~IsNull(tiger_parks_ras)) | (~IsNull(esri_parks_ras)) | (~IsNull(padus_parks_ras)) | (~IsNull(navteq_A_parks_ras)) | (~IsNull(navteq_B_parks_ras)), self.raster_value))
        final_parks_ras.save(os.path.join(self.OUTPUTS, 'FINAL_PARKSGOLF'))
