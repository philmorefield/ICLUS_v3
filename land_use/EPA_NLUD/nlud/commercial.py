'''
Use multiple data sources to generate commercial pixels for EPA-NLUD.
'''
import os

import arcpy as ap

from arcpy.sa import Con, IsNull


class Commercial():
    '''
    Use multiple data sources to generate commercial pixels for EPA-NLUD.
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

        self.indust = kwargs['indust']
        self.indust_bg = kwargs['indust_bg']

        self.nlcd_ras = kwargs['nlcd_ras']

        self.raster_value = kwargs['RASTER_VALUES']['Commercial']

        self.create_silvis_lehd_commerical_raster()
        self.create_census_commercial_raster()
        self.create_esri_commercial_raster()
        self.create_navteq_commercial_raster()
        self.create_final_commercial_raster()

    def create_silvis_lehd_commerical_raster(self):
        '''
        Create a raster of commercial land use from UW Silvis Census blocks and
        LEHD
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating commercial raster from UW Silvis blocks and LEHD...")

        lehd_commercial_raster = Con(self.comm > 0.0, Con((self.huden < 0.1) | (self.comm > (self.huden * 2)), Con(self.comm > self.indust, self.raster_value)))
        lehd_commercial_raster.save(os.path.join(self.INTERMEDIATE, 'LEHD_COMMERCIAL'))

        lehd_commercial_raster_bg = Con(self.comm_bg > 0.0, Con((self.huden_bg < 0.1) | (self.comm_bg > (self.huden_bg * 2)), Con(self.comm_bg > self.indust_bg, self.raster_value)))
        lehd_commercial_raster_bg.save(os.path.join(self.INTERMEDIATE, 'LEHD_COMMERCIAL_BG'))

    def create_census_commercial_raster(self):
        '''
        Create a raster of commercial land use from TIGER2020

        K2167 --> Convention Center
        K2300 --> Commercial Workplace
        K2361 --> Shopping Center
        K2363 --> Office Building or Office Park
        K2564 --> Amusement Center
        K2586 --> Zoo
        '''

        print(f"[Region {self.REGION_NUMBER}]   Creating commercial raster from TIGER2010...")
        tiger_landmarks = os.path.join(self.INPUTS, 'TIGER2010_arealm_proj')
        tiger_query = "MTFCC = 'K2167' Or \
                       MTFCC = 'K2300' Or \
                       MTFCC = 'K2361' Or \
                       MTFCC = 'K2363' Or \
                       MTFCC = 'K2564' Or \
                       MTFCC = 'K2586'"
        tiger_output_fc = 'in_memory\\tiger_commercial'

        tiger_commercial = ap.Select_analysis(in_features=tiger_landmarks,
                                              out_feature_class=tiger_output_fc,
                                              where_clause=tiger_query)

        ap.AddField_management(in_table=tiger_commercial,
                               field_name='COMMERCIAL_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=tiger_commercial,
                                     field='COMMERCIAL_TRUE',
                                     expression=self.raster_value,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.INTERMEDIATE, 'TIGER_COMMERCIAL')
        ap.PolygonToRaster_conversion(in_features=tiger_commercial,
                                      value_field='COMMERCIAL_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_esri_commercial_raster(self):
        '''
        Create a raster of commercial land use from ESRI_2010_lalndmrk

        D61 --> Shopping Center
        D64 --> Amusement Park
        D67 --> Stadium
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating commercial raster from ESRI_2010_lalndmrk_proj...")
        esri_landmarks = os.path.join(self.INPUTS, 'ESRI_2010_lalndmrk_proj')
        esri_query = "FCC = 'D61' Or \
                      FCC = 'D64' Or \
                      FCC = 'D67'"
        esri_output_fc = 'in_memory\\esri_commercial'

        esri_commercial = ap.Select_analysis(in_features=esri_landmarks,
                                             out_feature_class=esri_output_fc,
                                             where_clause=esri_query)

        ap.AddField_management(in_table=esri_commercial,
                               field_name='COMMERCIAL_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=esri_commercial,
                                     field='COMMERCIAL_TRUE',
                                     expression=self.raster_value,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.INTERMEDIATE, 'ESRI_COMMERCIAL')
        ap.PolygonToRaster_conversion(in_features=esri_commercial,
                                      value_field='COMMERCIAL_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_navteq_commercial_raster(self):
        '''
        Create a raster of commercial land use from Navteq 2011
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating commercial raster from Navteq 2011...")

        navteq_landuse_A = os.path.join(self.INPUTS, 'Navteq_2011_LandUseA_proj')
        navteq_query = "FEAT_TYPE = 'SHOPPING_CENTRE' Or \
                        FEAT_TYPE = 'AMUSEMENT PARK' Or \
                        FEAT_TYPE = 'ANIMAL_PARK' Or \
                        FEAT_TYPE = 'SPORTS COMPLEX'"
        navteq_output_fc = 'in_memory\\navteq_commercial'
        navteq_commercial = ap.Select_analysis(in_features=navteq_landuse_A,
                                               out_feature_class=navteq_output_fc,
                                               where_clause=navteq_query)

        ap.AddField_management(in_table=navteq_commercial,
                               field_name='COMMERCIAL_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=navteq_commercial,
                                     field='COMMERCIAL_TRUE',
                                     expression=self.raster_value,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.INTERMEDIATE, 'NAVTEQ_COMMERCIAL')
        ap.PolygonToRaster_conversion(in_features=navteq_commercial,
                                      value_field='COMMERCIAL_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_final_commercial_raster(self):
        '''
        Create the final raster of commercial land use
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating final commercial raster...")

        tiger_commercial = ap.Raster(os.path.join(self.INTERMEDIATE, 'TIGER_COMMERCIAL'))
        lehd_commercial = ap.Raster(os.path.join(self.INTERMEDIATE, 'LEHD_COMMERCIAL'))
        lehd_commercial_bg = ap.Raster(os.path.join(self.INTERMEDIATE, 'LEHD_COMMERCIAL_BG'))
        esri_commercial = ap.Raster(os.path.join(self.INTERMEDIATE, 'ESRI_COMMERCIAL'))
        navteq_commercial = ap.Raster(os.path.join(self.INTERMEDIATE, 'NAVTEQ_COMMERCIAL'))

        final_commercial_raster = Con(~IsNull(self.nlcd_ras), Con((self.nlcd_ras >= 21) & (self.nlcd_ras <= 24), Con((~IsNull(tiger_commercial)) | (~IsNull(lehd_commercial)) | (~IsNull(esri_commercial)) | (~IsNull(navteq_commercial)), self.raster_value)))
        out_ras = os.path.join(self.OUTPUTS, 'FINAL_COMMERCIAL')
        final_commercial_raster.save(out_ras)

        final_commercial_raster_bg = Con(~IsNull(self.nlcd_ras), Con((self.nlcd_ras >= 21) & (self.nlcd_ras <= 24), Con((~IsNull(tiger_commercial)) | (~IsNull(lehd_commercial_bg)) | (~IsNull(esri_commercial)) | (~IsNull(navteq_commercial)), self.raster_value)))
        out_ras = os.path.join(self.OUTPUTS, 'FINAL_COMMERCIAL_BG')
        final_commercial_raster_bg.save(out_ras)
