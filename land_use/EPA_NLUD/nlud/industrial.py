'''
Use multiple data sources to generate industrial pixels for EPA-NLUD.

20210210 - Now only the LEHD industrial definition is constrained to NLCD
           Developed pixels. The other polygonal data are used as is and are
           not constrained by any land cover type.
'''
import os

import arcpy as ap

from arcpy.sa import Con, IsNull


class Industrial():
    '''
    Use multiple data sources to generate industrial pixels for EPA-NLUD.
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

        self.raster_value = kwargs['RASTER_VALUES']['Industrial']

        self.create_silvis_lehd_industrial_raster()
        self.create_census_industrial_raster()
        self.create_esri_industrial_raster()
        self.create_usgs_industrial_raster()
        self.create_navteq_industrial_raster()
        self.create_final_industrial_raster()

    def create_silvis_lehd_industrial_raster(self):
        '''
        Create a raster of industrial land use from UW Silvis Census blocks and
        LEHD
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating industrial raster from UW Silvis blocks and LEHD...")

        lehd_industrial_raster = Con((self.indust > 0.0) & (self.nlcd_ras >= 21) & (self.nlcd_ras <= 24), Con((self.huden < 0.1) | (self.indust > (self.huden * 2)), Con(self.indust >= self.comm, self.raster_value)))
        lehd_industrial_raster.save(os.path.join(self.INTERMEDIATE, 'LEHD_INDUSTRIAL'))

        lehd_industrial_raster_bg = Con((self.indust_bg > 0.0) & (self.nlcd_ras >= 21) & (self.nlcd_ras <= 24), Con((self.huden_bg < 0.1) | (self.indust_bg > (self.huden_bg * 2)), Con(self.indust_bg >= self.comm_bg, self.raster_value)))
        lehd_industrial_raster_bg.save(os.path.join(self.INTERMEDIATE, 'LEHD_INDUSTRIAL_BG'))

    def create_census_industrial_raster(self):
        '''
        Create a raster of industrial land use from TIGER2010

        C3088 = Landfill
        K2362 = Industrial Building or Industrial Park
        C3075 = Tank/Tank Farm
        H2041 = Treatment Pond
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating industrial raster from TIGER2010...")
        tiger_landmarks = os.path.join(self.INPUTS, 'TIGER2010_arealm_proj')
        tiger_query = "MTFCC = 'C3088' Or \
                       MTFCC = 'K2362' Or \
                       MTFCC = 'C3075' Or \
                       MTFCC = 'H2041'"
        tiger_output_fc = 'in_memory\\tiger_industrial'

        tiger_industrial = ap.Select_analysis(in_features=tiger_landmarks,
                                              out_feature_class=tiger_output_fc,
                                              where_clause=tiger_query)

        ap.AddField_management(in_table=tiger_industrial,
                               field_name='INDUSTRIAL_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=tiger_industrial,
                                     field='INDUSTRIAL_TRUE',
                                     expression=self.raster_value,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.INTERMEDIATE, 'TIGER_INDUSTRIAL')
        ap.PolygonToRaster_conversion(in_features=tiger_industrial,
                                      value_field='INDUSTRIAL_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_esri_industrial_raster(self):
        '''
        Create a raster of industrial land use ESRI_2010_lalndrk

        D62 = Industrial Building or Industrial Park
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating industrial raster from ESRI_2010_lalndmrk_proj...")
        esri_landmarks = os.path.join(self.INPUTS, 'ESRI_2010_lalndmrk_proj')
        esri_query = "FCC = 'D62'"  # Industrial Building or Industrial Park
        esri_output_fc = 'in_memory\\esri_industrial'

        esri_industrial = ap.Select_analysis(in_features=esri_landmarks,
                                             out_feature_class=esri_output_fc,
                                             where_clause=esri_query)

        ap.AddField_management(in_table=esri_industrial,
                               field_name='INDUSTRIAL_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=esri_industrial,
                                     field='INDUSTRIAL_TRUE',
                                     expression=self.raster_value,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.INTERMEDIATE, 'ESRI_INDUSTRIAL')
        ap.PolygonToRaster_conversion(in_features=esri_industrial,
                                      value_field='INDUSTRIAL_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_usgs_industrial_raster(self):
        '''
        Create a raster of industrial land from the NLCD impervious descriptor
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating industrial raster from NLCD_2011_Impervious_descriptor_L48_20190405...")

        nlcd_raster = ap.Raster(os.path.join(self.INPUTS, 'NLCD_2011_Impervious_descriptor_L48_20190405'))

        usgs_industrial_raster = Con((nlcd_raster == 27) | (nlcd_raster == 28 | nlcd_raster == 29), self.raster_value)
        usgs_industrial_raster.save(os.path.join(self.INTERMEDIATE, 'USGS_INDUSTRIAL'))

    def create_navteq_industrial_raster(self):
        '''
        Create a raster of industrial land use from Navteq 2011
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating industrial raster from Navteq 2011...")

        navteq_landuse_A = os.path.join(self.INPUTS, 'Navteq_2011_LandUseA_proj')
        navteq_query = "FEAT_TYPE = 'INDUSTRIAL COMPLEX' Or \
                        FEAT_TYPE = 'RAILYARD'"
        navteq_output_fc = 'in_memory\\navteq_industrial'
        navteq_industrial = ap.Select_analysis(in_features=navteq_landuse_A,
                                               out_feature_class=navteq_output_fc,
                                               where_clause=navteq_query)

        ap.AddField_management(in_table=navteq_industrial,
                               field_name='INDUSTRIAL_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=navteq_industrial,
                                     field='INDUSTRIAL_TRUE',
                                     expression=self.raster_value,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.INTERMEDIATE, 'NAVTEQ_INDUSTRIAL')
        ap.PolygonToRaster_conversion(in_features=navteq_industrial,
                                      value_field='INDUSTRIAL_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_final_industrial_raster(self):
        '''
        Create the final raster of industrial land use
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating final industrial raster...")

        tiger_industrial = ap.Raster(os.path.join(self.INTERMEDIATE, 'TIGER_INDUSTRIAL'))
        lehd_industrial = ap.Raster(os.path.join(self.INTERMEDIATE, 'LEHD_INDUSTRIAL'))
        lehd_industrial_bg = ap.Raster(os.path.join(self.INTERMEDIATE, 'LEHD_INDUSTRIAL_BG'))
        esri_industrial = ap.Raster(os.path.join(self.INTERMEDIATE, 'ESRI_INDUSTRIAL'))
        usgs_industrial = ap.Raster(os.path.join(self.INTERMEDIATE, 'USGS_INDUSTRIAL'))
        navteq_industrial = ap.Raster(os.path.join(self.INTERMEDIATE, 'NAVTEQ_INDUSTRIAL'))

        final_industrial_raster = Con(~IsNull(self.nlcd_ras), Con((~IsNull(tiger_industrial)) | (~IsNull(lehd_industrial)) | (~IsNull(esri_industrial)) | (~IsNull(usgs_industrial)) | (~IsNull(navteq_industrial)), self.raster_value))
        out_ras = os.path.join(self.OUTPUTS, 'FINAL_INDUSTRIAL')
        final_industrial_raster.save(out_ras)

        final_industrial_raster_bg = Con(~IsNull(self.nlcd_ras), Con((~IsNull(tiger_industrial)) | (~IsNull(lehd_industrial_bg)) | (~IsNull(esri_industrial)) | (~IsNull(usgs_industrial)) | (~IsNull(navteq_industrial)), self.raster_value))
        out_ras = os.path.join(self.OUTPUTS, 'FINAL_INDUSTRIAL_BG')
        final_industrial_raster_bg.save(out_ras)
