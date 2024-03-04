'''
Use multiple data sources to generate institutional pixels for EPA-NLUD.
'''
import os

import arcpy as ap

from arcpy.sa import Con, IsNull


class Institutional():
    '''
    Use multiple data sources to generate institutional pixels for EPA-NLUD.
    '''
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.nlcd_ras = kwargs['nlcd_ras']
        self.urban_areas = kwargs['urban_areas']

        self.RASTER_VALUES = kwargs['RASTER_VALUES']

        self.create_tiger_institutional_raster()
        self.create_esri_institutional_raster()
        self.create_dhs_institutional_raster()
        self.create_navteq_institutional_raster()
        self.create_final_institutional_raster()

    def create_tiger_institutional_raster(self):
        '''
        Create a raster of institutional land use from TIGER2010

        K1231 --> Hospital
        K1233 --> Nursing Home
        K1234 --> County Home or Poor Farm
        K1235 --> Juvenile Institution
        K1236 --> Local Jail
        K1237 --> Federal Penitentiary
        K1238 --> Other Correctional Institution
        K1239 --> Convent or Monestary
        K1241 --> Sorority, Fraternity, or Dormitory
        K2100 --> Governmental
        K2110 --> Military Installation
        K2146 --> Community Center
        K2165 --> Government Center
        K2196 --> City/Town Hall
        K2540 --> University or College
        K2543 --> School or Academy
        K2582 --> Cemetary
        K3544 --> Place of Worship
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating institutional raster from TIGER2010_arealm_proj...")
        tiger_arealm = os.path.join(self.INPUTS, 'TIGER2010_arealm_proj')
        tiger_query = "MTFCC = 'K1231' Or \
                       MTFCC = 'K1233' Or \
                       MTFCC = 'K1234' Or \
                       MTFCC = 'K1235' Or \
                       MTFCC = 'K1236' Or \
                       MTFCC = 'K1237' Or \
                       MTFCC = 'K1238' Or \
                       MTFCC = 'K1239' Or \
                       MTFCC = 'K1241' Or \
                       MTFCC = 'K2100' Or \
                       MTFCC = 'K2110' Or \
                       MTFCC = 'K2146' Or \
                       MTFCC = 'K2165' Or \
                       MTFCC = 'K2196' Or \
                       MTFCC = 'K2540' Or \
                       MTFCC = 'K2543' Or \
                       MTFCC = 'K2582' Or \
                       MTFCC = 'K3544'"

        tiger_output_fc = 'in_memory\\tiger_inst'

        tiger_inst = ap.Select_analysis(in_features=tiger_arealm,
                                        out_feature_class=tiger_output_fc,
                                        where_clause=tiger_query)

        ap.AddField_management(in_table=tiger_inst,
                               field_name='INST_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=tiger_inst,
                                     field='INST_TRUE',
                                     expression=1,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.INTERMEDIATE, 'TIGER_INSTITUTIONAL')
        ap.PolygonToRaster_conversion(in_features=tiger_inst,
                                      value_field='INST_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

        # Federal (except for NPS) lands inside urban areas
        urban_fed_query = "MTFCC = 'K2182'"
        urban_fed_output_fc = 'in_memory\\fed_inst'

        urban_fed_inst = ap.Select_analysis(in_features=tiger_arealm,
                                            out_feature_class=urban_fed_output_fc,
                                            where_clause=urban_fed_query)

        ap.AddField_management(in_table=urban_fed_inst,
                               field_name='INST_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=urban_fed_inst,
                                     field='INST_TRUE',
                                     expression=1,
                                     expression_type='PYTHON3')

        out_rasterdataset = os.path.join(self.INTERMEDIATE, 'URBAN_FED_INSTITUTIONAL')
        ap.PolygonToRaster_conversion(in_features=urban_fed_inst,
                                      value_field='INST_TRUE',
                                      out_rasterdataset=out_rasterdataset,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

        fed_inst_ras = ap.Raster(out_rasterdataset)
        urban_fed_inst_ras = ap.sa.Con((~IsNull(self.urban_areas)) & (~IsNull(fed_inst_ras)), 1)

        ap.Mosaic_management(inputs=[urban_fed_inst_ras],
                             target=out_ras)

    def create_esri_institutional_raster(self):
        '''
        Creat a raster of institutional land use from ESRI_2010_lalndmrk

        D10 --> Military territory
        D31 --> Hospital
        D43 --> School, University, or College
        D82 --> Cemetary
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating institutional raster from ESRI_2010_lalndmrk_proj...")
        esri_arealm = os.path.join(self.INPUTS, 'ESRI_2010_lalndmrk_proj')
        esri_query = "FCC = 'D10' Or \
                      FCC = 'D31' Or \
                      FCC = 'D43' Or \
                      FCC = 'D82'"

        esri_output_fc = 'in_memory\\esri_inst'

        esri_inst = ap.Select_analysis(in_features=esri_arealm,
                                       out_feature_class=esri_output_fc,
                                       where_clause=esri_query)

        ap.AddField_management(in_table=esri_inst,
                               field_name='INST_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=esri_inst,
                                     field='INST_TRUE',
                                     expression=1,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.INTERMEDIATE, 'ESRI_INSTITUTIONAL')
        ap.PolygonToRaster_conversion(in_features=esri_inst,
                                      value_field='INST_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_dhs_institutional_raster(self):
        '''
        Create a raster of institutional land use from DHS_college_campus
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating institutional raster from DHS_college_campus...")
        dhs_college = os.path.join(self.INPUTS, 'DHS_college_campus')

        ap.AddField_management(in_table=dhs_college,
                               field_name='INST_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=dhs_college,
                                     field='INST_TRUE',
                                     expression=1,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.INTERMEDIATE, 'DHS_INSTITUTIONAL')
        ap.PolygonToRaster_conversion(in_features=dhs_college,
                                      value_field='INST_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_navteq_institutional_raster(self):
        '''
        Create a raster of institutional land use from Navteq 2011
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating institutional raster from Navteq 2011...")

        navteq_landuse_A = os.path.join(self.INPUTS, 'Navteq_2011_LandUseA_proj')
        navteq_query = "FEAT_TYPE = 'CEMETERY' Or \
                        FEAT_TYPE = 'HOSPITAL' Or \
                        FEAT_TYPE = 'MILITARY BASE' Or \
                        FEAT_TYPE = 'UNIVERSITY/COLLEGE'"
        navteq_output_fc = 'in_memory\\navteq_institutional'
        navteq_institutional = ap.Select_analysis(in_features=navteq_landuse_A,
                                                  out_feature_class=navteq_output_fc,
                                                  where_clause=navteq_query)

        ap.AddField_management(in_table=navteq_institutional,
                               field_name='INST_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=navteq_institutional,
                                     field='INST_TRUE',
                                     expression=1,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.INTERMEDIATE, 'NAVTEQ_INSTITUTIONAL')
        ap.PolygonToRaster_conversion(in_features=navteq_institutional,
                                      value_field='INST_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_final_institutional_raster(self):
        '''
        Create the final institutional land use raster
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating final institutional raster...")
        tiger_inst = ap.Raster(os.path.join(self.INTERMEDIATE, 'TIGER_INSTITUTIONAL'))
        tiger_mil = ap.Raster(os.path.join(self.INPUTS, 'MIL10'))
        esri_inst = ap.Raster(os.path.join(self.INTERMEDIATE, 'ESRI_INSTITUTIONAL'))
        dhs_inst = ap.Raster(os.path.join(self.INTERMEDIATE, 'DHS_INSTITUTIONAL'))
        navteq_inst = ap.Raster(os.path.join(self.INTERMEDIATE, 'NAVTEQ_INSTITUTIONAL'))

        dev = self.RASTER_VALUES['InstitutionalDeveloped']
        undev = self.RASTER_VALUES['InstitutionalUndeveloped']

        final_institutional_raster = Con(~IsNull(self.nlcd_ras), Con((~IsNull(tiger_inst)) | (~IsNull(tiger_mil)) | (~IsNull(esri_inst)) | (~IsNull(dhs_inst)) | (~IsNull(navteq_inst)), Con((self.nlcd_ras >= 22) & (self.nlcd_ras <= 24), dev, undev)))
        final_developed_raster = ap.sa.ExtractByAttributes(in_raster=final_institutional_raster, where_clause=f'VALUE = {dev}')
        final_developed_raster.save(os.path.join(self.OUTPUTS, 'FINAL_INSTITUTIONALDEVELOPED'))

        final_undeveloped_raster = ap.sa.ExtractByAttributes(in_raster=final_institutional_raster, where_clause=f'VALUE = {undev}')
        final_undeveloped_raster.save(os.path.join(self.OUTPUTS, 'FINAL_INSTITUTIONALUNDEVELOPED'))
