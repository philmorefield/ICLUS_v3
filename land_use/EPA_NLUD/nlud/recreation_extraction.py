'''
Use multiple data sources to generate recreation-extraction pixels for EPA NLUD.
'''
import os

import arcpy as ap

from arcpy.sa import Con, IsNull


class RecreationExtraction():
    '''
    Use multiple data sources to generate recreation-extraction pixels for EPA-
    NLUD
    '''
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.nlcd_ras = kwargs['nlcd_ras']

        self.raster_value = kwargs['RASTER_VALUES']['RecreationExtraction']

        self.create_padus_raster()
        self.create_final_recex_raster()

    def create_padus_raster(self):
        '''
        Create a recreation-extraction raster from PAD-US
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating recreation-extraction raster from PAD-US...")

        padus = os.path.join(self.INPUTS, 'PADUS_14_Combined_proj')
        padus_query = "GAP_Sts = '3'"  # protected, but managed for multile uses, e.g., recreation, extraction, farming
        padus_output_fc = 'in_memory\\padus_recex'

        padus_recex = ap.Select_analysis(in_features=padus,
                                         out_feature_class=padus_output_fc,
                                         where_clause=padus_query)

        ap.AddField_management(in_table=padus_recex,
                               field_name='RECEX_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=padus_recex,
                                     field='RECEX_TRUE',
                                     expression=self.raster_value,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.INTERMEDIATE, 'PADUS_RECEX')
        ap.PolygonToRaster_conversion(in_features=padus_recex,
                                      value_field='RECEX_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_final_recex_raster(self):
        '''
        Create the final recreation-extraction raster
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating final recreation-extraction raster...")

        padus_recex = ap.Raster(os.path.join(self.INTERMEDIATE, 'PADUS_RECEX'))

        final_recex_ras = Con(~IsNull(self.nlcd_ras), padus_recex)
        final_recex_ras.save(os.path.join(self.OUTPUTS, 'FINAL_RECREATIONEXTRACTION'))
