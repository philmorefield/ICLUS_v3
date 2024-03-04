'''
Use multiple data sources to generate conservation pixels for EPA NLUD.
'''
import os

import arcpy as ap

from arcpy.sa import Con, IsNull


class Conservation():
    '''
    Use multiple data source to generate conservation pixels for EPA NLUD
    '''
    def __init__(self, kwargs):
        self.region_number = kwargs['REGION_NUMBER']
        self.inputs = kwargs['INPUTS']
        self.outputs = kwargs['OUTPUTS']
        self.intermediate = kwargs['INTERMEDIATE']

        self.nlcd_ras = kwargs['nlcd_ras']

        self.raster_value = kwargs['RASTER_VALUES']['Conservation']

        self.create_padus_raster()
        self.create_final_conservation_raster()

    def create_padus_raster(self):
        '''
        Create a conservation raster from PAD-US
        '''
        print(f"[Region {self.region_number}]   Creating conservation raster from PAD-US...")

        padus = os.path.join(self.inputs, 'PADUS_14_Combined_proj')
        padus_query = "GAP_Sts = '1' Or GAP_Sts = '2' And Des_Tp <> 'MPA'"  # managed for biodiversity but not a Marine Protected Area
        padus_output_fc = 'in_memory\\padus_conservation'

        padus_cons = ap.Select_analysis(in_features=padus,
                                        out_feature_class=padus_output_fc,
                                        where_clause=padus_query)

        ap.AddField_management(in_table=padus_cons,
                               field_name='CONS_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=padus_cons,
                                     field='CONS_TRUE',
                                     expression=self.raster_value,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.intermediate, 'PADUS_CONSERVATION')
        ap.PolygonToRaster_conversion(in_features=padus_cons,
                                      value_field='CONS_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_final_conservation_raster(self):
        '''
        Create the final conservation raster
        '''
        print(f"[Region {self.region_number}]   Creating final conservation raster...")

        padus_cons = ap.Raster(os.path.join(self.intermediate, 'PADUS_CONSERVATION'))

        final_cons_ras = Con(~IsNull(self.nlcd_ras), padus_cons)
        final_cons_ras.save(os.path.join(self.outputs, 'FINAL_CONSERVATION'))
