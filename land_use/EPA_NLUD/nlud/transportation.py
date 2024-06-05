'''
Use multiple data sources to generate transportation pixels for EPA-NLUD.
'''
import os

import arcpy as ap

from arcpy.sa import Con, Expand, IsNull, Shrink


class Transportation():
    '''
    Use multiple data sources to generate transportation pixels for EPA-NLUD.
    '''
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.nlcd_ras = kwargs['nlcd_ras']

        self.raster_value = kwargs['RASTER_VALUES']['Transportation']

        self.create_esri_transportation_raster()
        self.create_tiger_transportation_raster()
        self.create_usgs_transportation_raster()
        self.create_navteq_transportation_raster()
        self.create_final_transportation_raster()

    def create_esri_transportation_raster(self):
        '''
        Create a raster of transportation land use from ESRI_2010_rail and
        ESRI_2010_airportp
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating transporation raster from ESRI_2010_rail/airportp...")
        esri_rails_raster = ap.Raster(os.path.join(self.INPUTS, 'ESRI_2010_rail'))
        esri_airport_raster = ap.Raster(os.path.join(self.INPUTS, 'ESRI_2010_airportp'))

        esri_rails_raster = Con(~IsNull(esri_rails_raster), Con((self.nlcd_ras >= 21) & (self.nlcd_ras <= 24), self.raster_value))
        esri_airport_raster = Con(~IsNull(esri_airport_raster), self.raster_value)
        esri_rails_raster.save(os.path.join(self.INTERMEDIATE, 'ESRI_RAILS'))
        esri_airport_raster.save(os.path.join(self.INTERMEDIATE, 'ESRI_AIRPORTS'))

    def create_tiger_transportation_raster(self):
        '''
        Create a raster of transportation land use from TIGER2010
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating transporation raster from TIGER2010...")

        tiger_ramps_raster = ap.Raster(os.path.join(self.INPUTS, 'TIGER2010_ramps'))

        tiger_transporation_raster = Con(~IsNull(tiger_ramps_raster), Con((self.nlcd_ras >= 21) & (self.nlcd_ras <= 24), self.raster_value))
        tiger_transporation_raster.save(os.path.join(self.INTERMEDIATE, 'TIGER_TRANSPORTATION'))

    def create_usgs_transportation_raster(self):
        '''
        Create a raster of transportation land use from
        NLCD_2011_Impervious_descriptor_L48_20190405
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating transporation raster from NLCD_2011_Impervious_descriptor_L48_20190405...")

        nlcd_raster = ap.Raster(os.path.join(self.INPUTS, 'NLCD_2011_Impervious_descriptor_l48_20210604'))
        tiger_transportation_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'TIGER_TRANSPORTATION'))

        # make a layer of primary roads and ramps
        proads_ramps = Con(nlcd_raster == 1, self.raster_value, Con(nlcd_raster == 20, self.raster_value, Con(tiger_transportation_raster == self.raster_value, self.raster_value)))
        proads_ramps.save(os.path.join(self.INTERMEDIATE, 'USGS_PROADS_RAMPS'))

        in_raster = os.path.join(self.INTERMEDIATE, 'USGS_PROADS_RAMPS')

        # perform Expand/Shrink to fill in highway medians, cloverleafs, etc.
        proads_ramps_expand = Expand(in_raster=in_raster,
                                     number_cells=2,
                                     zone_values=self.raster_value)
        proads_ramps_expand.save(os.path.join(self.INTERMEDIATE, 'PRIMARY_ROADS_AND_RAMPS_EXPAND'))

        in_raster = os.path.join(self.INTERMEDIATE, 'PRIMARY_ROADS_AND_RAMPS_EXPAND')
        proads_ramps_shrink = Shrink(in_raster=in_raster,
                                     number_cells=2,
                                     zone_values=self.raster_value)

        # all primary and secondary roads identified by NLCD mapping program
        # visual inspection of 'tertiary' roads shows that they are less than
        # 15m in width, and therefore would not make up the 'primary use' of a
        # 30m pixel
        usgs_transportation_raster = Con(((nlcd_raster == 20) | (nlcd_raster == 21)) | (~IsNull(proads_ramps_shrink)), self.raster_value)

        usgs_transportation_raster.save(os.path.join(self.INTERMEDIATE, 'USGS_TRANSPORTATION'))

    def create_navteq_transportation_raster(self):
        '''
        Create a raster of transporation land use from Navteq 2011
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating transportation raster from Navteq 2011...")

        # Airports from LandUseA
        navteq_landuse_A = os.path.join(self.INPUTS, 'Navteq_2011_LandUseA_proj')
        navteq_query = "FEAT_TYPE = 'AIRPORT'"
        navteq_output_fc = 'in_memory\\navteq_A_transportation'
        navteq_transportation = ap.Select_analysis(in_features=navteq_landuse_A,
                                                   out_feature_class=navteq_output_fc,
                                                   where_clause=navteq_query)

        ap.AddField_management(in_table=navteq_transportation,
                               field_name='TRANSPORTATION_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=navteq_transportation,
                                     field='TRANSPORTATION_TRUE',
                                     expression=self.raster_value,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.INTERMEDIATE, 'NAVTEQ_A_TRANSPORTATION')
        ap.PolygonToRaster_conversion(in_features=navteq_transportation,
                                      value_field='TRANSPORTATION_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

        # Aircraft roads (i.e., runways) from LandUseB; n < ~50 that don't
        # intersect with Airports from LandUseA
        navteq_landuse_B = os.path.join(self.INPUTS, 'Navteq_2011_LandUseB_proj')
        navteq_query = "FEAT_TYPE = 'AIRCRAFT ROADS'"
        navteq_output_fc = 'in_memory\\navteq_B_transportation'
        navteq_transportation = ap.Select_analysis(in_features=navteq_landuse_B,
                                                   out_feature_class=navteq_output_fc,
                                                   where_clause=navteq_query)

        ap.AddField_management(in_table=navteq_transportation,
                               field_name='TRANSPORTATION_TRUE',
                               field_type='SHORT')

        ap.CalculateField_management(in_table=navteq_transportation,
                                     field='TRANSPORTATION_TRUE',
                                     expression=self.raster_value,
                                     expression_type='PYTHON3')

        out_ras = os.path.join(self.INTERMEDIATE, 'NAVTEQ_B_TRANSPORTATION')
        ap.PolygonToRaster_conversion(in_features=navteq_transportation,
                                      value_field='TRANSPORTATION_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_final_transportation_raster(self):
        '''
        Create the final transportation land use raster
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating final transporation raster...")

        tiger_transportation_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'TIGER_TRANSPORTATION'))
        esri_rails_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'ESRI_RAILS'))
        esri_airports_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'ESRI_AIRPORTS'))
        usgs_transportation_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'USGS_TRANSPORTATION'))
        navteq_A_transportation_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'NAVTEQ_A_TRANSPORTATION'))
        navteq_B_transportation_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'NAVTEQ_B_TRANSPORTATION'))

        final_transporation_raster = Con((~IsNull(tiger_transportation_raster)) | (~IsNull(esri_rails_raster)) | (~IsNull(esri_airports_raster)) | (~IsNull(usgs_transportation_raster)) | (~IsNull(navteq_A_transportation_raster)) | (~IsNull(navteq_B_transportation_raster)), self.raster_value)
        final_transporation_raster.save(os.path.join(self.OUTPUTS, 'FINAL_TRANSPORTATION'))
