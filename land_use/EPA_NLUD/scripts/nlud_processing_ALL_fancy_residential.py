"""
Author: Phil Morefield
Purpose: EPA NLUD for ICLUS version 3
Created: 8/22/2018

20201106 - this version uses sliding housing density thresholds when determing
           residential classes, i.e., urban, place, and neither designations.
           One effect - for example - is that the urban classification can only
           occur in urban areas as delineated by US Census shapefiles. High
           density residential areas outside of urban areas would be classified
           as suburban.
"""
from multiprocessing import Pool
import os
import time

import arcpy as ap
from arcpy.sa import CellStatistics, Con, IsNull

ap.CheckOutExtension('Spatial')

ap.env.overwriteOutput = True
ap.env.parallelProcessingFactor = 8
ap.env.pyramid = "PYRAMIDS -1 NEAREST LZ77 NO_SKIP"

RASTER_VALUES = {'Transportation': 21,
                 'InstitutionalDeveloped': 20,
                 'InstitutionalUndeveloped': 19,
                 'Industrial': 18,
                 'Commercial': 17,
                 'MixedUse': 16,
                 'HighDensityUrban': 15,
                 'Urban': 14,
                 'Grayfield': 13,
                 'Suburban': 12,
                 'Exurban': 11,
                 'Rural': 10,
                 'ParksGolf': 9,
                 'Cropland': 8,
                 'Pasture': 7,
                 'PrivateGrassShrub': 6,
                 'PrivateForest': 5,
                 'RecreationExtraction': 4,
                 'Conservation': 3,
                 'Wetlands': 2,
                 'Water': 1}


class Water():
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.nlcd_ras = kwargs['nlcd_ras']

        self.raster_value = RASTER_VALUES['Water']

        self.create_nhd_water_raster()
        self.create_tiger_water_raster()
        self.create_nlcd_water_raster()
        self.create_padus_water_raster()
        self.create_final_water_raster()

    def create_nhd_water_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating water raster from NHDPlusv2...", time.ctime())
        nhd = os.path.join(self.INPUTS, 'NHDWaterbody_proj')
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

        out_ras = os.path.join(self.INTERMEDIATE, 'NHDWaterbody_WATER')
        ap.PolygonToRaster_conversion(in_features=nhd_water,
                                      value_field='WATER_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_tiger_water_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating water raster from TIGER2010...", time.ctime())
        tiger_water = os.path.join(self.INPUTS, 'TIGER2010_areawater_proj')
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

        out_ras = os.path.join(self.INTERMEDIATE, 'TIGER_WATER')
        ap.PolygonToRaster_conversion(in_features=tiger_water,
                                      value_field='WATER_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_nlcd_water_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating water raster from NLCD 2011...", time.ctime())
        nlcd_water = Con(self.nlcd_ras == 11, 1)
        out_ras = os.path.join(self.INTERMEDIATE, 'NLCD_WATER')
        nlcd_water.save(out_ras)

    def create_padus_water_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating water raster from PAD-US...", time.ctime())

        padus = os.path.join(self.INPUTS, 'PADUS_14_Combined_proj')
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

        out_ras = os.path.join(self.INTERMEDIATE, 'PADUS_WATER')
        ap.PolygonToRaster_conversion(in_features=padus_water,
                                      value_field='WATER_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_final_water_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating final water raster...", time.ctime())

        nhd_water = os.path.join(self.INTERMEDIATE, 'NHDWaterbody_WATER')
        tiger_water = os.path.join(self.INTERMEDIATE, 'TIGER_WATER')
        nlcd_water = os.path.join(self.INTERMEDIATE, 'NLCD_WATER')
        padus_water = os.path.join(self.INTERMEDIATE, 'PADUS_WATER')

        water_rasters = [nlcd_water,
                         nhd_water,
                         tiger_water,
                         padus_water]

        sum_water_rasters = CellStatistics(in_rasters_or_constants=water_rasters,
                                           statistics_type='SUM',
                                           ignore_nodata='DATA')

        final_water_raster = Con((~IsNull(self.nlcd_ras)) & (sum_water_rasters >= (self.raster_value * 2)), self.raster_value)
        out_ras = os.path.join(self.OUTPUTS, 'FINAL_WATER')
        final_water_raster.save(out_ras)


class Wetlands():
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.nlcd_ras = kwargs['nlcd_ras']

        self.raster_value = RASTER_VALUES['Wetlands']

        self.create_nhd_wetlands_raster()
        self.create_tiger_wetlands_raster()
        self.create_nlcd_wetlands_raster()
        self.create_final_wetlands_raster()

    def create_nhd_wetlands_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating wetlands raster from NHPlusv2...", time.ctime())
        nhd = os.path.join(self.INPUTS, 'NHDWaterbody_proj')
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

        out_ras = os.path.join(self.INTERMEDIATE, 'NHDWaterbody_WETLANDS')
        ap.PolygonToRaster_conversion(in_features=nhd_wetlands,
                                      value_field='WETLANDS_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_tiger_wetlands_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating wetlands raster from TIGER2010...", time.ctime())
        tiger_wetlands = os.path.join(self.INPUTS, 'TIGER2010_areawater_proj')
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

        out_ras = os.path.join(self.INTERMEDIATE, 'TIGER_WETLANDS')
        ap.PolygonToRaster_conversion(in_features=tiger_wetlands,
                                      value_field='WETLANDS_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_nlcd_wetlands_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating wetlands from from NLCD 2011...", time.ctime())

        nlcd_wetlands = Con((self.nlcd_ras == 90) | (self.nlcd_ras == 95), self.raster_value)
        out_ras = os.path.join(self.INTERMEDIATE, 'NLCD_WETLANDS')
        nlcd_wetlands.save(out_ras)

    def create_final_wetlands_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating final water-wetlands raster...", time.ctime())
        nhd_wetlands = os.path.join(self.INTERMEDIATE, 'NHDWaterbody_WETLANDS')
        tiger_wetlands = os.path.join(self.INTERMEDIATE, 'TIGER_WETLANDS')
        nlcd_wetlands = os.path.join(self.INTERMEDIATE, 'NLCD_WETLANDS')

        wetlands_rasters = [nhd_wetlands,
                            tiger_wetlands,
                            nlcd_wetlands]

        sum_wetlands_rasters = CellStatistics(in_rasters_or_constants=wetlands_rasters,
                                              statistics_type='SUM',
                                              ignore_nodata='DATA')

        final_wetlands_raster = Con((~IsNull(self.nlcd_ras)) & (sum_wetlands_rasters >= (self.raster_value * 2)), self.raster_value)
        out_ras = os.path.join(self.OUTPUTS, 'FINAL_WETLANDS')
        final_wetlands_raster.save(out_ras)


class Conservation():
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.nlcd_ras = kwargs['nlcd_ras']

        self.raster_value = RASTER_VALUES['Conservation']

        self.create_padus_raster()
        self.create_final_conservation_raster()

    def create_padus_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating conservation raster from PAD-US...", time.ctime())

        padus = os.path.join(self.INPUTS, 'PADUS_14_Combined_proj')
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

        out_ras = os.path.join(self.INTERMEDIATE, 'PADUS_CONSERVATION')
        ap.PolygonToRaster_conversion(in_features=padus_cons,
                                      value_field='CONS_TRUE',
                                      out_rasterdataset=out_ras,
                                      cell_assignment='CELL_CENTER',
                                      cellsize=30)

    def create_final_conservation_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating final conservation raster...", time.ctime())

        padus_cons = ap.Raster(os.path.join(self.INTERMEDIATE, 'PADUS_CONSERVATION'))

        final_cons_ras = Con(~IsNull(self.nlcd_ras), padus_cons)
        final_cons_ras.save(os.path.join(self.OUTPUTS, 'FINAL_CONSERVATION'))


class RecreationExtraction():
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.nlcd_ras = kwargs['nlcd_ras']

        self.raster_value = RASTER_VALUES['RecreationExtraction']

        self.create_padus_raster()
        self.create_final_recex_raster()

    def create_padus_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating recreation-extraction raster from PAD-US...", time.ctime())

        padus = os.path.join(self.INPUTS, 'PADUS_14_Combined_proj')
        padus_query = "GAP_Sts = '3'"  # managed for multiple uses
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
        print(f"[Region {self.REGION_NUMBER}]   Creating final recreation-extraction raster...", time.ctime())

        padus_recex = ap.Raster(os.path.join(self.INTERMEDIATE, 'PADUS_RECEX'))

        final_recex_ras = Con(~IsNull(self.nlcd_ras), padus_recex)
        final_recex_ras.save(os.path.join(self.OUTPUTS, 'FINAL_RECREATIONEXTRACTION'))


class PrivateForest():
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.nlcd_ras = kwargs['nlcd_ras']

        self.raster_value = RASTER_VALUES['PrivateForest']

        self.create_nlcd_forest_raster()
        self.create_final_privateforest_raster()

    def create_nlcd_forest_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating forest from from NLCD 2011...", time.ctime())

        nlcd_forest = Con((self.nlcd_ras == 41) | (self.nlcd_ras == 42) | (self.nlcd_ras == 43), self.raster_value)
        out_ras = os.path.join(self.INTERMEDIATE, 'NLCD_FOREST')
        nlcd_forest.save(out_ras)

    def create_final_privateforest_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating final private forest raster...", time.ctime())

        forest_ras = ap.Raster(os.path.join(self.INTERMEDIATE, 'NLCD_FOREST'))

        final_privateforest_ras = Con(~IsNull(self.nlcd_ras), forest_ras)
        final_privateforest_ras.save(os.path.join(self.OUTPUTS, 'FINAL_PRIVATEFOREST'))


class PrivateGrassShrub():
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.nlcd_ras = kwargs['nlcd_ras']

        self.raster_value = RASTER_VALUES['PrivateGrassShrub']

        self.create_nlcd_grass_shrub_raster()
        self.create_final_privategrassshrub_raster()

    def create_nlcd_grass_shrub_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating grass/shrub from from NLCD 2011...", time.ctime())

        nlcd_grass_shrub = Con((self.nlcd_ras == 52) | (self.nlcd_ras == 71), self.raster_value)
        out_ras = os.path.join(self.INTERMEDIATE, 'NLCD_GRASS_SHRUB')
        nlcd_grass_shrub.save(out_ras)

    def create_final_privategrassshrub_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating final private grass/shrub raster...", time.ctime())

        grass_shrub_ras = ap.Raster(os.path.join(self.INTERMEDIATE, 'NLCD_GRASS_SHRUB'))

        final_privategrassshrub_ras = Con(~IsNull(self.nlcd_ras), grass_shrub_ras)
        final_privategrassshrub_ras.save(os.path.join(self.OUTPUTS, 'FINAL_PRIVATEGRASSSHRUB'))


class Pasture():
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.nlcd_ras = kwargs['nlcd_ras']

        self.raster_value = RASTER_VALUES['Pasture']

        self.create_nlcd_pasture_raster()
        self.create_final_pasture_raster()

    def create_nlcd_pasture_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating pasture from from NLCD 2011...", time.ctime())

        nlcd_pasture = Con(self.nlcd_ras == 81, self.raster_value)
        out_ras = os.path.join(self.INTERMEDIATE, 'NLCD_PASTURE')
        nlcd_pasture.save(out_ras)

    def create_final_pasture_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating final private pasture raster...", time.ctime())

        pasture_ras = ap.Raster(os.path.join(self.INTERMEDIATE, 'NLCD_PASTURE'))

        final_pasture_ras = Con(~IsNull(self.nlcd_ras), pasture_ras)
        final_pasture_ras.save(os.path.join(self.OUTPUTS, 'FINAL_PASTURE'))


class Cropland():
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.nlcd_ras = kwargs['nlcd_ras']

        self.raster_value = RASTER_VALUES['Cropland']

        self.create_nlcd_cropland_raster()
        self.create_final_cropland_raster()

    def create_nlcd_cropland_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating cropland from from NLCD 2011...", time.ctime())

        nlcd_cropland = Con(self.nlcd_ras == 82, self.raster_value)
        out_ras = os.path.join(self.INTERMEDIATE, 'NLCD_CROPLAND')
        nlcd_cropland.save(out_ras)

    def create_final_cropland_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating final private cropland raster...", time.ctime())

        cropland_ras = ap.Raster(os.path.join(self.INTERMEDIATE, 'NLCD_CROPLAND'))

        final_cropland_ras = Con(~IsNull(self.nlcd_ras), cropland_ras)
        final_cropland_ras.save(os.path.join(self.OUTPUTS, 'FINAL_CROPLAND'))


class ParksGolf():
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.nlcd_ras = kwargs['nlcd_ras']
        self.urban_areas = kwargs['urban_areas']

        self.raster_value = RASTER_VALUES['ParksGolf']

        self.create_padus_parksgolf_raster()
        self.create_tiger_parksgolf_raster()
        self.create_esri_parksgolf_raster()
        self.create_final_parksgolf_raster()

    def create_padus_parksgolf_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating parks and golf course raster from PAD-US...", time.ctime())

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
        K1228 --> Campground
        K2180 --> Park
        K2181 --> National Park Service Land
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

    def create_final_parksgolf_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating final parks and golf raster...", time.ctime())

        padus_parks_ras = ap.Raster(os.path.join(self.INTERMEDIATE, 'PADUS_PARKSGOLF'))
        tiger_parks_ras = ap.Raster(os.path.join(self.INTERMEDIATE, 'TIGER_PARKSGOLF'))
        esri_parks_ras = ap.Raster(os.path.join(self.INTERMEDIATE, 'ESRI_PARKSGOLF'))

        final_parks_ras = Con(~IsNull(self.nlcd_ras), Con((~IsNull(tiger_parks_ras)) | (~IsNull(esri_parks_ras)) | (~IsNull(padus_parks_ras)), self.raster_value))
        final_parks_ras.save(os.path.join(self.OUTPUTS, 'FINAL_PARKSGOLF'))


class Rural():
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.huden = kwargs['huden']
        self.huden_bg = kwargs['huden_bg']
        self.nlcd_ras = kwargs['nlcd_ras']
        self.place = kwargs['place']
        self.urban_areas = kwargs['urban_areas']

        self.raster_value = RASTER_VALUES['Rural']

        self.create_silvis_rural_raster()
        self.create_final_rural_raster()

    def create_silvis_rural_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating rural residential raster from UW Silvis blocks...")

        rural_residential = Con(IsNull(self.urban_areas), Con(IsNull(self.place), Con((self.huden >= 0.1) & (self.huden < 0.5), self.raster_value), Con((self.huden >= 0.025) & (self.huden <= 0.1), self.raster_value)))
        rural_residential.save(os.path.join(self.INTERMEDIATE, 'SILVIS_RURAL'))

        rural_residential_bg = Con(IsNull(self.urban_areas), Con(IsNull(self.place), Con((self.huden_bg >= 0.1) & (self.huden_bg < 0.5), self.raster_value), Con((self.huden_bg >= 0.025) & (self.huden_bg <= 0.1), self.raster_value)))
        rural_residential_bg.save(os.path.join(self.INTERMEDIATE, 'SILVIS_RURAL_BG'))

    def create_final_rural_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating final rural residential raster...", time.ctime())

        rural_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'SILVIS_RURAL'))
        rural_raster_bg = ap.Raster(os.path.join(self.INTERMEDIATE, 'SILVIS_RURAL_BG'))

        final_rural_raster = Con((self.nlcd_ras != 81) & (self.nlcd_ras != 82), rural_raster)
        final_rural_raster.save(os.path.join(self.OUTPUTS, 'FINAL_RURAL'))

        final_rural_raster_bg = Con((self.nlcd_ras != 81) & (self.nlcd_ras != 82), rural_raster_bg)
        final_rural_raster_bg.save(os.path.join(self.OUTPUTS, 'FINAL_RURAL_BG'))


class Exurban():
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.huden = kwargs['huden']
        self.huden_bg = kwargs['huden_bg']
        self.nlcd_ras = kwargs['nlcd_ras']
        self.place = kwargs['place']
        self.urban_areas = kwargs['urban_areas']

        self.raster_value = RASTER_VALUES['Exurban']

        self.create_silvis_exurban_raster()
        self.create_final_exurban_raster()

    def create_silvis_exurban_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating exurban residential raster from UW Silvis blocks...")

        exurban_residential = Con(IsNull(self.urban_areas), Con(IsNull(self.place), Con(self.huden >= 0.5, self.raster_value), Con((self.huden > 0.1) & (self.huden < 0.5), self.raster_value)))
        exurban_residential.save(os.path.join(self.INTERMEDIATE, 'SILVIS_EXURBAN'))

        exurban_residential_bg = Con(IsNull(self.urban_areas), Con(IsNull(self.place), Con(self.huden_bg >= 0.5, self.raster_value), Con((self.huden_bg > 0.1) & (self.huden_bg < 0.5), self.raster_value)))
        exurban_residential_bg.save(os.path.join(self.INTERMEDIATE, 'SILVIS_EXURBAN_BG'))

    def create_final_exurban_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating final exurban residential raster...", time.ctime())
        exurban_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'SILVIS_EXURBAN'))
        exurban_raster_bg = ap.Raster(os.path.join(self.INTERMEDIATE, 'SILVIS_EXURBAN_BG'))

        final_exurban_raster = Con((self.nlcd_ras != 81) & (self.nlcd_ras != 82), exurban_raster)
        final_exurban_raster.save(os.path.join(self.OUTPUTS, 'FINAL_EXURBAN'))

        final_exurban_raster_bg = Con((self.nlcd_ras != 81) & (self.nlcd_ras != 82), exurban_raster_bg)
        final_exurban_raster_bg.save(os.path.join(self.OUTPUTS, 'FINAL_EXURBAN_BG'))


class Suburban():
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.huden = kwargs['huden']
        self.huden_bg = kwargs['huden_bg']
        self.nlcd_ras = kwargs['nlcd_ras']
        self.place = kwargs['place']
        self.urban_areas = kwargs['urban_areas']

        self.raster_value = RASTER_VALUES['Suburban']

        self.create_silvis_suburban_raster()
        self.create_final_suburban_raster()

    def create_silvis_suburban_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating suburban residential raster from UW Silvis blocks...")

        suburban_residential = Con((~IsNull(self.urban_areas)) & (self.huden <= 4.0) & (self.huden >= 0.025), self.raster_value, Con((~IsNull(self.place)) & (self.huden >= 0.5), self.raster_value))
        suburban_residential.save(os.path.join(self.INTERMEDIATE, 'SILVIS_SUBURBAN'))

        suburban_residential_bg = Con((~IsNull(self.urban_areas)) & (self.huden_bg <= 4.0) & (self.huden_bg >= 0.025), self.raster_value, Con((~IsNull(self.place)) & (self.huden_bg >= 0.5), self.raster_value))
        suburban_residential_bg.save(os.path.join(self.INTERMEDIATE, 'SILVIS_SUBURBAN_BG'))

    def create_final_suburban_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating final suburban residential raster...", time.ctime())
        suburban_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'SILVIS_SUBURBAN'))
        suburban_raster_bg = ap.Raster(os.path.join(self.INTERMEDIATE, 'SILVIS_SUBURBAN_BG'))

        final_suburban_raster = Con((~IsNull(self.nlcd_ras)) & (~IsNull(suburban_raster)), suburban_raster)
        final_suburban_raster.save(os.path.join(self.OUTPUTS, 'FINAL_SUBURBAN'))

        final_suburban_raster_bg = Con((~IsNull(self.nlcd_ras)) & (~IsNull(suburban_raster_bg)), suburban_raster)
        final_suburban_raster_bg.save(os.path.join(self.OUTPUTS, 'FINAL_SUBURBAN_BG'))


class Urban():
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.huden = kwargs['huden']
        self.huden_bg = kwargs['huden_bg']
        self.nlcd_ras = kwargs['nlcd_ras']
        self.urban_areas = kwargs['urban_areas']

        self.raster_value = RASTER_VALUES['Urban']
        self.create_silvis_urban_raster()
        self.create_final_urban_raster()

    def create_silvis_urban_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating urban residential raster from UW Silvis blocks...")

        urban_residential = Con((~IsNull(self.urban_areas)) & (self.huden > 4.0) & (self.huden <= 10.0), self.raster_value)
        urban_residential.save(os.path.join(self.INTERMEDIATE, 'SILVIS_URBAN'))

        urban_residential_bg = Con((~IsNull(self.urban_areas)) & (self.huden_bg > 4.0) & (self.huden_bg <= 10.0), self.raster_value)
        urban_residential_bg.save(os.path.join(self.INTERMEDIATE, 'SILVIS_URBAN_BG'))

    def create_final_urban_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating final urban residential raster...", time.ctime())

        urban_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'SILVIS_URBAN'))
        urban_raster_bg = ap.Raster(os.path.join(self.INTERMEDIATE, 'SILVIS_URBAN_BG'))

        final_urban_raster = Con((~IsNull(self.nlcd_ras)) & (~IsNull(urban_raster)), urban_raster)
        final_urban_raster.save(os.path.join(self.OUTPUTS, 'FINAL_URBAN'))

        final_urban_raster_bg = Con((~IsNull(self.nlcd_ras)) & (~IsNull(urban_raster_bg)), urban_raster_bg)
        final_urban_raster_bg.save(os.path.join(self.OUTPUTS, 'FINAL_URBAN_BG'))


class HighDensityUrban():
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.huden = kwargs['huden']
        self.huden_bg = kwargs['huden_bg']
        self.nlcd_ras = kwargs['nlcd_ras']
        self.urban_areas = kwargs['urban_areas']

        self.raster_value = RASTER_VALUES['HighDensityUrban']

        self.create_silvis_high_density_urban_raster()
        self.create_final_high_density_urban_raster()

    def create_silvis_high_density_urban_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating high-density urban residential raster from UW Silvis blocks...")

        high_density_urban_residential = Con((~IsNull(self.urban_areas)) & (self.huden > 10.0), self.raster_value)
        high_density_urban_residential.save(os.path.join(self.INTERMEDIATE, 'SILVIS_HIGH_DENSITY_URBAN'))

        high_density_urban_residential_bg = Con((~IsNull(self.urban_areas)) & (self.huden_bg > 10.0), self.raster_value)
        high_density_urban_residential_bg.save(os.path.join(self.INTERMEDIATE, 'SILVIS_HIGH_DENSITY_URBAN_BG'))

    def create_final_high_density_urban_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating final high-density urban residential raster...", time.ctime())
        high_density_urban_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'SILVIS_HIGH_DENSITY_URBAN'))
        high_density_urban_raster_bg = ap.Raster(os.path.join(self.INTERMEDIATE, 'SILVIS_HIGH_DENSITY_URBAN_BG'))

        final_high_density_urban_raster = Con((~IsNull(self.nlcd_ras)) & (~IsNull(high_density_urban_raster)), high_density_urban_raster)
        final_high_density_urban_raster.save(os.path.join(self.OUTPUTS, 'FINAL_HIGHDENSITYURBAN'))

        final_high_density_urban_raster_bg = Con((~IsNull(self.nlcd_ras)) & (~IsNull(high_density_urban_raster_bg)), high_density_urban_raster_bg)
        final_high_density_urban_raster_bg.save(os.path.join(self.OUTPUTS, 'FINAL_HIGHDENSITYURBAN_BG'))


class Commercial():
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

        self.raster_value = RASTER_VALUES['Commercial']

        self.create_silvis_lehd_commerical_raster()
        self.create_census_commercial_raster()
        self.create_esri_commercial_raster()
        self.create_final_commercial_raster()

    def create_silvis_lehd_commerical_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating commercial raster from UW Silvis blocks and LEHD...")

        lehd_commercial_raster = Con(self.comm > 0.0, Con((self.huden < 0.1) | (self.comm > (self.huden * 2)), Con(self.comm > self.indust, self.raster_value)))
        lehd_commercial_raster.save(os.path.join(self.INTERMEDIATE, 'LEHD_COMMERCIAL'))

        lehd_commercial_raster_bg = Con(self.comm_bg > 0.0, Con((self.huden_bg < 0.1) | (self.comm_bg > (self.huden_bg * 2)), Con(self.comm_bg > self.indust_bg, self.raster_value)))
        lehd_commercial_raster_bg.save(os.path.join(self.INTERMEDIATE, 'LEHD_COMMERCIAL_BG'))

    def create_census_commercial_raster(self):
        '''
        K2167 --> Convention Center
        K2300 --> Commercial Workplace
        K2361 --> Shopping Center
        K2363 --> Office Building or Office Park
        K2564 --> Amusement Center
        K2586 --> Zoo
        '''

        print(f"[Region {self.REGION_NUMBER}]   Creating commercial raster from TIGER2010...", time.ctime())
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
        D61 --> Shopping Center
        D64 --> Amusement Park
        D67 --> Stadium
        '''
        print(f"[Region {self.REGION_NUMBER}]   Creating commercial raster from ESRI_2010_lalndmrk_proj...", time.ctime())
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

    def create_final_commercial_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating final commercial raster...", time.ctime())

        tiger_commercial = os.path.join(self.INTERMEDIATE, 'TIGER_COMMERCIAL')
        lehd_commercial = os.path.join(self.INTERMEDIATE, 'LEHD_COMMERCIAL')
        lehd_commercial_bg = os.path.join(self.INTERMEDIATE, 'LEHD_COMMERCIAL_BG')
        esri_commercial = os.path.join(self.INTERMEDIATE, 'ESRI_COMMERCIAL')

        final_commercial_raster = Con(~IsNull(self.nlcd_ras), Con((self.nlcd_ras >= 21) & (self.nlcd_ras <= 24), Con((~IsNull(tiger_commercial)) | (~IsNull(lehd_commercial)) | (~IsNull(esri_commercial)), self.raster_value)))
        out_ras = os.path.join(self.OUTPUTS, 'FINAL_COMMERCIAL')
        final_commercial_raster.save(out_ras)

        final_commercial_raster_bg = Con(~IsNull(self.nlcd_ras), Con((self.nlcd_ras >= 21) & (self.nlcd_ras <= 24), Con((~IsNull(tiger_commercial)) | (~IsNull(lehd_commercial_bg)) | (~IsNull(esri_commercial)), self.raster_value)))
        out_ras = os.path.join(self.OUTPUTS, 'FINAL_COMMERCIAL_BG')
        final_commercial_raster_bg.save(out_ras)


class Industrial():
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

        self.raster_value = RASTER_VALUES['Industrial']

        self.create_silvis_lehd_industrial_raster()
        self.create_census_industrial_raster()
        self.create_esri_industrial_raster()
        self.create_usgs_industrial_raster()
        self.create_final_industrial_raster()

    def create_silvis_lehd_industrial_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating industrial raster from UW Silvis blocks and LEHD...")

        lehd_industrial_raster = Con(self.indust > 0.0, Con((self.huden < 0.1) | (self.indust > (self.huden * 2)), Con(self.indust >= self.comm, self.raster_value)))
        lehd_industrial_raster.save(os.path.join(self.INTERMEDIATE, 'LEHD_INDUSTRIAL'))

        lehd_industrial_raster_bg = Con(self.indust_bg > 0.0, Con((self.huden_bg < 0.1) | (self.indust_bg > (self.huden_bg * 2)), Con(self.indust_bg >= self.comm_bg, self.raster_value)))
        lehd_industrial_raster_bg.save(os.path.join(self.INTERMEDIATE, 'LEHD_INDUSTRIAL_BG'))

    def create_census_industrial_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating industrial raster from TIGER2010...", time.ctime())
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
        print(f"[Region {self.REGION_NUMBER}]   Creating industrial raster from ESRI_2010_lalndmrk_proj...", time.ctime())
        esri_landmarks = os.path.join(self.INPUTS, 'ESRI_2010_lalndmrk_proj')
        esri_query = "FCC = 'D62'"
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
        print(f"[Region {self.REGION_NUMBER}]   Creating industrial raster from NLCD_2011_Impervious_descriptor_L48_20190405...", time.ctime())

        nlcd_raster = ap.Raster(os.path.join(self.INPUTS, 'NLCD_2011_Impervious_descriptor_L48_20190405'))

        usgs_industrial_raster = Con((nlcd_raster == 11) | (nlcd_raster == 12), self.raster_value)
        usgs_industrial_raster.save(os.path.join(self.INTERMEDIATE, 'USGS_INDUSTRIAL'))

    def create_final_industrial_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating final industrial raster...", time.ctime())

        tiger_industrial = ap.Raster(os.path.join(self.INTERMEDIATE, 'TIGER_INDUSTRIAL'))
        lehd_industrial = ap.Raster(os.path.join(self.INTERMEDIATE, 'LEHD_INDUSTRIAL'))
        lehd_industrial_bg = ap.Raster(os.path.join(self.INTERMEDIATE, 'LEHD_INDUSTRIAL_BG'))
        esri_industrial = ap.Raster(os.path.join(self.INTERMEDIATE, 'ESRI_INDUSTRIAL'))
        usgs_industrial = ap.Raster(os.path.join(self.INTERMEDIATE, 'USGS_INDUSTRIAL'))

        final_industrial_raster = Con(~IsNull(self.nlcd_ras), Con((self.nlcd_ras >= 21) & (self.nlcd_ras <= 24), Con((~IsNull(tiger_industrial)) | (~IsNull(lehd_industrial)) | (~IsNull(esri_industrial)) | (~IsNull(usgs_industrial)), self.raster_value)))
        out_ras = os.path.join(self.OUTPUTS, 'FINAL_INDUSTRIAL')
        final_industrial_raster.save(out_ras)

        final_industrial_raster_bg = Con(~IsNull(self.nlcd_ras), Con((self.nlcd_ras >= 21) & (self.nlcd_ras <= 24), Con((~IsNull(tiger_industrial)) | (~IsNull(lehd_industrial_bg)) | (~IsNull(esri_industrial)) | (~IsNull(usgs_industrial)), self.raster_value)))
        out_ras = os.path.join(self.OUTPUTS, 'FINAL_INDUSTRIAL_BG')
        final_industrial_raster_bg.save(out_ras)


class MixedUse():
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

        self.raster_value = RASTER_VALUES['MixedUse']

        self.create_mixed_use_raster()

    def create_mixed_use_raster(self):

        final_mixed_use_raster = Con((~IsNull(self.nlcd_ras)) & (~IsNull(self.urban_areas)), Con((self.huden > 5.0) & (self.comm > 5.0) & (self.nlcd_ras >= 23) & (self.nlcd_ras <= 24), self.raster_value))
        out_ras = os.path.join(self.OUTPUTS, 'FINAL_MIXEDUSE')
        final_mixed_use_raster.save(out_ras)

        final_mixed_use_raster_bg = Con((~IsNull(self.nlcd_ras)) & (~IsNull(self.urban_areas)), Con((self.huden_bg > 5.0) & (self.comm_bg > 5.0) & (self.nlcd_ras >= 23) & (self.nlcd_ras <= 24), self.raster_value))
        out_ras = os.path.join(self.OUTPUTS, 'FINAL_MIXEDUSE_BG')
        final_mixed_use_raster_bg.save(out_ras)


class Institutional():
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.nlcd_ras = kwargs['nlcd_ras']
        self.urban_areas = kwargs['urban_areas']

        self.create_tiger_institutional_raster()
        self.create_esri_institutional_raster()
        self.create_dhs_institutional_raster()
        self.create_final_institutional_raster()

    def create_tiger_institutional_raster(self):
        '''
        K1231 --> Hospital
        K1233 --> Nursing Home
        K1234 --> County Home or Poor Farm
        K1235 --> Juvenile Institution
        K1236 --> Local Jail
        K1237 --> Federal Penitentiary
        K1238 --> Other Correctional Institution
        K1239 --> Convent or Monestary
        K1241 --> Sorority, Fraternity, or Dormitory
        K2100 --> Govermental
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

    def create_final_institutional_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating final institutional raster...", time.ctime())
        tiger_inst = ap.Raster(os.path.join(self.INTERMEDIATE, 'TIGER_INSTITUTIONAL'))
        tiger_mil = ap.Raster(os.path.join(self.INPUTS, 'MIL10'))
        esri_inst = ap.Raster(os.path.join(self.INTERMEDIATE, 'ESRI_INSTITUTIONAL'))
        dhs_inst = ap.Raster(os.path.join(self.INTERMEDIATE, 'DHS_INSTITUTIONAL'))

        dev = 20
        undev = 19

        final_institutional_raster = Con(~IsNull(self.nlcd_ras), Con((~IsNull(tiger_inst)) | (~IsNull(tiger_mil)) | (~IsNull(esri_inst)) | (~IsNull(dhs_inst)), Con((self.nlcd_ras >= 22) & (self.nlcd_ras <= 24), dev, undev)))
        final_developed_raster = ap.sa.ExtractByAttributes(in_raster=final_institutional_raster, where_clause=f'VALUE = {dev}')
        final_developed_raster.save(os.path.join(self.OUTPUTS, 'FINAL_INSTITUTIONALDEVELOPED'))

        final_undeveloped_raster = ap.sa.ExtractByAttributes(in_raster=final_institutional_raster, where_clause=f'VALUE = {undev}')
        final_undeveloped_raster.save(os.path.join(self.OUTPUTS, 'FINAL_INSTITUTIONALUNDEVELOPED'))


class Grayfield():
    '''
    Calculated at Block Group level only.
    '''
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.huden_bg = kwargs['huden_bg']
        self.jobden_bg = kwargs['jobden_bg']
        self.nlcd_ras = kwargs['nlcd_ras']
        self.place = kwargs['place']
        self.popden_bg = kwargs['popden_bg']
        self.urban_areas = kwargs['urban_areas']

        self.raster_value = RASTER_VALUES['Grayfield']

        self.create_silvis_grayfield_raster()
        self.create_final_grayfield_raster()

    def create_silvis_grayfield_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating grayfield residential raster from UW Silvis blocks...")

        grayfield = Con(~IsNull(self.urban_areas), Con((self.huden_bg > 0.5) & (self.popden_bg < 0.1), self.raster_value, Con((self.nlcd_ras == 23) | (self.nlcd_ras == 24), Con(self.jobden_bg < 0.01, self.raster_value))))
        grayfield.save(os.path.join(self.INTERMEDIATE, 'grayfield'))

    def create_final_grayfield_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating final grayfield residential raster...", time.ctime())
        grayfield_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'grayfield'))

        final_grayfield_raster = Con((~IsNull(self.nlcd_ras)) & (~IsNull(grayfield_raster)), grayfield_raster)
        final_grayfield_raster.save(os.path.join(self.OUTPUTS, 'FINAL_GRAYFIELD'))


class Transportation():
    def __init__(self, kwargs):
        self.REGION_NUMBER = kwargs['REGION_NUMBER']
        self.INPUTS = kwargs['INPUTS']
        self.OUTPUTS = kwargs['OUTPUTS']
        self.INTERMEDIATE = kwargs['INTERMEDIATE']

        self.nlcd_ras = kwargs['nlcd_ras']

        self.raster_value = RASTER_VALUES['Transportation']

        self.create_esri_transportation_raster()
        self.create_tiger_transportation_raster()
        self.create_usgs_transportation_raster()
        self.create_final_transportation_raster()

    def create_esri_transportation_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating transporation raster from ESRI_2010_rail/airportp...")
        esri_rails_raster = ap.Raster(os.path.join(self.INPUTS, 'ESRI_2010_rail'))
        esri_airport_raster = ap.Raster(os.path.join(self.INPUTS, 'ESRI_2010_airportp'))

        esri_rails_raster = Con(~IsNull(esri_rails_raster), Con((self.nlcd_ras >= 21) & (self.nlcd_ras <= 24), self.raster_value))
        esri_airport_raster = Con(~IsNull(esri_airport_raster), self.raster_value)
        esri_rails_raster.save(os.path.join(self.INTERMEDIATE, 'ESRI_RAILS'))
        esri_airport_raster.save(os.path.join(self.INTERMEDIATE, 'ESRI_AIRPORTS'))

    def create_tiger_transportation_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating transporation raster from TIGER2010...")

        tiger_ramps_raster = ap.Raster(os.path.join(self.INPUTS, 'TIGER2010_ramps'))

        tiger_transporation_raster = Con(~IsNull(tiger_ramps_raster), Con((self.nlcd_ras >= 21) & (self.nlcd_ras <= 24), self.raster_value))
        tiger_transporation_raster.save(os.path.join(self.INTERMEDIATE, 'TIGER_TRANSPORTATION'))

    def create_usgs_transportation_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating transporation raster from NLCD_2011_Impervious_descriptor_L48_20190405...")

        nlcd_raster = ap.Raster(os.path.join(self.INPUTS, 'NLCD_2011_Impervious_descriptor_L48_20190405'))

        # all primary and secondary roads identified by NLCD mapping program
        usgs_transportation_raster = Con((nlcd_raster >= 1) & (nlcd_raster <= 4), self.raster_value)
        usgs_transportation_raster.save(os.path.join(self.INTERMEDIATE, 'USGS_TRANSPORTATION'))

    def create_final_transportation_raster(self):
        print(f"[Region {self.REGION_NUMBER}]   Creating final transporation raster...", time.ctime())

        tiger_transportation_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'TIGER_TRANSPORTATION'))
        esri_rails_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'ESRI_RAILS'))
        esri_airports_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'ESRI_AIRPORTS'))
        usgs_transportation_raster = ap.Raster(os.path.join(self.INTERMEDIATE, 'USGS_TRANSPORTATION'))

        final_transporation_raster = Con((~IsNull(tiger_transportation_raster)) | (~IsNull(esri_rails_raster)) | (~IsNull(esri_airports_raster)) | (~IsNull(usgs_transportation_raster)), self.raster_value)
        final_transporation_raster.save(os.path.join(self.OUTPUTS, 'FINAL_TRANSPORTATION'))


def combine_all_layers(kwargs):
    print(f"[Region {kwargs['REGION_NUMBER']}]   Combining all land use layers...", time.ctime())

    REGION_NUMBER = kwargs['REGION_NUMBER']
    OUTPUTS = kwargs['OUTPUTS']

    raster_list = [os.path.join(OUTPUTS, f'FINAL_{key.upper()}') for key in RASTER_VALUES]

    # Census Block

    transportation = ap.Raster(os.path.join(OUTPUTS, 'FINAL_TRANSPORTATION'))
    water = ap.Raster(os.path.join(OUTPUTS, 'FINAL_WATER'))
    wetlands = ap.Raster(os.path.join(OUTPUTS, 'FINAL_WETLANDS'))
    conservation = ap.Raster(os.path.join(OUTPUTS, 'FINAL_CONSERVATION'))
    parksgolf = ap.Raster(os.path.join(OUTPUTS, 'FINAL_PARKSGOLF'))

    max_value_raster = ap.sa.CellStatistics(in_rasters_or_constants=raster_list,
                                            statistics_type='MAXIMUM')
    out_raster = Con(~IsNull(transportation), transportation, Con(~IsNull(water), water, Con(~IsNull(wetlands), wetlands, Con(~IsNull(conservation), conservation, Con(~IsNull(parksgolf), parksgolf, max_value_raster)))))
    out_raster.save(os.path.join(OUTPUTS, f'NLUD_2010_R{REGION_NUMBER}'))

    in_raster = os.path.join(OUTPUTS, f'NLUD_2010_R{REGION_NUMBER}')
    input_CLR_file = 'D:\\projects\\EPA_NLUD\\EPA_NLUD_colormap_compare_20191230.clr'
    ap.AddColormap_management(in_raster=in_raster, input_CLR_file=input_CLR_file)

    # Census Block Group

    BGs = ('Commercial', 'Industrial', 'MixedUse', 'HighDensityUrban', 'Urban', 'Suburban', 'Exurban', 'Rural')
    raster_list = [os.path.join(OUTPUTS, f'FINAL_{key.upper()}_BG') if key in BGs else os.path.join(OUTPUTS, f'FINAL_{key.upper()}') for key in RASTER_VALUES]

    max_value_raster = ap.sa.CellStatistics(in_rasters_or_constants=raster_list,
                                            statistics_type='MAXIMUM')
    out_raster = Con(~IsNull(transportation), transportation, Con(~IsNull(water), water, Con(~IsNull(wetlands), wetlands, Con(~IsNull(conservation), conservation, Con(~IsNull(parksgolf), parksgolf, max_value_raster)))))
    out_raster.save(os.path.join(OUTPUTS, f'NLUD_2010_R{REGION_NUMBER}_BG'))

    in_raster = os.path.join(OUTPUTS, f'NLUD_2010_R{REGION_NUMBER}_BG')
    input_CLR_file = 'D:\\projects\\EPA_NLUD\\EPA_NLUD_colormap_compare_20191230.clr'
    ap.AddColormap_management(in_raster=in_raster, input_CLR_file=input_CLR_file)

    print(f"[Region {kwargs['REGION_NUMBER']}]   Finished!")


def mosaic_regions(region_numbers):
    '''
    Census Block
    '''
    input_rasters = []
    for num in region_numbers:
        input_rasters.append(f'D:\\projects\\EPA_NLUD\\region_{num}\\OUTPUTS_2010.gdb\\NLUD_2010_R{num}')

    print("Mosaicking CONUS rasters...", end='')
    ap.MosaicToNewRaster_management(input_rasters=input_rasters,
                                    output_location='D:\\projects\\EPA_NLUD\\CONUS.gdb',
                                    raster_dataset_name_with_extension='EPA_NLUD_2010_CONUS',
                                    pixel_type='8_BIT_UNSIGNED',
                                    cellsize=30,
                                    number_of_bands=1)

    # Census Block Group

    input_rasters = []
    for num in region_numbers:
        input_rasters.append(f'D:\\projects\\EPA_NLUD\\region_{num}\\OUTPUTS_2010.gdb\\NLUD_2010_R{num}_BG')

    ap.MosaicToNewRaster_management(input_rasters=input_rasters,
                                    output_location='D:\\projects\\EPA_NLUD\\CONUS.gdb',
                                    raster_dataset_name_with_extension='EPA_NLUD_2010_CONUS_BG',
                                    pixel_type='8_BIT_UNSIGNED',
                                    cellsize=30,
                                    number_of_bands=1)

    print("finished!")


def worker_function(region_number=None):

    kws = {'REGION_NUMBER': region_number}
    kws['INPUTS'] = f'D:\\projects\\EPA_NLUD\\region_{region_number}\\INPUTS_2010.gdb'

    kws['OUTPUTS'] = f'D:\\projects\\EPA_NLUD\\region_{region_number}\\OUTPUTS_2010.gdb'
    kws['INTERMEDIATE'] = f'D:\\projects\\EPA_NLUD\\region_{region_number}\\INTERMEDIATE_2010.gdb'

    if not ap.Exists(kws['INTERMEDIATE']):
        ap.CreateFileGDB_management(out_folder_path=os.path.dirname(kws['INTERMEDIATE']),
                                    out_name=os.path.basename(kws['INTERMEDIATE']))

    if not ap.Exists(kws['OUTPUTS']):
        ap.CreateFileGDB_management(out_folder_path=os.path.dirname(kws['OUTPUTS']),
                                    out_name=os.path.basename(kws['OUTPUTS']))

    kws['comm'] = ap.Raster(os.path.join(kws['INPUTS'], 'COMMDEN10'))
    kws['indust'] = ap.Raster(os.path.join(kws['INPUTS'], 'INDUSTDEN10'))
    kws['huden'] = ap.Raster(os.path.join(kws['INPUTS'], 'HUDEN10'))
    kws['popden'] = ap.Raster(os.path.join(kws['INPUTS'], 'POPDEN10'))
    kws['jobden'] = ap.Raster(os.path.join(kws['INPUTS'], 'JOBDEN10'))

    kws['comm_bg'] = ap.Raster(os.path.join(kws['INPUTS'], 'COMMDEN10_BG'))
    kws['indust_bg'] = ap.Raster(os.path.join(kws['INPUTS'], 'INDUSTDEN10_BG'))
    kws['huden_bg'] = ap.Raster(os.path.join(kws['INPUTS'], 'HUDEN10_BG'))
    kws['popden_bg'] = ap.Raster(os.path.join(kws['INPUTS'], 'POPDEN10_BG'))
    kws['jobden_bg'] = ap.Raster(os.path.join(kws['INPUTS'], 'JOBDEN10_BG'))

    kws['urban_areas'] = ap.Raster(os.path.join(kws['INPUTS'], 'UAC10'))
    kws['place'] = ap.Raster(os.path.join(kws['INPUTS'], 'PLACE10'))
    kws['nlcd_ras'] = ap.Raster(os.path.join(kws['INPUTS'], 'NLCD_2011_Land_Cover_L48_20190424_roads_thinned'))

    ap.env.snapRaster = kws['nlcd_ras']
    ap.env.extent = kws['nlcd_ras']
    ap.env.mask = kws['nlcd_ras']

    Transportation(kwargs=kws)
    Institutional(kwargs=kws)
    MixedUse(kwargs=kws)
    Industrial(kwargs=kws)
    Commercial(kwargs=kws)
    HighDensityUrban(kwargs=kws)
    Urban(kwargs=kws)
    Grayfield(kwargs=kws)
    Suburban(kwargs=kws)
    Exurban(kwargs=kws)
    Rural(kwargs=kws)
    ParksGolf(kwargs=kws)
    Cropland(kwargs=kws)
    Pasture(kwargs=kws)
    PrivateGrassShrub(kwargs=kws)
    PrivateForest(kwargs=kws)
    RecreationExtraction(kwargs=kws)
    Conservation(kwargs=kws)
    Wetlands(kwargs=kws)
    Water(kwargs=kws)

    combine_all_layers(kws)


def blend_layers():
    '''
    Use the Block Group version of the NLUD where ever the Block version has a
    null value.
    '''

    print("Blending NLUD layers...", end="")
    inputs_gdb = f'D:\\projects\\EPA_NLUD\\dev\\INPUTS_2010.gdb'
    nlcd = ap.Raster(os.path.join(inputs_gdb, 'NLCD_2011_Land_Cover_L48_20190424_roads_thinned'))

    gdb = 'D:\\projects\\EPA_NLUD\\CONUS.gdb'
    block_raster = ap.Raster(os.path.join(gdb, 'EPA_NLUD_2010_CONUS'))
    block_group_raster = ap.Raster(os.path.join(gdb, 'EPA_NLUD_2010_CONUS_BG'))

    water_value = RASTER_VALUES['Water']
    wetlands_value = RASTER_VALUES['Wetlands']

    # Use the Block Group-level NLUD to fill-in gaps in the Block-level NLUD
    # If there are any remaining gaps, fill them in with water or wetlands as
    # appropriate.
    final = Con(IsNull(block_raster), Con(~IsNull(block_group_raster), block_group_raster, Con(nlcd == 11, water_value, Con((nlcd == 90) | (nlcd == 95), wetlands_value))), block_raster)

    final.save(os.path.join(gdb, 'EPA_NLUD_2010_CONUS_FINAL'))

    in_raster = os.path.join(gdb, 'EPA_NLUD_2010_CONUS_FINAL')
    input_CLR_file = 'D:\\projects\\EPA_NLUD\\EPA_NLUD_colormap_compare_20191230.clr'
    ap.AddColormap_management(in_raster=in_raster, input_CLR_file=input_CLR_file)

    ap.BuildPyramidsandStatistics_management(in_workspace=in_raster,
                                             include_subdirectories='NONE',
                                             skip_existing='OVERWRITE')

    print("finished!")


def main():

    regions_to_process = range(1, 8)
    regions_to_mosaick = range(1, 8)

    pool = Pool(processes=7, maxtasksperchild=1)
    pool.map(func=worker_function, iterable=regions_to_process)
    pool.close()
    pool.terminate()

    mosaic_regions(regions_to_mosaick)
    blend_layers()


if __name__ == '__main__':
    main()
