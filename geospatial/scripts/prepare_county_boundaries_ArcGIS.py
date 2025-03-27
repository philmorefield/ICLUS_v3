import os
import sqlite3

import arcpy as ap
import pandas as pd

PROJECT_FOLDER = 'D:\\OneDrive\\ICLUS_v3'
CENSUS_GDB = os.path.join(PROJECT_FOLDER, 'geospatial', 'Census', '2020', 'tlgdb_2020_a_us_substategeo.gdb')
ICLUS_GDB = os.path.join(PROJECT_FOLDER, 'geospatial', 'iclusv3_geospatial.gdb')
MIGRATION_DATABASE = os.path.join(PROJECT_FOLDER, 'population\\inputs\\databases\\migration.sqlite')


def rename_attribute():
    in_table = os.path.join(CENSUS_GDB, 'County')
    ap.management.AlterField(in_table=in_table,
                             field='geoid',
                             new_field_name='GEOID')

def apply_fips_changes():
    ''''
    This function updates county FIPS codes and subsequently dissolves county
    boundaries based on those changes. This function is expecting a raw
    (i.e., unaltered) county boundary shapefile from the Census Bureau.
    '''
    print("Applying FIPS changes to Census county boundaries...")
    sql = 'SELECT OLD_FIPS, NEW_FIPS FROM fips_or_name_changes'
    con = sqlite3.connect(database=MIGRATION_DATABASE, timeout=300)
    df = pd.read_sql(sql=sql, con=con)
    con.close()

    ap.Delete_management('memory\\temp_merge_attributes')
    arr = df.to_records(index=False).astype([('OLD_FIPS', '<S5'), ('NEW_FIPS', '<S5')])
    ap.da.NumPyArrayToTable(in_array=arr, out_table='memory\\temp_merge_attributes')

    in_table = os.path.join(CENSUS_GDB, 'County')
    ap.management.JoinField(in_data=in_table,
                            in_field='GEOID',
                            join_table='memory\\temp_merge_attributes',
                            join_field='OLD_FIPS')

    in_layer_or_view = os.path.join(CENSUS_GDB, 'County')
    ap.management.SelectLayerByAttribute(in_layer_or_view=in_layer_or_view,
                                         selection_type="NEW_SELECTION",
                                         where_clause="NEW_FIPS IS NULL")

    ap.management.CalculateField(in_table=in_table,
                                 field="NEW_FIPS",
                                 expression="!GEOID!",
                                 expression_type="PYTHON3")

    ap.management.SelectLayerByAttribute(in_layer_or_view=in_layer_or_view,
                                         selection_type="REMOVE_FROM_SELECTION",
                                         where_clause="")

    in_features = os.path.join(CENSUS_GDB, 'County')
    out_features = os.path.join(ICLUS_GDB, 'County_DISSOLVE1')
    ap.analysis.PairwiseDissolve(in_features=in_features,
                                 out_feature_class=out_features,
                                 dissolve_field="NEW_FIPS",
                                 statistics_fields="NEW_NAME FIRST",
                                 multi_part="MULTI_PART")


def rasterize_counties():
    print("Rasterizing county boundaries...")
    in_features = os.path.join(ICLUS_GDB, 'County_DISSOLVE1')
    out_raster = os.path.join(ICLUS_GDB, 'County_RASTER2')
    ap.conversion.PolygonToRaster(in_features=in_features,
                                  value_field='NEW_FIPS',
                                  out_rasterdataset=out_raster,
                                  cellsize=30)

    ap.management.BuildPyramids(in_raster_dataset=out_raster)

def main():
    apply_fips_changes()
    rasterize_counties()

    rename_attribute()
    ...

if __name__ == '__main__':
    main()