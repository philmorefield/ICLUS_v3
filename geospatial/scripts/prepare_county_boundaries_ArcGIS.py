import os
import sqlite3

import arcpy as ap
import pandas as pd

# D:\OneDrive\Data\USGS\2021
PROJECT_FOLDER = 'D:\\OneDrive\\ICLUS_v3'
CENSUS_GDB = os.path.join(PROJECT_FOLDER, 'geospatial', 'Census', '2020', 'tlgdb_2020_a_us_substategeo.gdb')
MIGRATION_DATABASE = os.path.join(PROJECT_FOLDER, 'population\\inputs\\databases\\migration.sqlite')


def rename_attribute():
    in_table = os.path.join(CENSUS_GDB, 'County')
    ap.management.AlterField(in_table=in_table,
                             field='geoid',
                             new_field_name='GEOID')

def get_fips_changes():
    sql = 'SELECT OLD_FIPS, NEW_FIPS FROM fips_or_name_changes'
    con = sqlite3.connect(database=MIGRATION_DATABASE, timeout=300)

    df = pd.read_sql(sql=sql, con=con)
    con.close()

    return df

def merge_attributes(df):
    ap.Delete_management('memory')
    arr = df.to_records(index=False)
    ap.da.NumPyArrayToTable(in_array=arr, out_table='memory\\temp_merge_attributes')

    in_table = os.path.join(CENSUS_GDB, 'County')
    ap.management.JoinField(in_data=in_table,
                            in_field='GEOID',
                            join_table='memory\\temp_merge_attributes',
                            join_field='OLD_FIPS')

    # gdf = gdf.dissolve(by='GEOID').reset_index()[['GEOID', 'geometry']]

    # gdf.to_file(GEOPKG, layer='conus_ak_hi_county_2020_DISSOLVED')

    return

def check_valid_geoid(gdf):
    sql = 'SELECT * FROM valid_cyfips'
    con = sqlite3.connect(MIGRATION_DATABASE, timeout=300)
    df = pd.read_sql(sql=sql, con=con)

    gdf = gdf.merge(right=df, how='left', left_on='GEOID', right_on='CYFIPS')

    assert gdf.loc[gdf.CYFIPS.isnull()].shape[0] == 0


def main():
    rename_attribute()
    df = get_fips_changes()
    gdf = merge_attributes(df=df)
    check_valid_geoid(gdf)


if __name__ == '__main__':
    main()