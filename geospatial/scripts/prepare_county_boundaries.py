import os
import sqlite3

import geopandas as gpd
import pandas as pd


PROJECT_FOLDER = 'D:\\OneDrive\\ICLUS_v3'
MIGRATION_DATABASE = os.path.join(PROJECT_FOLDER, 'population\\inputs\\databases\\migration.sqlite')
GEOPKG = os.path.join(PROJECT_FOLDER, 'geospatial\\iclus_v3_geospatial.gpkg')

def get_county_boundaries():

    layer = 'conus_ak_hi_county_2020'

    gdf = gpd.read_file(filename=GEOPKG, layer=layer)
    gdf.rename(columns={'geoid': 'GEOID'}, inplace=True)
    gdf = gdf[['GEOID', 'geometry']]

    return gdf

def get_fips_changes():
    sql = 'SELECT OLD_FIPS, NEW_FIPS FROM fips_or_name_changes'
    con = sqlite3.connect(database=MIGRATION_DATABASE, timeout=300)

    df = pd.read_sql(sql=sql, con=con)
    con.close()

    return df

def merge_attributes(gdf, df):
    gdf = gdf.merge(right=df, how='left', left_on='GEOID', right_on='OLD_FIPS')
    gdf.loc[~gdf.NEW_FIPS.isnull(), 'GEOID'] = gdf['NEW_FIPS']
    gdf = gdf.dissolve(by='GEOID').reset_index()[['GEOID', 'geometry']]

    gdf.to_file(GEOPKG, layer='conus_ak_hi_county_2020_DISSOLVED')

    return gdf

def check_valid_geoid(gdf):
    sql = 'SELECT * FROM valid_cyfips'
    con = sqlite3.connect(MIGRATION_DATABASE, timeout=300)

    df = pd.read_sql(sql=sql, con=con)
    gdf = gdf.merge(right=df, how='left', left_on='GEOID', right_on='CYFIPS')

    assert gdf.loc[gdf.CYFIPS.isnull()].shape[0] == 0


def main():
    gdf_temp = get_county_boundaries()
    df = get_fips_changes()
    gdf = merge_attributes(gdf=gdf_temp, df=df)
    check_valid_geoid(gdf)


if __name__ == '__main__':
    main()