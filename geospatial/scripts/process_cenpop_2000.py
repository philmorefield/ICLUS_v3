import glob
import os
import sqlite3

import arcpy as ap
import pandas as pd

ap.env.overwriteOutput = True
ANALYSIS_DB = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases\\analysis.sqlite'
CENSUS_FOLDER = 'D:\\OneDrive\\Dissertation\\data\\Census\\center_of_population\\2000'
GDB = 'D:\\OneDrive\\ICLUS_v3\\geospatial\\iclusv3_geospatial.gdb'
MIGRATION_DB = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases\\migration.sqlite'


def join_attrs_to_near_table(near_table):
    near_arr = ap.da.TableToNumPyArray(in_table=near_table,
                                       field_names=['IN_FID', 'NEAR_FID', 'NEAR_DIST'])
    df = pd.DataFrame.from_records(near_arr)

    # this is used to attach origin and destination identifying information
    pop_cols = ['OBJECTID', 'COFIPS', 'CYNAME', 'STUSPS', 'POPULATION']
    population_shps_path = os.path.join(GDB, 'CenPop2000')
    population_arr = ap.da.TableToNumPyArray(in_table=population_shps_path,
                                             field_names=pop_cols)
    pop_df = pd.DataFrame.from_records(population_arr)

    df = df.merge(right=pop_df,
                 how='left',
                 left_on='IN_FID',
                 right_on='OBJECTID',
                 copy=False)

    rename_dict = {'POPULATION': 'Oi',
                   'CYNAME': 'ORIGIN_CYNAME',
                   'STUSPS': 'ORIGIN_STATE',
                   'COFIPS': 'ORIGIN_FIPS',
                   'NEAR_DIST': 'Dij'}

    df.rename(rename_dict, axis=1, inplace=True)
    df.drop(['OBJECTID', 'IN_FID'], axis=1, inplace=True)

    df = df.merge(right=pop_df,
                  how='left',
                  left_on='NEAR_FID',
                  right_on='OBJECTID',
                  copy=False)

    rename_dict = {'POPULATION': 'Dj',
                   'CYNAME': 'DESTINATION_CYNAME',
                   'STUSPS': 'DESTINATION_STATE',
                   'COFIPS': 'DESTINATION_FIPS'}
    df.rename(rename_dict, axis=1, inplace=True)
    df.drop(['OBJECTID', 'NEAR_FID'], axis=1, inplace=True)

    df['Dij'] = (df['Dij'] * 0.0006213712).round(1)

    con = sqlite3.connect(MIGRATION_DB)
    valid_df = pd.read_sql_query('SELECT * FROM valid_cyfips', con=con)
    con.close()

    assert not df.isnull().any().any()
    assert set(df['ORIGIN_FIPS']).issubset(valid_df['CYFIPS'])
    assert set(df['DESTINATION_FIPS']).issubset(valid_df['CYFIPS'])

    df = df[['ORIGIN_FIPS',
             'ORIGIN_CYNAME',
             'ORIGIN_STATE',
             'DESTINATION_FIPS',
             'DESTINATION_CYNAME',
             'DESTINATION_STATE',
             'Dij']]
    con = sqlite3.connect(ANALYSIS_DB)
    df.to_sql(name='county_to_county_distance_2000',
              con=con,
              if_exists='replace',
              index=False)
    con.close()


def main():
    txts = glob.glob(os.path.join(CENSUS_FOLDER, '*.txt'))
    cols = ['STFIPS', 'CYFIPS', 'CYNAME', 'POPULATION', 'LAT', 'LON']
    df = pd.concat([pd.read_csv(txt, header=None, names=cols) for txt in txts], ignore_index=True)
    df['COFIPS'] = df['STFIPS'].astype('str').str.zfill(2) + df['CYFIPS'].astype('str').str.zfill(3)
    df.drop(labels=['STFIPS', 'CYFIPS'], axis=1, inplace=True)

    con = sqlite3.connect(MIGRATION_DB)
    changes = pd.read_sql_query('SELECT OLD_FIPS, NEW_FIPS FROM fips_or_name_changes', con=con)
    valid = pd.read_sql_query('SELECT * FROM valid_cyfips', con=con)
    con.close()

    df = df.merge(right=changes, how='left', left_on='COFIPS', right_on='OLD_FIPS', copy=False)
    df.loc[~pd.isnull(df.NEW_FIPS), 'COFIPS'] = df['NEW_FIPS']
    df = df[['COFIPS', 'POPULATION', 'LAT', 'LON']]
    p1 = df.POPULATION.sum().sum()
    df['SUM_POP'] = df['POPULATION'].groupby(df['COFIPS']).transform('sum')
    df['LAT_x_POP'] = df['LAT'] * df['POPULATION']
    df['LON_x_POP'] = df['LON'] * df['POPULATION']
    df['SUM_LAT_x_POP'] = df['LAT_x_POP'].groupby(df['COFIPS']).transform('sum')
    df['SUM_LON_x_POP'] = df['LON_x_POP'].groupby(df['COFIPS']).transform('sum')
    df['LAT_WGT_AVG'] = df['SUM_LAT_x_POP'] / df['SUM_POP']
    df['LON_WGT_AVG'] = df['SUM_LON_x_POP'] / df['SUM_POP']

    df = df.groupby('COFIPS', as_index=False).agg({'POPULATION': 'sum', 'LAT_WGT_AVG': 'mean', 'LON_WGT_AVG': 'mean'})
    df.rename({'LON_WGT_AVG': 'LON', 'LAT_WGT_AVG': 'LAT'}, axis=1, inplace=True)
    df = df.merge(right=valid, how='left', left_on='COFIPS', right_on='CYFIPS', copy=False)
    df['CYNAME'] = df['CYNAME'].str.encode('latin-1')
    p2 = df.POPULATION.sum().sum()

    assert p1 == p2
    assert not df.isnull().any().any()
    assert set(df['COFIPS']).issubset(valid['CYFIPS'])

    sr = ap.SpatialReference(4326)
    arr = df[['COFIPS', 'CYNAME', 'STUSPS', 'POPULATION', 'LAT', 'LON']].to_records(index=False)
    arr = arr.astype([('COFIPS', '|S5'), ('CYNAME', '|S100'), ('STUSPS', '|S2'), ('POPULATION', '<i8'), ('LAT', '<f8'), ('LON', '<f8')])
    fc = os.path.join(GDB, 'CenPop2000')

    if ap.Exists(dataset=fc):
        ap.Delete_management(in_data=fc)

    ap.da.NumPyArrayToFeatureClass(in_array=arr,
                                   out_table=fc,
                                   shape_fields=('LON', 'LAT'),
                                   spatial_reference=sr)

    out_table = os.path.join(GDB, 'CenPop_2000_CY_API_Near_Table')
    ap.GenerateNearTable_analysis(in_features=fc,
                                  near_features=fc,
                                  out_table=out_table,
                                  search_radius=None,
                                  location="NO_LOCATION",
                                  angle="NO_ANGLE",
                                  closest="ALL",
                                  closest_count=0,
                                  method="GEODESIC")
    join_attrs_to_near_table(near_table=out_table)

    print("\nDone!\n")


if __name__ == '__main__':
    main()
