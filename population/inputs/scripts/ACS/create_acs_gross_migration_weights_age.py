import os
import sqlite3

from itertools import product

import pandas as pd

pd.set_option("display.max_columns", None) # show all cols

BASE_FOLDER = 'D:\\projects\\ICLUS_v3\\population'
if os.path.isdir('D:\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'

DATABASE_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\databases')
MIGRATION_DB = os.path.join(DATABASE_FOLDER, 'migration.sqlite')
POPULATION_DB = os.path.join(DATABASE_FOLDER, 'population.sqlite')
ANALYSIS_DB = os.path.join(DATABASE_FOLDER, 'analysis.sqlite')
ACS_DB = os.path.join(DATABASE_FOLDER, 'acs.sqlite')
CENSUS_CSV_PATH = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\Census\\2020\\decennial\\population_by_age')

ACS_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\ACS\\2011_2015')

ACS_AGE_GROUP_MAP = {1: '1_TO_4',
                     2: '5_TO_17',
                     3: '18_TO_19',
                     4: '20_TO_24',
                     5: '25_TO_29',
                     6: '30_TO_34',
                     7: '35_TO_39',
                     8: '40_TO_44',
                     9: '45_TO_49',
                     10: '50_TO_54',
                     11: '55_TO_59',
                     12: '60_TO_64',
                     13: '65_TO_69',
                     14: '70_TO_74',
                     15: '75_TO_OVER'}

CENSUS_AGE_GROUPS = ['0_TO_4', '5_TO_9', '10_TO_14', '15_TO_17', '18_TO_19',
                     '20', '21', '22_TO_24', '25_TO_29',
                     '30_TO_34', '35_TO_39', '40_TO_44', '45_TO_49',
                     '50_TO_54', '55_TO_59', '60_TO_61', '62_TO_64',
                     '65_TO_66', '67_TO_69', '70_TO_74', '75_TO_79',
                     '80_TO_84', '85_AND_OVER']

def make_fips_changes(df):
    con =sqlite3.connect(MIGRATION_DB)
    query = 'SELECT OLD_FIPS AS COFIPS, NEW_FIPS \
             FROM fips_or_name_changes'
    df_fips = pd.read_sql_query(sql=query, con=con)
    con.close()

    if 'ORIGIN_FIPS' in df.columns and 'DESTINATION_FIPS' in df.columns:
        df = df.merge(right=df_fips,
                    how='left',
                    left_on='ORIGIN_FIPS',
                    right_on='COFIPS')
        df.loc[~df.NEW_FIPS.isnull(), 'ORIGIN_FIPS'] = df['NEW_FIPS']
        df = df.drop(columns=['NEW_FIPS', 'COFIPS'])

        df = df.merge(right=df_fips,
                    how='left',
                    left_on='DESTINATION_FIPS',
                    right_on='COFIPS')
        df.loc[~df.NEW_FIPS.isnull(), 'DESTINATION_FIPS'] = df['NEW_FIPS']
        df = df.drop(columns=['NEW_FIPS', 'COFIPS'])

        if 'RACE' in df.columns:
            df = df.groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'RACE'], as_index=False).sum()
        elif 'AGE_GROUP' in df.columns:
            df = df.groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'AGE_GROUP'], as_index=False).sum()
        else:
            df = df.groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS'], as_index=False).sum()

    elif 'COFIPS' in df.columns:
        df = df.merge(right=df_fips,
                    how='left',
                    on='COFIPS')

        df.loc[~df.NEW_FIPS.isnull(), 'COFIPS'] = df['NEW_FIPS']
        df = df.drop(columns='NEW_FIPS')

        if 'RACE' in df.columns:
            df = df.groupby(by=['COFIPS', 'RACE'], as_index=False).sum()
        elif 'AGE_GROUP' in df.columns:
            df = df.groupby(by=['COFIPS', 'AGE_GROUP'], as_index=False).sum()
        else:
            df = df.groupby(by='COFIPS', as_index=False).sum()

    return df


def get_euclidean_distance():
    query = 'SELECT ORIGIN_FIPS, DESTINATION_FIPS, Dij \
             FROM county_to_county_distance_2020'
    con = sqlite3.connect(ANALYSIS_DB)
    df = pd.read_sql_query(sql=query, con=con)
    con.close()

    assert not df.isnull().any().any()

    return df

def get_census_2020_county_population_by_age_():
    csv_name = 'DECENNIALDHC2020.P12-Data.csv'
    csv = os.path.join(CENSUS_CSV_PATH, csv_name)
    df = pd.read_csv(filepath_or_buffer=csv,
                     skiprows=1,
                     encoding='latin-1')

    other_columns = [f'{gender}_{age_group}' for gender, age_group in list(product(['MALE', 'FEMALE'], CENSUS_AGE_GROUPS))]
    df.columns = ['COFIPS'] + other_columns
    df['COFIPS'] = df['COFIPS'].str[-5:]

    df = make_fips_changes(df)

    df = df.melt(id_vars='COFIPS', var_name='AGE_GROUP', value_name='ORIGIN_POPULATION_CENSUS')
    df['AGE_GROUP'] = df['AGE_GROUP'].str.split(pat='_', n=1).str[1]

    df['CO_POP_NOT_OTHER'] = df.query('AGE_GROUP != "OTHER"').groupby(by='COFIPS')['ORIGIN_POPULATION_CENSUS'].transform('sum')
    df['TOTAL_POP'] = df.groupby(by='COFIPS', as_index=False)['ORIGIN_POPULATION_CENSUS'].transform('sum')
    df['PCT_POP_NOT_OTHER'] =  df['CO_POP_NOT_OTHER'] / df['TOTAL_POP']
    df = df[['COFIPS', 'AGE_GROUP', 'ORIGIN_POPULATION_CENSUS', 'PCT_POP_NOT_OTHER']].query('AGE_GROUP != "OTHER"')

    return df


def get_acs_2011_2015_migration():
    xl_filename = 'county-to-county-by-age-2011-2015-current-residence-sort.xlsx'

    columns = ('D_STFIPS', 'D_COFIPS', 'O_STFIPS', 'O_COFIPS', 'AGE_GROUP',
               'D_STATE', 'D_COUNTY', 'D_POP', 'D_POP_MOE', 'D_NONMOVERS',
               'D_NONMOVERS_MOE', 'D_MOVERS', 'D_MOVERS_MOE',
               'D_MOVERS_SAME_CY', 'D_MOVERS_SAME_CY_MOE',
               'D_MOVERS_FROM_DIFF_CY_SAME_ST',
               'D_MOVERS_FROM_DIFF_CY_SAME_ST_MOE', 'D_MOVERS_FROM_DIFF_ST',
               'D_MOVERS_DIFF_ST_MOE', 'D_MOVERS_FROM_ABROAD',
               'D_MOVERS_FROM_ABROAD_MOE', 'O_STATE', 'O_COUNTY',
               'ORIGIN_POPULATION_ACS', 'O_POP_MOE', 'O_NONMOVERS',
               'O_NOMMOVERS_MOE', 'O_MOVERS', 'O_MOVERS_MOE',
               'O_MOVERS_SAME_CY', 'O_MOVERS_SAME_CY_MOE',
               'O_MOVERS_FROM_DIFF_CY_SAME_ST',
               'O_MOVERS_FROM_DIFF_CY_SAME_ST_MOE', 'O_MOVERS_FROM_DIFF_ST',
               'O_MOVERS_DIFF_ST_MOE', 'O_MOVERS_PUERTO_RICO',
               'O_MOVERS_PUERTO_RICO_MOE', 'TOTAL_FLOW', 'TOTAL_FLOW_MOE')

    xls = pd.ExcelFile(os.path.join(ACS_FOLDER, 'migration', xl_filename))
    df = pd.concat([xls.parse(sheet_name=name, header=None, names=columns, skiprows=4, skipfooter=8) for name in xls.sheet_names if name != 'Puerto Rico'])

    df = df[~df.O_STFIPS.str.contains('XXX')]
    foreign = ('EUR', 'ASI', 'SAM', 'ISL', 'NAM', 'CAM', 'CAR', 'AFR', 'OCE')
    df = df.loc[~df.O_STFIPS.isin(foreign), ['D_STFIPS', 'D_COFIPS', 'O_STFIPS', 'O_COFIPS', 'AGE_GROUP', 'ORIGIN_POPULATION_ACS', 'TOTAL_FLOW']]

    df['D_STFIPS'] = df.D_STFIPS.astype(int).astype(str).str.zfill(2)
    df['D_COFIPS'] = df.D_COFIPS.astype(int).astype(str).str.zfill(3)
    df['DESTINATION_FIPS'] = df.D_STFIPS + df.D_COFIPS

    df['O_STFIPS'] = df.O_STFIPS.astype(int).astype(str).str.zfill(2)
    df['O_COFIPS'] = df.O_COFIPS.astype(int).astype(str).str.zfill(3)
    df['ORIGIN_FIPS'] = df.O_STFIPS + df.O_COFIPS

    df['AGE_GROUP'] = df.AGE_GROUP.replace(to_replace=ACS_AGE_GROUP_MAP)
    df = df[['DESTINATION_FIPS', 'ORIGIN_FIPS', 'AGE_GROUP', 'ORIGIN_POPULATION_ACS', 'TOTAL_FLOW']]
    df = df.sort_values(by=['ORIGIN_FIPS', 'AGE_GROUP', 'DESTINATION_FIPS'])
    df = df.loc[~df.ORIGIN_POPULATION_ACS.isnull()]

    # df['TOTAL_FLOW'] = df['TOTAL_FLOW'].copy().astype(float)

    return df

def get_gross_migration_ratios_by_age():
    origin_age = get_census_2020_county_population_by_age_()
    migration = get_acs_2011_2015_migration()

    df = migration.merge(right=origin_age,
                         left_on=['ORIGIN_FIPS', 'AGE_GROUP'],
                         right_on=['COFIPS', 'AGE_GROUP'],
                         how='left')
    df = df.drop(columns=['COFIPS'])

    df['SUM_CENSUS_A_N_T_POPULATION_ORIGIN'] = df.loc[df.AGE_GROUP.isin(['AIAN', 'NHPI', 'TWO_OR_MORE'])].groupby(by='ORIGIN_FIPS', as_index=False)['ORIGIN_POPULATION_CENSUS'].transform('sum')
    df.loc[df.AGE.isin(['AIAN', 'NHPI', 'TWO_OR_MORE']), 'TOTAL_FLOW'] = (df.ORIGIN_POPULATION_CENSUS / df.SUM_CENSUS_A_N_T_POPULATION_ORIGIN) * (df.TOTAL_FLOW * df.PCT_POP_NOT_OTHER)

    df['AGE_MIGRATION_FRACTION'] = df['TOTAL_FLOW'] / df['ORIGIN_POPULATION_ACS']
    df = df[['ORIGIN_FIPS', 'DESTINATION_FIPS', 'AGE_GROUP', 'TOTAL_FLOW', 'AGE_MIGRATION_FRACTION']]

    df = make_fips_changes(df)
    valid_fips = get_euclidean_distance()
    df = df.loc[df.ORIGIN_FIPS.isin(valid_fips.ORIGIN_FIPS)]
    df = df.loc[df.DESTINATION_FIPS.isin(valid_fips.ORIGIN_FIPS)]

    con = sqlite3.connect(ACS_DB)
    df.to_sql(name='acs_gross_migration_weights_2011_2015_age',
              con=con,
              if_exists='replace',
              index=False)
    con.close()

def main():
    get_gross_migration_ratios_by_age()

if __name__ == '__main__':
    main()
