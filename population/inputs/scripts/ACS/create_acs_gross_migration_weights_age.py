import os
import sqlite3

from itertools import product

import pandas as pd

pd.set_option("display.max_columns", None) # show all cols

BASE_FOLDER = 'D:\\projects\\ICLUS_v3\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'
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
                     15: '75_AND_OVER'}

CENSUS_AGE_GROUPS = ['0_TO_4', '5_TO_9', '10_TO_14', '15_TO_17', '18_TO_19',
                     '20', '21', '22_TO_24', '25_TO_29',
                     '30_TO_34', '35_TO_39', '40_TO_44', '45_TO_49',
                     '50_TO_54', '55_TO_59', '60_TO_61', '62_TO_64',
                     '65_TO_66', '67_TO_69', '70_TO_74', '75_TO_79',
                     '80_TO_84', '85_AND_OVER']

CENSUS_AGE_GROUP_MAP = {'15_TO_17': '15_TO_19',
                        '18_TO_19': '15_TO_19',
                        '20': '20_TO_24',
                        '21': '20_TO_24',
                        '22_TO_24': '20_TO_24',
                        '60_TO_61': '60_TO_64',
                        '62_TO_64': '60_TO_64',
                        '65_TO_66': '65_TO_69',
                        '67_TO_69': '65_TO_69'}

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
        elif 'ORIGIN_AGE_GROUP' in df.columns:
            df = df.groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'ORIGIN_AGE_GROUP'], as_index=False).sum()
        elif 'MIGRATION_AGE_GROUP' in df.columns:
            df = df.groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'MIGRATION_AGE_GROUP'], as_index=False).sum()
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
        elif 'ORIGIN_AGE_GROUP' in df.columns:
            df = df.groupby(by=['COFIPS', 'ORIGIN_AGE_GROUP'], as_index=False).sum()
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

    df = df.melt(id_vars='COFIPS', var_name='ORIGIN_AGE_GROUP', value_name='ORIGIN_POPULATION')
    df['SEX'] = df['ORIGIN_AGE_GROUP'].str.split(pat='_', n=1).str[0]
    df['ORIGIN_AGE_GROUP'] = df['ORIGIN_AGE_GROUP'].str.split(pat='_', n=1).str[1]

    return df


def get_acs_2011_2015_migration():
    xl_filename = 'county-to-county-by-age-2011-2015-current-residence-sort.xlsx'

    columns = ('D_STFIPS', 'D_COFIPS', 'O_STFIPS', 'O_COFIPS', 'MIGRATION_AGE_GROUP',
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
               'O_MOVERS_PUERTO_RICO_MOE', 'FLOW', 'TOTAL_FLOW_MOE')

    xls = pd.ExcelFile(os.path.join(ACS_FOLDER, 'migration', xl_filename))
    df = pd.concat([xls.parse(sheet_name=name, header=None, names=columns, skiprows=4, skipfooter=8) for name in xls.sheet_names if name != 'Puerto Rico'])

    df = df[~df.O_STFIPS.str.contains('XXX')]
    foreign = ('EUR', 'ASI', 'SAM', 'ISL', 'NAM', 'CAM', 'CAR', 'AFR', 'OCE')
    df = df.loc[~df.O_STFIPS.isin(foreign), ['D_STFIPS', 'D_COFIPS', 'O_STFIPS', 'O_COFIPS', 'MIGRATION_AGE_GROUP', 'ORIGIN_POPULATION_ACS', 'FLOW']]

    df['D_STFIPS'] = df.D_STFIPS.astype(int).astype(str).str.zfill(2)
    df['D_COFIPS'] = df.D_COFIPS.astype(int).astype(str).str.zfill(3)
    df['DESTINATION_FIPS'] = df.D_STFIPS + df.D_COFIPS

    df['O_STFIPS'] = df.O_STFIPS.astype(int).astype(str).str.zfill(2)
    df['O_COFIPS'] = df.O_COFIPS.astype(int).astype(str).str.zfill(3)
    df['ORIGIN_FIPS'] = df.O_STFIPS + df.O_COFIPS

    df['MIGRATION_AGE_GROUP'] = df['MIGRATION_AGE_GROUP'].replace(to_replace=ACS_AGE_GROUP_MAP)
    df = df[['ORIGIN_FIPS', 'DESTINATION_FIPS', 'MIGRATION_AGE_GROUP', 'FLOW']]
    df = make_fips_changes(df)

    df = df.sort_values(by=['ORIGIN_FIPS', 'MIGRATION_AGE_GROUP', 'DESTINATION_FIPS'])

    return df

def disaggregate_5_to_17_age_group(df_orig):
    '''
    Disaggregate the 5_TO_17 age group into 5_TO_9, 10_TO_14, and 15_TO_17.
    '''
    df = df_orig.copy()
    df = df.query('MIGRATION_AGE_GROUP == "5_TO_17"')
    df['FRACTION_5_17'] = (df.groupby(by='COFIPS')['ORIGIN_POPULATION'].transform('sum'))
    df['FRACTION_5_17'] = df['ORIGIN_POPULATION'].div(df['FRACTION_5_17']).mul(df['FLOW'])
    df['MIGRATION_AGE_GROUP'] = df['ORIGIN_AGE_GROUP']
    df['FLOW'] = df['FRACTION_5_17']
    df = df.drop(columns='FRACTION_5_17')

    df_orig = df_orig.loc[~df_orig.MIGRATION_AGE_GROUP.isin(['5_TO_17'])]
    df = pd.concat([df_orig, df], ignore_index=True)

    return df

def disaggregate_75_AND_OVER_age_group(df_orig):
    '''
    Disaggregate the 5_TO_17 age group into 5_TO_9, 10_TO_14, and 15_TO_17.
    '''
    df = df_orig.copy()
    df = df.query('MIGRATION_AGE_GROUP == "75_AND_OVER"')
    df['FRACTION_75_AND_OVER'] = (df.groupby(by='COFIPS')['ORIGIN_POPULATION'].transform('sum'))
    df['FRACTION_75_AND_OVER'] = df['ORIGIN_POPULATION'].div(df['FRACTION_75_AND_OVER']).mul(df['FLOW'])
    df['MIGRATION_AGE_GROUP'] = df['ORIGIN_AGE_GROUP']
    df['FLOW'] = df['FRACTION_75_AND_OVER']
    df = df.drop(columns='FRACTION_75_AND_OVER')

    df_orig = df_orig.loc[~df_orig.MIGRATION_AGE_GROUP.isin(['75_AND_OVER'])]
    df = pd.concat([df_orig, df], ignore_index=True)

    return df

def calculate_flow_percentages(migration, origin):
    '''
    Purpose: create a dataframe that hold the proportion (%) of the origin
    population in each gross migration flow.

    1. MAP the MIGRATION_AGE_GROUP to the ORIGIN_AGE_GROUP
    2. Combine ORIGIN_AGE_GROUP "20", "21", and "22_TO_24" into "20_TO_24"
    3. Combine ORIGIN_AGE_GROUP "60_TO_61" and "62_TO_64" into "60_TO_64"
    4. Combine ORIGIN_AGE_GROUP "65_TO_66" and "67_TO_69" into "65_TO_69"
    5. GROUPBY the ORIGIN_AGE_GROUP and update the ORIGIN_POPULATION
    6. MERGE the migration dataframe with the origin dataframe
    7. Disaggretate the MIGRATION_AGE_GROUP "5_TO_17" into "5_TO_9", "10_TO_14", and "15_TO_17"
    8. Disaggregate the MIGRATION_AGE_GROUP "75_AND_OVER" into "75_TO_79", "80_TO_84", and "85_AND_OVER"

    9. Combine ORIGIN_AGE_GROUP "15_TO_17" and "18_TO_19" into "15_TO_19"
    10. Rename the MIGRATION_AGE_GROUP "1_TO_4" to "0_TO_4"

    '''
    pop_to_migration_map = {'0_TO_4': '1_TO_4',
                            '5_TO_9': '5_TO_17',
                            '10_TO_14': '5_TO_17',
                            '15_TO_17': '5_TO_17',
                            '18_TO_19': '18_TO_19',
                            '20': '20_TO_24',
                            '21': '20_TO_24',
                            '22_TO_24': '20_TO_24',
                            '25_TO_29': '25_TO_29',
                            '30_TO_34': '30_TO_34',
                            '35_TO_39': '35_TO_39',
                            '40_TO_44': '40_TO_44',
                            '45_TO_49': '45_TO_49',
                            '50_TO_54': '50_TO_54',
                            '55_TO_59': '55_TO_59',
                            '60_TO_61': '60_TO_64',
                            '62_TO_64': '60_TO_64',
                            '65_TO_66': '65_TO_69',
                            '67_TO_69': '65_TO_69',
                            '70_TO_74': '70_TO_74',
                            '75_TO_79': '75_AND_OVER',
                            '80_TO_84': '75_AND_OVER',
                            '85_AND_OVER': '75_AND_OVER'}

    # 1. MAP the MIGRATION_AGE_GROUP to the ORIGIN_AGE_GROUP
    origin['MIGRATION_AGE_GROUP'] = origin.ORIGIN_AGE_GROUP.map(pop_to_migration_map)

    # 2. Combine ORIGIN_AGE_GROUP "20", "21", and "22_TO_24" into "20_TO_24"
    origin.loc[origin.ORIGIN_AGE_GROUP.isin(['20', '21', '22_TO_24']), 'ORIGIN_AGE_GROUP'] = '20_TO_24'

    # 3. Combine ORIGIN_AGE_GROUP "60_TO_61" and "62_TO_64" into "60_TO_64"
    origin.loc[origin.ORIGIN_AGE_GROUP.isin(['60_TO_61', '62_TO_64']), 'ORIGIN_AGE_GROUP'] = '60_TO_64'

    # 4. Combine ORIGIN_AGE_GROUP "65_TO_66" and "67_TO_69" into "65_TO_69"
    origin.loc[origin.ORIGIN_AGE_GROUP.isin(['65_TO_66', '67_TO_69']), 'ORIGIN_AGE_GROUP'] = '65_TO_69'

    # 5. GROUPBY the ORIGIN_AGE_GROUP and update the ORIGIN_POPULATION
    origin = origin.drop(columns='SEX')
    origin = origin.groupby(by=['COFIPS', 'ORIGIN_AGE_GROUP', 'MIGRATION_AGE_GROUP'], as_index=False).sum()

    # 6. MERGE
    df = migration.merge(right=origin,
                         left_on=['ORIGIN_FIPS', 'MIGRATION_AGE_GROUP'],
                         right_on=['COFIPS', 'MIGRATION_AGE_GROUP'],
                         how='left')

    # 7. Disaggretate the MIGRATION_AGE_GROUP "5_TO_17" into "5_TO_9", "10_TO_14", and "15_TO_17"
    df = disaggregate_5_to_17_age_group(df)

    # 8. Disaggregate the MIGRATION_AGE_GROUP "75_AND_OVER" into "75_TO_79", "80_TO_84", and "85_AND_OVER"
    df = disaggregate_75_AND_OVER_age_group(df)

    # 9. Combine ORIGIN_AGE_GROUP "15_TO_17" and "18_TO_19" into "15_TO_19"
    df.loc[df.ORIGIN_AGE_GROUP.isin(['15_TO_17', '18_TO_19']), 'ORIGIN_AGE_GROUP'] = '15_TO_19'


    origin['MIGRATION_AGE_GROUP_TOTAL'] = origin.groupby(by=['COFIPS', 'MIGRATION_AGE_GROUP'])['ORIGIN_POPULATION'].transform('sum')
    origin['MIGRATION_AGE_GROUP_FRACTION'] = origin['ORIGIN_POPULATION'] / origin['MIGRATION_AGE_GROUP_TOTAL']

    df['TOTAL_FLOW_ADJ'] = df['FLOW'] * df['MIGRATION_AGE_GROUP_FRACTION']

    # # 6. Rename the MIGRATION_AGE_GROUP "1_TO_4" to "0_TO_4"
    # origin.loc[origin.MIGRATION_AGE_GROUP == '1_TO_4', 'MIGRATION_AGE_GROUP'] = '0_TO_4'

    return df


def get_gross_migration_ratios_by_age():
    origin = get_census_2020_county_population_by_age_()
    migration = get_acs_2011_2015_migration()

    df = calculate_flow_percentages(migration, origin)



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
