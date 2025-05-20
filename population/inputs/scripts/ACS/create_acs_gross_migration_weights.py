import os
import sqlite3

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
CENSUS_CSV_PATH = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\Census\\2020\\decennial')

ACS_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\ACS\\2011_2015')

ACS_RACE_MAP = {1: 'WHITE', 2: 'BLACK', 3: 'ASIAN', 4: 'OTHER'}
CENSUS_RACE_MAP = {'WA': 'WHITE',
                   'BA': 'BLACK',
                   'IA': 'AIAN',
                   'AA': 'ASIAN',
                   'HA': 'NHPI',
                   'TOM': 'TWO_OR_MORE'}

CENSUS_AGE_GROUP_MAP = {1: '0-4',
                        2: '5-9',
                        3: '10-14',
                        4: '15-19',
                        5: '20-24',
                        6: '25-29',
                        7: '30-34',
                        8: '35-39',
                        9: '40-44',
                        10: '45-49',
                        11: '50-54',
                        12: '55-59',
                        13: '60-64',
                        14: '65-69',
                        15: '70-74',
                        16: '75-79',
                        17: '80-84',
                        18: '85+'}

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

def get_acs_2010_2014_county_population_by_race():
    csv_name = 'ACSDT5YSPT2015.B01003-Data.csv'
    df = pd.read_csv(filepath_or_buffer=os.path.join(ACS_FOLDER, 'population', csv_name),
                     skiprows=1,
                     encoding='latin-1')
    df.columns = ['COFIPS', 'CYNAME', 'POPULATION', 'MOE', 'RACE_CODE', 'RACE', 'EMPTY']
    df = df.drop(columns=['CYNAME', 'MOE', 'RACE_CODE', 'EMPTY'])
    df['COFIPS'] = df['COFIPS'].str[-5:]
    df['RACE'] = df['RACE'].replace({'White alone': 'WHITE',
                                     'Black or African American alone': 'BLACK',
                                     'American Indian and Alaska Native alone (300, A01-Z99)': 'AIAN',
                                     'Asian alone (400-499)': 'ASIAN',
                                     'Native Hawaiian and Other Pacific Islander alone (500-599)': 'NHPI',
                                     'Two or more races': 'TWO_OR_MORE'})

    df = make_fips_changes(df)

    df['SUM_OTHER'] = df.loc[df.RACE.isin(['AIAN', 'NHPI', 'TWO_OR_MORE'])].groupby(by=['COFIPS'])['POPULATION'].transform('sum')

    return df


def get_census_2010_2014_county_population_by_race_():
    csv_name = 'cc-est2019-alldata.csv'
    csv = os.path.join(CENSUS_CSV_PATH, csv_name)
    df = pd.read_csv(csv, encoding='latin-1')
    df['COFIPS'] = df['STATE'].astype(str).str.zfill(2) + df['COUNTY'].astype(str).str.zfill(3)
    df = df.query('3 <= YEAR <= 7')
    df = df.query('AGEGRP >= 1')
    df = df.rename(columns={'AGEGRP': 'AGE_GROUP'})
    df['AGE_GROUP'] = df['AGE_GROUP'].replace(to_replace=CENSUS_AGE_GROUP_MAP)

    df['WHITE'] = df['WA_MALE'] + df['WA_FEMALE']
    df['BLACK'] = df['BA_MALE'] + df['BA_FEMALE']
    df['AIAN'] = df['IA_MALE'] + df['IA_FEMALE']
    df['ASIAN'] = df['AA_MALE'] + df['AA_FEMALE']
    df['NHPI'] = df['NA_MALE'] + df['NA_FEMALE']
    df['TWO_OR_MORE'] = df['TOM_MALE'] + df['TOM_FEMALE']

    df = df[['COFIPS', 'WHITE', 'BLACK', 'AIAN', 'ASIAN', 'NHPI', 'TWO_OR_MORE']]
    df = df.melt(id_vars='COFIPS', var_name='RACE', value_name='POPULATION')
    df = make_fips_changes(df)
    df = df.groupby(by=['COFIPS', 'RACE'], as_index=False).mean()
    df['POPULATION'] = (df['POPULATION'] / 5).astype(int)

    df['SUM_OTHER'] = df.loc[df.RACE.isin(['AIAN', 'NHPI', 'TWO_OR_MORE'])].groupby(by=['COFIPS'])['POPULATION'].transform('sum')

    return df


def get_census_2020_county_population_by_race_():
    csv_name = 'DECENNIALPL2020.P1-Data.csv'
    csv = os.path.join(CENSUS_CSV_PATH, csv_name)
    df = pd.read_csv(filepath_or_buffer=csv,
                     skiprows=1,
                     encoding='latin-1').iloc[:, :11]
    df.columns = ['COFIPS', 'CYNAME', 'TOTAL_POPULATION', 'ONE_RACE', 'WHITE', 'BLACK', 'AIAN', 'ASIAN', 'NHPI', 'OTHER', 'TWO_OR_MORE']
    df = df.drop(columns=['CYNAME', 'TOTAL_POPULATION', 'ONE_RACE'])
    df['COFIPS'] = df['COFIPS'].str[-5:]

    df = make_fips_changes(df)
    df = df.groupby(by='COFIPS', as_index=False).sum()

    df = df.melt(id_vars='COFIPS', var_name='RACE', value_name='CENSUS_POPULATION')
    df['CO_POP_NOT_OTHER'] = df.query('RACE != "OTHER"').groupby(by='COFIPS')['CENSUS_POPULATION'].transform('sum')
    df['TOTAL_POP'] = df.groupby(by='COFIPS', as_index=False)['CENSUS_POPULATION'].transform('sum')
    df['PCT_POP_NOT_OTHER'] =  df['CO_POP_NOT_OTHER'] / df['TOTAL_POP']
    df = df[['COFIPS', 'RACE', 'CENSUS_POPULATION', 'PCT_POP_NOT_OTHER']].query('RACE != "OTHER"')
    # df['SUM_OTHER'] = df.loc[df.RACE.isin(['AIAN', 'NHPI', 'TWO_OR_MORE'])].groupby(by=['COFIPS'])['POPULATION'].transform('sum')

    return df


def get_acs_2011_2015_migration():
    xl_filename = 'county-to-county-by-race-2011-2015-current-residence-sort.xlsx'

    columns = ('D_STFIPS', 'D_COFIPS', 'O_STFIPS', 'O_COFIPS', 'RACE',
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
    df = df.loc[~df.O_STFIPS.isin(foreign), ['D_STFIPS', 'D_COFIPS', 'O_STFIPS', 'O_COFIPS', 'RACE', 'ORIGIN_POPULATION_ACS', 'TOTAL_FLOW']]

    df['D_STFIPS'] = df.D_STFIPS.astype(int).astype(str).str.zfill(2)
    df['D_COFIPS'] = df.D_COFIPS.astype(int).astype(str).str.zfill(3)
    df['DESTINATION_FIPS'] = df.D_STFIPS + df.D_COFIPS

    df['O_STFIPS'] = df.O_STFIPS.astype(int).astype(str).str.zfill(2)
    df['O_COFIPS'] = df.O_COFIPS.astype(int).astype(str).str.zfill(3)
    df['ORIGIN_FIPS'] = df.O_STFIPS + df.O_COFIPS

    df['RACE'] = df.RACE.replace(to_replace=ACS_RACE_MAP)
    df = df[['DESTINATION_FIPS', 'ORIGIN_FIPS', 'RACE', 'ORIGIN_POPULATION_ACS', 'TOTAL_FLOW']]

    df = df.sort_values(by=['ORIGIN_FIPS', 'RACE', 'DESTINATION_FIPS'])

    # use 'OTHER' migration information for 'AIAN', 'NHPI', and 'TWO_OR_MORE'
    for population_race in ['AIAN', 'NHPI', 'TWO_OR_MORE']:
        temp = df.loc[df.RACE == 'OTHER'].copy()
        temp['RACE'] = population_race
        df = pd.concat([df, temp], ignore_index=True)
    df = df.loc[df.RACE != 'OTHER']

    return df

def get_gross_migration_ratios_by_race():
    origin_race = get_census_2020_county_population_by_race_()
    migration = get_acs_2011_2015_migration()

    df = migration.merge(right=origin_race,
                         left_on=['ORIGIN_FIPS', 'RACE'],
                         right_on=['COFIPS', 'RACE'],
                         how='left')
    df = df.drop(columns=['COFIPS'])
    # df.loc[df.ORIGIN_POPULATION_ACS.isnull(), 'ORIGIN_POPULATION_ACS'] = df.CENSUS_POPULATION

    df['SUM_CENSUS_A_N_T_POPULATION_ORIGIN'] = df.loc[df.RACE.isin(['AIAN', 'NHPI', 'TWO_OR_MORE'])].groupby(by='ORIGIN_FIPS', as_index=False)['CENSUS_POPULATION'].transform('sum')
    # df.rename(columns={'POPULATION': 'ORIGIN_POPULATION'}, inplace=True)
    df.loc[df.RACE.isin(['AIAN', 'NHPI', 'TWO_OR_MORE']), 'TOTAL_FLOW'] = (df.CENSUS_POPULATION / df.SUM_CENSUS_A_N_T_POPULATION_ORIGIN) * (df.TOTAL_FLOW * df.PCT_POP_NOT_OTHER)
    df['RACE_MIGRATION_FRACTION'] = df['TOTAL_FLOW'] / df['ORIGIN_POPULATION']

    df = df.drop(columns=['ORIGIN_POPULATION', 'SUM_OTHER'])

    df = make_fips_changes(df)
    valid_fips = get_euclidean_distance()
    df = df.loc[df.ORIGIN_FIPS.isin(valid_fips.ORIGIN_FIPS)]
    df = df.loc[df.DESTINATION_FIPS.isin(valid_fips.ORIGIN_FIPS)]


    p = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases'
    f = 'acs.sqlite'
    con = sqlite3.connect(os.path.join(p, f))
    df.to_sql(name='acs_gross_migration_weights_2011_2015',
              con=con,
              if_exists='replace',
              index=False)
    con.close()

def main():
    get_gross_migration_ratios_by_race()

if __name__ == '__main__':
    main()
