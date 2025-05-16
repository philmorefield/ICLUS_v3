import os
import sqlite3

import pandas as pd

# pd.set_option("display.max_columns", None) # show all cols

BASE_FOLDER = 'D:\\projects\\ICLUS_v3\\population'
if os.path.isdir('D:\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'

DATABASE_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\databases')
MIGRATION_DB = os.path.join(DATABASE_FOLDER, 'migration.sqlite')
POPULATION_DB = os.path.join(DATABASE_FOLDER, 'population.sqlite')
ANALYSIS_DB = os.path.join(DATABASE_FOLDER, 'analysis.sqlite')
ACS_DB = os.path.join(DATABASE_FOLDER, 'acs.sqlite')
CENSUS_CSV_PATH = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\Census\\2019')

ACS_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\ACS\\2011_2015')

years = ('2006-2010', '2011-2015')

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


def get_gross_migration_ratios_by_race():
    xl_filename = 'county-to-county-by-race-2011-2015-current-residence-sort.xlsx'

    columns = ('D_STFIPS', 'D_COFIPS', 'O_STFIPS', 'O_COFIPS', 'RACE', 'D_STATE', 'D_COUNTY', 'D_POP',
                'D_POP_MOE', 'D_NONMOVERS', 'D_NONMOVERS_MOE', 'D_MOVERS',
                'D_MOVERS_MOE', 'D_MOVERS_SAME_CY', 'D_MOVERS_SAME_CY_MOE',
                'D_MOVERS_FROM_DIFF_CY_SAME_ST',
                'D_MOVERS_FROM_DIFF_CY_SAME_ST_MOE', 'D_MOVERS_FROM_DIFF_ST',
                'D_MOVERS_DIFF_ST_MOE', 'D_MOVERS_FROM_ABROAD',
                'D_MOVERS_FROM_ABROAD_MOE', 'O_STATE', 'O_COUNTY', 'O_POP',
                'O_POP_MOE', 'O_NONMOVERS', 'O_NOMMOVERS_MOE', 'O_MOVERS',
                'O_MOVERS_MOE', 'O_MOVERS_SAME_CY', 'O_MOVERS_SAME_CY_MOE',
                'O_MOVERS_FROM_DIFF_CY_SAME_ST',
                'O_MOVERS_FROM_DIFF_CY_SAME_ST_MOE', 'O_MOVERS_FROM_DIFF_ST',
                'O_MOVERS_DIFF_ST_MOE', 'O_MOVERS_PUERTO_RICO',
                'O_MOVERS_PUERTO_RICO_MOE', 'TOTAL_FLOW', 'TOTAL_FLOW_MOE')

    xls = pd.ExcelFile(os.path.join(ACS_FOLDER, 'migration', xl_filename))
    df = pd.concat([xls.parse(sheet_name=name, header=None, names=columns, skiprows=4, skipfooter=8) for name in xls.sheet_names if name != 'Puerto Rico'])

    df = df[~df.O_STFIPS.str.contains('XXX')]
    foreign = ('EUR', 'ASI', 'SAM', 'ISL', 'NAM', 'CAM', 'CAR', 'AFR', 'OCE')
    df = df.loc[~df.O_STFIPS.isin(foreign), ['D_STFIPS', 'D_COFIPS', 'O_STFIPS', 'O_COFIPS', 'RACE', 'TOTAL_FLOW']]

    df['D_STFIPS'] = df.D_STFIPS.astype(int).astype(str).str.zfill(2)
    df['D_COFIPS'] = df.D_COFIPS.astype(int).astype(str).str.zfill(3)
    df['DESTINATION_FIPS'] = df.D_STFIPS + df.D_COFIPS

    df['O_STFIPS'] = df.O_STFIPS.astype(int).astype(str).str.zfill(2)
    df['O_COFIPS'] = df.O_COFIPS.astype(int).astype(str).str.zfill(3)
    df['ORIGIN_FIPS'] = df.O_STFIPS + df.O_COFIPS

    df['RACE'] = df.RACE.replace(to_replace=ACS_RACE_MAP)
    df = df[['DESTINATION_FIPS', 'ORIGIN_FIPS', 'RACE', 'TOTAL_FLOW']]

    df = df.sort_values(by=['ORIGIN_FIPS', 'RACE', 'DESTINATION_FIPS'])
    assert not df.isnull().any().any()
    df = make_fips_changes(df)

    valid_fips = get_euclidean_distance()
    df = df.loc[df.ORIGIN_FIPS.isin(valid_fips.ORIGIN_FIPS)]
    df = df.loc[df.DESTINATION_FIPS.isin(valid_fips.ORIGIN_FIPS)]

    # use 'OTHER' migration information for 'AIAN', 'NHPI', and 'TWO_OR_MORE'
    for population_race in ['AIAN', 'NHPI', 'TWO_OR_MORE']:
        temp = df.loc[df.RACE == 'OTHER'].copy()
        temp['RACE'] = population_race
        df = pd.concat([df, temp], ignore_index=True)
    df = df.loc[df.RACE != 'OTHER']

    origin_race = get_acs_2010_2014_county_population_by_race()

    df = df.merge(right=origin_race,
                  left_on=['ORIGIN_FIPS', 'RACE'],
                  right_on=['COFIPS', 'RACE'],
                  how='left')
    df = df.drop(columns=['COFIPS'])
    df.rename(columns={'POPULATION': 'ORIGIN_POPULATION'}, inplace=True)
    df.loc[df.RACE.isin(['AIAN', 'NHPI', 'TWO_OR_MORE']), 'TOTAL_FLOW'] = (df.ORIGIN_POPULATION / df.SUM_OTHER) * df.TOTAL_FLOW

    df = df.drop(columns=['ORIGIN_POPULATION', 'SUM_OTHER'])
    df = df.groupby(['DESTINATION_FIPS', 'RACE'], as_index=False).sum()
    df['RACE_SUM'] = df.groupby('RACE')['TOTAL_FLOW'].transform(sum)
    df['WEIGHT_x_10^6'] = (df['TOTAL_FLOW'] / df['RACE_SUM']) * 1000000
    df = df.pivot(index='DESTINATION_FIPS', columns='RACE', values='WEIGHT_x_10^6')
    df.reset_index(inplace=True)
    df.columns.name = None
    df.fillna(value=0, inplace=True)
    df = df[['DESTINATION_FIPS'] + list(RACE_MAP.values())]
    dfs.append(df)

    df1, df2 = dfs
    new1 = df2[~df2.DESTINATION_FIPS.isin(df1.DESTINATION_FIPS)]
    new1.loc[:, list(RACE_MAP.values())] = 0
    df1 = df1.append(other=new1, ignore_index=True, verify_integrity=True, sort=True)
    df1 = df1[['DESTINATION_FIPS'] + list(RACE_MAP.values())]
    df1.sort_values(by='DESTINATION_FIPS', inplace=True)

    new2 = df1[~df1.DESTINATION_FIPS.isin(df2.DESTINATION_FIPS)]
    new2.loc[:, list(RACE_MAP.values())] = 0
    df2 = df2.append(other=new2, ignore_index=True, verify_integrity=True, sort=True)
    df2 = df2[['DESTINATION_FIPS'] + list(RACE_MAP.values())]
    df2.sort_values(by='DESTINATION_FIPS', inplace=True)

    df1.set_index(keys='DESTINATION_FIPS', inplace=True)
    df2.set_index(keys='DESTINATION_FIPS', inplace=True)

    df = (df1 + df2) / 2

    df.reset_index(inplace=True)

    p = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases'
    f = 'acs.sqlite'
    con = sqlite3.connect(os.path.join(p, f))
    df.to_sql(name='acs_gross_migration_weights_2011_2015',
              con=con,
              if_exists='replace',
              index=False)
    con.close()

def main():
     df_race = get_gross_migration_ratios_by_race()

if __name__ == '__main__':
    main()
