'''
Revised: 2025-03-27


'''
import glob
import os
import sqlite3

from itertools import product

import pandas as pd

na_values = ['Suppressed', 'Not Applicable', 'None', 'Missing', 'Not Available', 'Unreliable']


RACE_MAP = {'2106-3': 'WHITE',
            '2054-5': 'BLACK',
            '1002-5': 'AIAN',
            'A': 'ASIAN',
            'NHOPI': 'NHPI',
            'M': 'TWO_OR_MORE'}

AGE_GROUP_SORT_MAP = {'0-4': 0,
                      '5-9': 1,
                      '10-14': 2,
                      '15-19': 3,
                      '20-24': 4,
                      '25-29': 5,
                      '30-34': 6,
                      '35-39': 7,
                      '40-44': 8,
                      '45-49': 9,
                      '50-54': 10,
                      '55-59': 11,
                      '60-64': 12,
                      '65-69': 13,
                      '70-74': 14,
                      '75-79': 15,
                      '80-84': 16,
                      '85+': 17}

if os.path.exists('D:\\OneDrive\\ICLUS_v3'):
    ICLUS_FOLDER = 'D:\\OneDrive\\ICLUS_v3'
else:
    ICLUS_FOLDER = 'D:\\projects\\ICLUS_v3'

CSV_FILES = os.path.join(ICLUS_FOLDER, 'population\\inputs\\raw_files\\CDC')
DATABASE_FOLDER = os.path.join(ICLUS_FOLDER, 'population\\inputs\\databases')
MIGRATION_DB = os.path.join(DATABASE_FOLDER, 'migration.sqlite')


def get_cofips_and_state():
    query = 'SELECT COFIPS, STUSPS AS STABBR \
             FROM fips_to_urb20_bea10_hhs'
    con = sqlite3.connect(MIGRATION_DB)
    df = pd.read_sql_query(sql=query, con=con)
    con.close()

    return df


def create_template():
    cofips_all = get_cofips_and_state()

    ages = ['1',
            '1-4',
            '5-9',
            '10-14',
            '15-19',
            '20-24',
            '25-29',
            '30-34',
            '35-39',
            '40-44',
            '45-49',
            '50-54',
            '55-59',
            '60-64',
            '65-69',
            '70-74',
            '75-79',
            '80-84',
            '85+']

    races = list(RACE_MAP.values())
    cofips = list(cofips_all.COFIPS.values)
    genders = ['MALE', 'FEMALE']

    df = pd.DataFrame(list(product(cofips, races, ages, genders)),
                      columns=['COFIPS', 'RACE', 'AGE_GROUP', 'SEX'])

    return df


def apply_county_level_mortality(df):

    dataframes = []

    # first apply county level mortality; not all cohorts will have values
    for csv in glob.glob(os.path.join(CSV_FILES, 'Underlying*.txt')):
        temp = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
        if '85+' in os.path.basename(csv):
            continue
        if 'STATE' in os.path.basename(csv):
            continue
        if 'HHS' in os.path.basename(csv):
            continue
        if 'NATIONAL' in os.path.basename(csv):
            continue
        temp['RACE'] = temp['Single Race 6 Code'].map(RACE_MAP)
        temp = temp[['County Code', 'RACE', 'Sex', 'Five-Year Age Groups Code', 'Crude Rate']]
        temp.columns = ['COFIPS', 'RACE', 'SEX', 'AGE_GROUP', 'MORTALITY']
        temp.dropna(how='any', inplace=True)
        temp['COFIPS'] = temp['COFIPS'].astype(int).astype(str).str.zfill(5)

        dataframes.append(temp)

    mort = pd.concat(objs=dataframes, ignore_index=True)
    mort['SEX'] = mort['SEX'].str.upper()

    df = df.merge(right=mort, how='left', on=['COFIPS', 'RACE', 'AGE_GROUP', 'SEX'])

    return df

def apply_state_level_mortality(df):
    fn = 'Underlying Cause of Death, 2018-2022, STATE.txt'
    csv = os.path.join(CSV_FILES, fn)
    mort = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
    mort = mort[['State Code', 'Single Race 6 Code', 'Sex', 'Five-Year Age Groups Code', 'Crude Rate']]
    mort.columns = ['STFIPS', 'RACE', 'SEX', 'AGE_GROUP', 'STATE_MORTALITY']
    mort.dropna(how='any', inplace=True)

    # add 85+ age group to the dataframe
    csv = os.path.join(CSV_FILES, 'Underlying Cause of Death, 2018-2022, 85+, STATE.txt')
    temp = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
    temp['Five-Year Age Groups Code'] = '85+'
    temp = temp[['State Code', 'Single Race 6 Code', 'Sex', 'Five-Year Age Groups Code', 'Crude Rate']]
    temp.columns = ['STFIPS', 'RACE', 'SEX', 'AGE_GROUP', 'STATE_MORTALITY']
    temp.dropna(how='any', inplace=True)

    mort = pd.concat(objs=[mort, temp], ignore_index=True)
    mort['STFIPS'] = mort['STFIPS'].astype(int).astype(str).str.zfill(2)
    mort['RACE'] = mort['RACE'].map(RACE_MAP)
    mort['SEX'] = mort['SEX'].str.upper()

    df['STFIPS'] = df['COFIPS'].str[:2]
    df = df.merge(right=mort, how='left', on=['STFIPS', 'RACE', 'SEX', 'AGE_GROUP'])
    df.loc[df.MORTALITY.isnull(), 'MORTALITY'] = df['STATE_MORTALITY']

    df = df.drop(columns=['STFIPS', 'STATE_MORTALITY'])

    return df


def apply_hhs_level_mortality(df):
    query = 'SELECT COFIPS, HHS AS HHS_REGION \
             FROM cofips_state_msa20_bea10_hhs'
    con = sqlite3.connect(MIGRATION_DB)
    hhs = pd.read_sql_query(sql=query, con=con)
    con.close()

    fn = 'Underlying Cause of Death, 2018-2022, HHS.txt'
    csv = os.path.join(CSV_FILES, fn)
    mort = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
    mort = mort[['HHS Region Code', 'Single Race 6 Code', 'Sex', 'Five-Year Age Groups Code', 'Crude Rate']]
    mort.columns = ['HHS_REGION', 'RACE', 'SEX', 'AGE_GROUP', 'HHS_MORTALITY']
    mort.dropna(how='any', inplace=True)

    # add 85+ age group to the dataframe
    csv = os.path.join(CSV_FILES, 'Underlying Cause of Death, 2018-2022, 85+, HHS.txt')
    temp = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
    temp['Five-Year Age Groups Code'] = '85+'
    temp = temp[['HHS Region Code', 'Single Race 6 Code', 'Sex', 'Five-Year Age Groups Code', 'Crude Rate']]
    temp.columns = ['HHS_REGION', 'RACE', 'SEX', 'AGE_GROUP', 'HHS_MORTALITY']
    temp.dropna(how='any', inplace=True)

    mort = pd.concat(objs=[mort, temp], ignore_index=True)
    mort['HHS_REGION'] = mort['HHS_REGION'].str.replace('HHS', '').astype(int)
    mort['RACE'] = mort['RACE'].map(RACE_MAP)
    mort['SEX'] = mort['SEX'].str.upper()

    # identify the HHS region for each county
    df = df.merge(right=hhs, how='left', on='COFIPS')

    # join HHS-level mortality rates
    df['HHS_REGION'] = df['HHS_REGION'].astype(int)
    df = df.merge(right=mort, how='left', on=['HHS_REGION', 'RACE', 'SEX', 'AGE_GROUP'])
    df.loc[df.MORTALITY.isnull(), 'MORTALITY'] = df['HHS_MORTALITY']

    df = df.drop(columns=['HHS_REGION', 'HHS_MORTALITY'])

    return df


def apply_national_mortality(df):
    fn = 'Underlying Cause of Death, 2018-2022, NATIONAL.txt'
    csv = os.path.join(CSV_FILES, fn)
    mort = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
    mort = mort[['Single Race 6 Code', 'Sex', 'Five-Year Age Groups Code', 'Crude Rate']]
    mort.columns = ['RACE', 'SEX', 'AGE_GROUP', 'NATIONAL_MORTALITY']
    mort.dropna(how='any', inplace=True)

    # add 85+ age group to the dataframe
    csv = os.path.join(CSV_FILES, 'Underlying Cause of Death, 2018-2022, 85+, NATIONAL.txt')
    temp = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
    temp['Five-Year Age Groups Code'] = '85+'
    temp = temp[['Single Race 6 Code', 'Sex', 'Five-Year Age Groups Code', 'Crude Rate']]
    temp.columns = ['RACE', 'SEX', 'AGE_GROUP', 'NATIONAL_MORTALITY']
    temp.dropna(how='any', inplace=True)

    mort = pd.concat(objs=[mort, temp], ignore_index=True)
    mort['RACE'] = mort['RACE'].map(RACE_MAP)
    mort['SEX'] = mort['SEX'].str.upper()

    # join national mortality rates
    df = df.merge(right=mort, how='left', on=['RACE', 'SEX', 'AGE_GROUP'])
    df.loc[df.MORTALITY.isnull(), 'MORTALITY'] = df['NATIONAL_MORTALITY']

    df = df.drop(columns=['NATIONAL_MORTALITY'])

    # remarkably, the combination of NHPI/FEMALE/5-9 is 'Unrealiable' at the
    # national level, with only 19 deaths in the numberator (20 is the
    # threshold). The denominator is 159,000, so the rate is 11.9.
    if df.loc[(df.RACE == 'NHPI') & (df.AGE_GROUP == '5-9') & (df.SEX == 'FEMALE'), 'MORTALITY'].isnull().all():
        df.loc[(df.RACE == 'NHPI') & (df.AGE_GROUP == '5-9') & (df.SEX == 'FEMALE'), 'MORTALITY'] = 11.9

    assert not df.isnull().any().any()

    return df

def combine_under_5_age_groups(df):
    # combine <1 and 1-4 age groups using a weighted average
    weight_map = {'1': 0.2,
                  '1-4': 0.8}

    young = df.copy().query('AGE_GROUP == "1" | AGE_GROUP == "1-4"')
    young['WEIGHT'] = young['AGE_GROUP'].map(weight_map)
    young['MORT_x_WEIGHT'] = young.eval('MORTALITY * WEIGHT')
    young['NUMERATOR'] = young.groupby(by=['COFIPS', 'RACE', 'SEX'])['MORT_x_WEIGHT'].transform('sum')
    young['DENOMENATOR'] = young.groupby(by=['COFIPS', 'RACE', 'SEX'])['WEIGHT'].transform('sum')
    young['MORTALITY'] = young.eval('NUMERATOR / DENOMENATOR')
    young['AGE_GROUP'] = '0-4'
    young = young.drop(columns=['WEIGHT', 'NUMERATOR', 'DENOMENATOR', 'MORT_x_WEIGHT'])
    young = young.drop_duplicates()

    df = df.query('AGE_GROUP != "1" & AGE_GROUP != "1-4"')
    df = pd.concat(objs=[df, young], ignore_index=True, verify_integrity=True)
    df.MORTALITY = df.MORTALITY.round(0).astype(int)

    return df


def main():
    # create the template Dataframe that hold all county/race/age combinations
    # and start merging information
    df = create_template()
    df = apply_county_level_mortality(df)
    df = apply_state_level_mortality(df)
    df = apply_hhs_level_mortality(df)
    df = apply_national_mortality(df)
    df = combine_under_5_age_groups(df)

    df = df.sort_values(by=['AGE_GROUP', 'COFIPS', 'RACE'], key=lambda x: x.map(AGE_GROUP_SORT_MAP))

    con = sqlite3.connect(os.path.join(DATABASE_FOLDER, 'cdc.sqlite'))
    df.to_sql(name='mortality_2018_2022_county',
              con=con,
              if_exists='replace',
              index=False)
    con.close()

    print("Finished!")


if __name__ == '__main__':
    main()
