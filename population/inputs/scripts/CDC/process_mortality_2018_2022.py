import glob
import os
import sqlite3
import warnings

from itertools import product

import pandas as pd

na_values = ['Suppressed', 'Not Applicable', 'None', 'Missing']


RACE_MAP = {'2106-3': 'WHITE',
            '2054-5': 'BLACK',
            '1002-5': 'AIAN',
            'A': 'ASIAN',
            'NHOPI': 'NHPI',
            'M': 'TWO_OR_MORE'}

DISSERTATION_FOLDER = 'D:\\OneDrive\\Dissertation'
DATABASE_FOLDER = os.path.join(DISSERTATION_FOLDER, 'databases')
MIGRATION_DB = os.path.join(DATABASE_FOLDER, 'migration.sqlite')
CDC_INPUTS = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_3', 'inputs', 'cdc')
CDC_DATA = os.path.join(DISSERTATION_FOLDER, 'data', 'CDC', '2018_2022')


def get_fips_changes():
    query = 'SELECT OLD_FIPS, NEW_FIPS \
              FROM fips_or_name_changes'
    con = sqlite3.connect(MIGRATION_DB)
    df = pd.read_sql_query(sql=query, con=con)
    con.close()

    return df


def get_co2hhs():
    query = 'SELECT GEOID AS COFIPS, HHS_REGION \
              FROM fips_to_state \
              WHERE HHS_REGION != 0'
    con = sqlite3.connect(MIGRATION_DB)
    df = pd.read_sql(sql=query, con=con)
    con.close()

    return df


def get_85_plus():
    # the 'main' file which has WHITE, BLACK, AIAN, and ASIAN
    f = 'Underlying Cause of Death, 2018-2022, 85-100.txt'
    csv = os.path.join(CDC_DATA, f)
    df = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
    df = df[['State Code', 'Single Race 6 Code', 'Gender', 'Deaths', 'Population', 'Crude Rate']]
    df.columns = ['STFIPS', 'RACE', 'GENDER', 'DEATHS', 'MPOP', 'MORTALITY']
    df.query('~STFIPS.isnull() & ~DEATHS.isnull() & ~MPOP.isnull()', inplace=True)
    df['GENDER'] = df['GENDER'].str.upper()
    df['RACE'] = df['RACE'].map(RACE_MAP)
    df['STFIPS'] = df['STFIPS'].astype(int).astype(str).str.zfill(2)
    df['DEATHS'] = df['DEATHS'].astype(int)
    df['MPOP'] = df['MPOP'].astype(int)
    df['MORTALITY'] = df.eval('(DEATHS / MPOP) * 100000').round(0).astype(int)
    df = df[['STFIPS', 'RACE', 'GENDER', 'DEATHS', 'MPOP', 'MORTALITY']]

    co_mort = create_template()
    co_mort.drop(columns='AGE_GROUP', inplace=True)
    co_mort.drop_duplicates(inplace=True)
    co_mort['STFIPS'] = co_mort['COFIPS'].str[:2]

    df = co_mort.merge(right=df, how='left', on=['STFIPS', 'RACE', 'GENDER'])

    df['AGE_GROUP'] = '85+'

    # fill in missing values; 'impute' uses urban/rural difference; substitute
    # does not
    df = impute_county_values_by_economic_area(df)
    assert df.shape[0] == 37332
    df = impute_county_values_by_state(df)
    assert df.shape[0] == 37332
    df = impute_county_values_by_hhs_region(df)
    assert df.shape[0] == 37332
    df = impute_county_values_nationally(df)
    assert df.shape[0] == 37332
    df = substitute_state_averages(df)
    assert df.shape[0] == 37332
    df = substitute_hhs_averages(df)
    assert df.shape[0] == 37332
    df = substitute_national_averages(df)

    assert ~df.MORTALITY.isnull().any()
    assert df.shape[0] == 37332

    return df[['COFIPS', 'RACE', 'AGE_GROUP', 'GENDER', 'MORTALITY']]


def create_template():
    urban_rural = get_urban_counties()

    ages = ['<1',
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
            '80-84']
    races = list(set(RACE_MAP.values()))
    cofips = list(urban_rural.COFIPS.values)
    genders = ['MALE', 'FEMALE']

    df = pd.DataFrame(list(product(cofips, races, ages, genders)),
                      columns=['COFIPS', 'RACE', 'AGE_GROUP', 'GENDER'])

    df = df.merge(right=urban_rural, how='left', on='COFIPS')

    return df


def get_urban_counties():
    db = os.path.join(DATABASE_FOLDER, 'migration.sqlite')
    query = 'SELECT GEOID AS COFIPS, UA10 AS URBAN_DESTINATION \
             FROM ua10_counties'
    con = sqlite3.connect(db)
    df = pd.read_sql_query(sql=query, con=con)
    con.close()

    return df


def get_county_to_bea_df():
    db = os.path.join(DATABASE_FOLDER, 'migration.sqlite')
    query = 'SELECT * FROM county_to_BEA10'
    con = sqlite3.connect(db)
    df = pd.read_sql_query(sql=query, con=con)
    con.close()

    return df


def get_state_level_df():
    # process races first
    f = 'Underlying Cause of Death, 2018-2022, STATE.txt'
    csv = os.path.join(CDC_DATA, f)
    df = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
    df = df[['State Code', 'Single Race 6 Code', 'Gender', 'Five-Year Age Groups', 'Deaths', 'Population', 'Crude Rate']]
    df.columns = ['STFIPS', 'RACE', 'GENDER', 'AGE_GROUP', 'DEATHS', 'MPOP', 'STATE_MORTALITY']
    df.query('~DEATHS.isnull() & ~MPOP.isnull()', inplace=True)
    # df.dropna(how='any', inplace=True)
    df['RACE'] = df['RACE'].map(RACE_MAP)
    df['STFIPS'] = df['STFIPS'].astype(int).astype(str).str.zfill(2)
    df['DEATHS'] = df['DEATHS'].astype(int)
    df['MPOP'] = df['MPOP'].astype(int)
    df['STATE_MORTALITY'] = df.eval('((DEATHS / MPOP) * 100000)').round(0).astype(int)
    df['GENDER'] = df['GENDER'].str.upper()
    df['DEATHS'] = df['DEATHS'].astype(int)
    df['AGE_GROUP'] = df['AGE_GROUP'].str.replace('years', '').str.replace('year', '').str.replace(' ', '')

    assert ~df.isnull().any().any()

    return df[['STFIPS', 'RACE', 'AGE_GROUP', 'GENDER', 'STATE_MORTALITY']]


def get_hhs_level_df():
    f = 'Underlying Cause of Death, 2018-2022, HHS.txt'
    csv = os.path.join(CDC_DATA, f)
    df = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
    df = df[['HHS Region Code', 'Single Race 6 Code', 'Gender', 'Five-Year Age Groups', 'Deaths', 'Population', 'Crude Rate']]
    df.columns = ['HHS_REGION', 'RACE', 'GENDER', 'AGE_GROUP', 'DEATHS', 'MPOP', 'HHS_MORTALITY']
    df.query('~DEATHS.isnull() & ~MPOP.isnull()', inplace=True)
    # df.dropna(how='any', inplace=True)
    df['RACE'] = df['RACE'].map(RACE_MAP)
    df['HHS_REGION'] = df['HHS_REGION'].str.replace('HHS', '').astype(int)
    df['DEATHS'] = df['DEATHS'].astype(int)
    df['MPOP'] = df['MPOP'].astype(int)
    df['HHS_MORTALITY'] = df.eval('((DEATHS / MPOP) * 100000)').round(0).astype(int)
    df['GENDER'] = df['GENDER'].str.upper()
    df['DEATHS'] = df['DEATHS'].astype(int)
    df['AGE_GROUP'] = df['AGE_GROUP'].str.replace('years', '').str.replace('year', '').str.replace(' ', '')

    return df[['HHS_REGION', 'RACE', 'GENDER', 'AGE_GROUP', 'HHS_MORTALITY']]


def get_national_level_df():
    f = 'Underlying Cause of Death, 2018-2022, NATIONAL.txt'
    csv = os.path.join(CDC_DATA, f)
    df = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
    df = df[['Single Race 6 Code', 'Gender', 'Five-Year Age Groups', 'Deaths', 'Population', 'Crude Rate']]
    df.columns = ['RACE', 'GENDER', 'AGE_GROUP', 'DEATHS', 'MPOP', 'HHS_MORTALITY']
    df.query('~DEATHS.isnull() & ~MPOP.isnull()', inplace=True)
    # df.dropna(how='any', inplace=True)
    df['COUNTRY'] = 'USA'
    df['RACE'] = df['RACE'].map(RACE_MAP)
    df['DEATHS'] = df['DEATHS'].astype(int)
    df['MPOP'] = df['MPOP'].astype(int)
    df['USA_MORTALITY'] = df.eval('((DEATHS / MPOP) * 100000)').round(0).astype(int)
    df['GENDER'] = df['GENDER'].str.upper()
    df['DEATHS'] = df['DEATHS'].astype(int)
    df['AGE_GROUP'] = df['AGE_GROUP'].str.replace('years', '').str.replace('year', '').str.replace(' ', '')

    return df[['COUNTRY', 'RACE', 'GENDER', 'AGE_GROUP', 'USA_MORTALITY']]


def impute_county_values_by_economic_area(df):
    # impute values within BEA economic areas using a population-weighted average
    start_nulls = df.query('MORTALITY.isnull()').shape[0]

    # a table for mapping counties to BEA economic areas
    cy2bea = get_county_to_bea_df()

    df = df.merge(right=cy2bea, how='left', on='COFIPS')
    df.loc[(~df.MPOP.isnull()) & (~df.MORTALITY.isnull()), 'MPOP_x_MORT'] = df.MPOP * df.MORTALITY
    df.loc[~df.MPOP_x_MORT.isnull(), 'NUMERATOR'] = df.groupby(by=['BEA10', 'RACE', 'AGE_GROUP', 'GENDER', 'URBAN_DESTINATION'])['MPOP_x_MORT'].transform('sum')
    df.loc[~df.MPOP_x_MORT.isnull(), 'DENOMENATOR'] = df.groupby(by=['BEA10', 'RACE', 'AGE_GROUP', 'GENDER', 'URBAN_DESTINATION'])['MPOP'].transform('sum')
    df.eval('_BEA_MORT_WAVG = NUMERATOR / DENOMENATOR', inplace=True)
    df['BEA_MORT_WAVG'] = df.groupby(by=['BEA10', 'RACE', 'AGE_GROUP', 'GENDER', 'URBAN_DESTINATION'])['_BEA_MORT_WAVG'].transform('max')
    df.loc[df.MORTALITY.isnull(), 'MORTALITY'] = df['BEA_MORT_WAVG']
    df.drop(columns=['BEA10', 'BEA_MORT_WAVG', '_BEA_MORT_WAVG'], inplace=True)

    end_nulls = df.query('MORTALITY.isnull()').shape[0]

    if start_nulls == end_nulls:
        warnings.warn("The function 'impute_county_values_by_economic_area' did not reduce the number of null values")
    elif end_nulls > start_nulls:
        raise Exception

    return df


def impute_county_values_by_state(df):
    # impute values within states using a population-weighted average
    start_nulls = df.query('MORTALITY.isnull()').shape[0]

    df['STFIPS'] = df['COFIPS'].str[:2]
    df.loc[(~df.MPOP.isnull()) & (~df.MORTALITY.isnull()), 'MPOP_x_MORT'] = df.MPOP * df.MORTALITY
    df.loc[~df.MPOP_x_MORT.isnull(), 'NUMERATOR'] = df.groupby(by=['STFIPS', 'RACE', 'AGE_GROUP', 'GENDER', 'URBAN_DESTINATION'])['MPOP_x_MORT'].transform('sum')
    df.loc[~df.MPOP_x_MORT.isnull(), 'DENOMENATOR'] = df.groupby(by=['STFIPS', 'RACE', 'AGE_GROUP', 'GENDER', 'URBAN_DESTINATION'])['MPOP'].transform('sum')
    df.eval('_STATE_MORT_WAVG = NUMERATOR / DENOMENATOR', inplace=True)
    df['STATE_MORT_WAVG'] = df.groupby(by=['STFIPS', 'RACE', 'AGE_GROUP', 'GENDER', 'URBAN_DESTINATION'])['_STATE_MORT_WAVG'].transform('max')
    df.loc[df.MORTALITY.isnull(), 'MORTALITY'] = df['STATE_MORT_WAVG']
    df.drop(columns=['STFIPS', 'STATE_MORT_WAVG', '_STATE_MORT_WAVG'], inplace=True)

    end_nulls = df.query('MORTALITY.isnull()').shape[0]

    if start_nulls == end_nulls:
        warnings.warn("The function 'impute_county_values_by_state' did not reduce the number of null values")
    elif end_nulls > start_nulls:
        raise Exception

    return df


def impute_county_values_by_hhs_region(df):
    # impute values within states using a population-weighted average
    start_nulls = df.query('MORTALITY.isnull()').shape[0]

    # a table for mapping states and counties to HHS regions
    co2hhs = get_co2hhs()

    df = df.merge(right=co2hhs, how='left', on='COFIPS')
    df.loc[(~df.MPOP.isnull()) & (~df.MORTALITY.isnull()), 'MPOP_x_MORT'] = df.MPOP * df.MORTALITY
    df.loc[~df.MPOP_x_MORT.isnull(), 'NUMERATOR'] = df.groupby(by=['HHS_REGION', 'RACE', 'AGE_GROUP', 'GENDER', 'URBAN_DESTINATION'])['MPOP_x_MORT'].transform('sum')
    df.loc[~df.MPOP_x_MORT.isnull(), 'DENOMENATOR'] = df.groupby(by=['HHS_REGION', 'RACE', 'AGE_GROUP', 'GENDER', 'URBAN_DESTINATION'])['MPOP'].transform('sum')
    df.eval('_HHS_MORT_WAVG = NUMERATOR / DENOMENATOR', inplace=True)
    df['HHS_MORT_WAVG'] = df.groupby(by=['HHS_REGION', 'RACE', 'AGE_GROUP', 'GENDER', 'URBAN_DESTINATION'])['_HHS_MORT_WAVG'].transform('max')
    df.loc[df.MORTALITY.isnull(), 'MORTALITY'] = df['HHS_MORT_WAVG']
    df.drop(columns=['HHS_REGION', 'HHS_MORT_WAVG', '_HHS_MORT_WAVG'], inplace=True)

    end_nulls = df.query('MORTALITY.isnull()').shape[0]

    if start_nulls == end_nulls:
        warnings.warn("The function 'impute_county_values_by_hhs_region' did not reduce the number of null values")
    elif end_nulls > start_nulls:
        raise Exception

    return df


def impute_county_values_nationally(df):
    # impute values nationally using a population-weighted average
    start_nulls = df.query('MORTALITY.isnull()').shape[0]

    df['UNIT'] = 1
    df.loc[(~df.MPOP.isnull()) & (~df.MORTALITY.isnull()), 'MPOP_x_MORT'] = df.MPOP * df.MORTALITY
    df.loc[~df.MPOP_x_MORT.isnull(), 'NUMERATOR'] = df.groupby(by=['UNIT', 'RACE', 'AGE_GROUP', 'GENDER', 'URBAN_DESTINATION'])['MPOP_x_MORT'].transform('sum')
    df.loc[~df.MPOP_x_MORT.isnull(), 'DENOMENATOR'] = df.groupby(by=['UNIT', 'RACE', 'AGE_GROUP', 'GENDER', 'URBAN_DESTINATION'])['MPOP'].transform('sum')
    df.eval('_USA_MORT_WAVG = NUMERATOR / DENOMENATOR', inplace=True)
    df['USA_MORT_WAVG'] = df.groupby(by=['UNIT', 'RACE', 'AGE_GROUP', 'GENDER', 'URBAN_DESTINATION'])['_USA_MORT_WAVG'].transform('max')
    df.loc[df.MORTALITY.isnull(), 'MORTALITY'] = df['USA_MORT_WAVG']
    df.drop(columns=['UNIT', 'USA_MORT_WAVG', '_USA_MORT_WAVG'], inplace=True)

    end_nulls = df.query('MORTALITY.isnull()').shape[0]

    if start_nulls == end_nulls:
        warnings.warn("The function 'impute_county_values_nationally' did not reduce the number of null values")
    elif end_nulls > start_nulls:
        raise Exception

    return df


def substitute_state_averages(df):
    # use state averages wherever county values are null
    start_nulls = df.query('MORTALITY.isnull()').shape[0]
    stmort = get_state_level_df()

    stmort = stmort[['STFIPS', 'RACE', 'GENDER', 'AGE_GROUP', 'STATE_MORTALITY']]
    df['STFIPS'] = df['COFIPS'].str[:2]
    df = df.merge(right=stmort, how='left', on=['STFIPS', 'RACE', 'AGE_GROUP', 'GENDER'])
    df.loc[df.MORTALITY.isnull(), 'MORTALITY'] = df['STATE_MORTALITY']
    df.drop(columns=['STFIPS', 'STATE_MORTALITY'], inplace=True)

    end_nulls = df.query('MORTALITY.isnull()').shape[0]

    if start_nulls == end_nulls:
        warnings.warn("The function 'substitute_state_averages' did not reduce the number of null values")
    elif end_nulls > start_nulls:
        raise Exception

    return df


def substitute_hhs_averages(df, race=None):
    # use HHS region averages wherever county values are null
    start_nulls = df.query('MORTALITY.isnull()').shape[0]
    hhsmort = get_hhs_level_df()

    co2hhs = get_co2hhs()
    df = df.merge(right=co2hhs, how='left', on='COFIPS')
    df = df.merge(right=hhsmort, how='left', on=['HHS_REGION', 'RACE', 'AGE_GROUP', 'GENDER'])
    df.loc[df.MORTALITY.isnull(), 'MORTALITY'] = df['HHS_MORTALITY']
    df.drop(columns=['HHS_REGION', 'HHS_MORTALITY'], inplace=True)

    end_nulls = df.query('MORTALITY.isnull()').shape[0]

    if start_nulls == end_nulls:
        warnings.warn("The function 'substitute_state_averages' did not reduce the number of null values")
    elif end_nulls > start_nulls:
        raise Exception

    return df


def substitute_national_averages(df):
    # use HHS region averages wherever county values are null
    start_nulls = df.query('MORTALITY.isnull()').shape[0]

    usamort = get_national_level_df()
    df['COUNTRY'] = 'USA'
    df = df.merge(right=usamort, how='left', on=['COUNTRY', 'RACE', 'AGE_GROUP', 'GENDER'])
    df.loc[df.MORTALITY.isnull(), 'MORTALITY'] = df['USA_MORTALITY']
    df.drop(columns=['COUNTRY', 'USA_MORTALITY', 'MPOP_x_MORT', 'NUMERATOR', 'DENOMENATOR'], inplace=True)

    end_nulls = df.query('MORTALITY.isnull()').shape[0]

    if start_nulls == end_nulls:
        warnings.warn("The function 'substitute_national_averages' did not reduce the number of null values")
    elif end_nulls > start_nulls:
        raise Exception

    return df


def main():
    dataframes = []
    for csv in glob.glob(os.path.join(CDC_DATA, 'Underlying*.txt')):
        df = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
        if '85-100' in os.path.basename(csv):
            continue
        # if 'Hispanic Origin' in df.columns:
        #     continue
        if 'STATE' in os.path.basename(csv):
            continue
        if 'HHS' in os.path.basename(csv):
            continue
        if 'NATIONAL' in os.path.basename(csv):
            continue
        if 'Single Race 6 Code' in df.columns:
            df['RACE'] = df['Single Race 6 Code'].map(RACE_MAP)
        else:
            raise Exception
        df = df[['County Code', 'RACE', 'Gender', 'Five-Year Age Groups Code', 'Deaths', 'Population', 'Crude Rate']]
        df.columns = ['COFIPS', 'RACE', 'GENDER', 'AGE_GROUP', 'DEATHS', 'MPOP', 'MORTALITY']
        df.query('~DEATHS.isnull() & ~MPOP.isnull()', inplace=True)
        # df.dropna(how='any', inplace=True)
        df['COFIPS'] = df['COFIPS'].astype(int).astype(str).str.zfill(5)
        df['DEATHS'] = df['DEATHS'].astype(int)
        df['MPOP'] = df['MPOP'].astype(int)
        df['MORTALITY'] = df.eval('((DEATHS / MPOP) * 100000)').round(0).astype(int)
        dataframes.append(df)
    df = pd.concat(objs=dataframes, ignore_index=True)
    df['GENDER'] = df['GENDER'].str.upper()
    df['DEATHS'] = df['DEATHS'].astype(int)
    df['MPOP'] = df['MPOP'].astype(int)

    co_mort = df.copy()

    # create the template Dataframe that hold all county/race/age combinations
    # and start merging information
    df = create_template()
    df = df.merge(right=co_mort, how='left', on=['COFIPS', 'RACE', 'AGE_GROUP', 'GENDER'])

    # fill in missing values; 'impute' uses urban/rural difference; substitute
    # does not
    df = impute_county_values_by_economic_area(df)
    assert df.shape[0] == 671976
    df = impute_county_values_by_state(df)
    assert df.shape[0] == 671976
    df = impute_county_values_by_hhs_region(df)
    assert df.shape[0] == 671976
    df = impute_county_values_nationally(df)
    assert df.shape[0] == 671976
    df = substitute_state_averages(df)
    assert df.shape[0] == 671976
    df = substitute_hhs_averages(df)
    assert df.shape[0] == 671976
    df = substitute_national_averages(df)
    assert ~df.MORTALITY.isnull().any()
    assert df.shape[0] == 671976

    df = df[['COFIPS', 'RACE', 'AGE_GROUP', 'GENDER', 'MORTALITY']]

    # mortality for 85+ by county
    older = get_85_plus()
    df = pd.concat(objs=[df, older], ignore_index=True, verify_integrity=True)
    assert ~df.MORTALITY.isnull().any()
    assert df.shape[0] == 709308
    df.MORTALITY = df.MORTALITY.round(0).astype(int)

    # make sure COFIPS is updated
    fips_changes = get_fips_changes()
    df = df.merge(right=fips_changes, how='left', left_on='COFIPS', right_on='OLD_FIPS')
    df.loc[~pd.isnull(df['NEW_FIPS']), 'COFIPS'] = df['NEW_FIPS']
    df.drop(labels=['OLD_FIPS', 'NEW_FIPS'], axis=1, inplace=True)

    # TODO: This should be population weighted
    df = df.groupby(by=['COFIPS', 'RACE', 'AGE_GROUP', 'GENDER'], as_index=False).mean()

    # combine <1 and 1-4 age groups using a weighted average; 0.2 and 0.8
    # respectively
    YOUNG_WEIGHT_MAP = {'<1': 0.2,
                        '1-4': 0.8}

    young = df.query('AGE_GROUP == "<1" | AGE_GROUP == "1-4"')
    young['WEIGHT'] = young['AGE_GROUP'].map(YOUNG_WEIGHT_MAP)
    young['MORT_x_WEIGHT'] = young.eval('MORTALITY * WEIGHT')
    young['NUMERATOR'] = young.groupby(by=['COFIPS', 'RACE', 'GENDER'])['MORT_x_WEIGHT'].transform('sum')
    young['DENOMENATOR'] = young.groupby(by=['COFIPS', 'RACE', 'GENDER'])['WEIGHT'].transform('sum')
    young['MORTALITY'] = young.eval('NUMERATOR / DENOMENATOR')
    assert young.shape[0] == 74640

    young['AGE_GROUP'] = '0-4'
    young.drop(columns=['WEIGHT', 'NUMERATOR', 'DENOMENATOR', 'MORT_x_WEIGHT'], inplace=True)
    young.drop_duplicates(inplace=True)

    df.query('AGE_GROUP != "<1" & AGE_GROUP != "1-4"', inplace=True)
    df = pd.concat(objs=[df, young], ignore_index=True, verify_integrity=True)
    df.MORTALITY = df.MORTALITY.round(0).astype(int)
    assert df.shape[0] == 671760
    assert ~df.isnull().any().any()

    p = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases'
    f = 'cdc.sqlite'
    con = sqlite3.connect(os.path.join(p, f))
    df.to_sql(name='mortality_2018_2022_county',
              con=con,
              if_exists='replace',
              index=False)
    con.close()


if __name__ == '__main__':
    main()
