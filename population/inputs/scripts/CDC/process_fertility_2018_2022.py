import os
import sqlite3
import warnings

from itertools import product

import pandas as pd

na_values = ['Suppressed', 'Not Applicable', 'None', 'Missing', 'Not Available']


RACE_MAP = {'2106-3': 'WHITE',
            '2054-5': 'BLACK',
            '1002-5': 'AIAN',
            'A': 'ASIAN',
            'NHOPI': 'NHPI',
            'M': 'TWO_OR_MORE'}

CSV_FILES = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\raw_files\\CDC'
DATABASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases'
MIGRATION_DB = os.path.join(DATABASE_FOLDER, 'migration.sqlite')
CDC_INPUTS = os.path.join(DATABASE_FOLDER, 'cdc')


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


def get_urban_counties():
    query = 'SELECT GEOID AS COFIPS, UA10 AS URBAN_DESTINATION \
             FROM ua10_counties'
    con = sqlite3.connect(MIGRATION_DB)
    df = pd.read_sql_query(sql=query, con=con)
    con.close()

    return df


def get_county_to_bea_df():
    query = 'SELECT * FROM county_to_BEA10'
    con = sqlite3.connect(MIGRATION_DB)
    df = pd.read_sql_query(sql=query, con=con)
    con.close()

    return df


def get_county_level_fertility():

    # single-year age groups for 15-49, and including <15 and 50+
    csv = os.path.join(CSV_FILES, 'Natality, 2018-2022_county.txt')
    df = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
    df = df[['County of Residence Code', 'Mother\'s Single Race 6 Code', 'Age of Mother 9 Code', 'Fertility Rate', 'Female Population']]
    df.columns = ['COFIPS', 'RACE', 'AGE_GROUP', 'FERTILITY', 'FPOP']
    df.dropna(how='all', inplace=True)

    df['COFIPS'] = df['COFIPS'].astype(int).astype(str).str.zfill(5)
    df['RACE'] = df['RACE'].map(RACE_MAP)

    df = df[~df.RACE.isnull()]  # 'na_values' parameter was set previously

    return df


def get_state_level_df():
    # single-year age groups for 15-49, and including <15 and 50+
    csv = os.path.join(CSV_FILES, 'Natality, 2018-2022_state.txt')
    df = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
    df = df[['State of Residence Code', 'Mother\'s Single Race 6 Code', 'Age of Mother 9 Code', 'Fertility Rate']]
    df.columns = ['STFIPS', 'RACE', 'AGE_GROUP', 'STATE_FERTILITY']
    df.dropna(how='all', inplace=True)

    df['STFIPS'] = df['STFIPS'].astype(int).astype(str).str.zfill(2)
    df['RACE'] = df['RACE'].map(RACE_MAP)

    df = df[~df.RACE.isnull()]  # when race is "Not Available"

    return df[['STFIPS', 'RACE', 'AGE_GROUP', 'STATE_FERTILITY']]


def get_hhs_level_df():

    # single-year age groups for 15-49, and including <15 and 50+
    csv = os.path.join(CSV_FILES, 'Natality, 2018-2022_hhs.txt')
    df = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
    df = df[['HHS Region of Residence Code', 'Mother\'s Single Race 6 Code', 'Age of Mother 9 Code', 'Fertility Rate']]
    df.columns = ['HHS_REGION', 'RACE', 'AGE_GROUP', 'HHS_FERTILITY']
    df.dropna(inplace=True)
    df['HHS_REGION'] = df['HHS_REGION'].str.replace('HHS', '').astype(int)
    df['RACE'] = df['RACE'].map(RACE_MAP)
    df = df[~df.RACE.isnull()]  # when race is "Not Available"

    return df[['HHS_REGION', 'RACE', 'AGE_GROUP', 'HHS_FERTILITY']]


def create_template():
    urban_rural = get_urban_counties()

    ages = ['<15',
            '15-19',
            '20-24',
            '25-29',
            '30-34',
            '35-39',
            '40-44',
            '45-49',
            '50+']
    races = list(RACE_MAP.values())
    cofips = list(urban_rural.COFIPS.values)

    df = pd.DataFrame(list(product(cofips, races, ages)),
                      columns=['COFIPS', 'RACE', 'AGE_GROUP'])

    df = df.merge(right=urban_rural, how='left', on='COFIPS')

    return df


def imput_county_values_by_economic_area(df):
    # impute values within BEA economic areas using a population-weighted average
    start_nulls = df.query('FERTILITY.isnull()').shape[0]

    # a table for mapping counties to BEA economic areas
    cy2bea = get_county_to_bea_df()

    df = df.merge(right=cy2bea, how='left', on='COFIPS')
    df.loc[(~df.FPOP.isnull()) & (~df.FERTILITY.isnull()), 'FPOP_x_FERT'] = df.FPOP * df.FERTILITY
    df.loc[~df.FPOP_x_FERT.isnull(), 'NUMERATOR'] = df.groupby(by=['BEA10', 'RACE', 'AGE_GROUP', 'URBAN_DESTINATION'])['FPOP_x_FERT'].transform('sum')
    df.loc[~df.FPOP_x_FERT.isnull(), 'DENOMENATOR'] = df.groupby(by=['BEA10', 'RACE', 'AGE_GROUP', 'URBAN_DESTINATION'])['FPOP'].transform('sum')
    df.eval('_BEA_FERT_WAVG = NUMERATOR / DENOMENATOR', inplace=True)
    df['BEA_FERT_WAVG'] = df.groupby(by=['BEA10', 'RACE', 'AGE_GROUP', 'URBAN_DESTINATION'])['_BEA_FERT_WAVG'].transform('max')
    df.loc[df.FERTILITY.isnull(), 'FERTILITY'] = df['BEA_FERT_WAVG']
    df.drop(columns=['BEA10', 'BEA_FERT_WAVG', '_BEA_FERT_WAVG'], inplace=True)

    end_nulls = df.query('FERTILITY.isnull()').shape[0]

    if start_nulls == end_nulls:
        warnings.warn("The function 'impute_county_values_by_economic_area' did not reduce the number of null values")
    elif end_nulls > start_nulls:
        raise Exception

    return df


def impute_county_values_by_state(df):
    # impute values within BEA economic areas using a population-weighted average
    start_nulls = df.query('FERTILITY.isnull()').shape[0]

    # impute values within states using a population-weighted average
    df['STFIPS'] = df['COFIPS'].str[:2]
    df.loc[(~df.FPOP.isnull()) & (~df.FERTILITY.isnull()), 'FPOP_x_FERT'] = df.FPOP * df.FERTILITY
    df.loc[~df.FPOP_x_FERT.isnull(), 'NUMERATOR'] = df.groupby(by=['STFIPS', 'RACE', 'AGE_GROUP', 'URBAN_DESTINATION'])['FPOP_x_FERT'].transform('sum')
    df.loc[~df.FPOP_x_FERT.isnull(), 'DENOMENATOR'] = df.groupby(by=['STFIPS', 'RACE', 'AGE_GROUP', 'URBAN_DESTINATION'])['FPOP'].transform('sum')
    df.eval('_STATE_FERT_WAVG = NUMERATOR / DENOMENATOR', inplace=True)
    df['STATE_FERT_WAVG'] = df.groupby(by=['STFIPS', 'RACE', 'AGE_GROUP', 'URBAN_DESTINATION'])['_STATE_FERT_WAVG'].transform('max')
    df.loc[df.FERTILITY.isnull(), 'FERTILITY'] = df['STATE_FERT_WAVG']
    df.drop(columns=['STFIPS', 'STATE_FERT_WAVG', '_STATE_FERT_WAVG'], inplace=True)

    end_nulls = df.query('FERTILITY.isnull()').shape[0]

    if start_nulls == end_nulls:
        warnings.warn("The function 'impute_county_values_by_state' did not reduce the number of null values")
    elif end_nulls > start_nulls:
        raise Exception

    return df


def impute_county_values_by_hhs_region(df):
    # impute values within BEA economic areas using a population-weighted average
    start_nulls = df.query('FERTILITY.isnull()').shape[0]

    # a table for mapping states and counties to HHS regions
    co2hhs = get_co2hhs()

    # impute values with HHS regions
    df = df.merge(right=co2hhs, how='left', on='COFIPS')
    df.loc[(~df.FPOP.isnull()) & (~df.FERTILITY.isnull()), 'FPOP_x_FERT'] = df.FPOP * df.FERTILITY
    df.loc[~df.FPOP_x_FERT.isnull(), 'NUMERATOR'] = df.groupby(by=['HHS_REGION', 'RACE', 'AGE_GROUP', 'URBAN_DESTINATION'])['FPOP_x_FERT'].transform('sum')
    df.loc[~df.FPOP_x_FERT.isnull(), 'DENOMENATOR'] = df.groupby(by=['HHS_REGION', 'RACE', 'AGE_GROUP', 'URBAN_DESTINATION'])['FPOP'].transform('sum')
    df.eval('_HHS_FERT_WAVG = NUMERATOR / DENOMENATOR', inplace=True)
    df['HHS_FERT_WAVG'] = df.groupby(by=['HHS_REGION', 'RACE', 'AGE_GROUP', 'URBAN_DESTINATION'])['_HHS_FERT_WAVG'].transform('max')
    df.loc[df.FERTILITY.isnull(), 'FERTILITY'] = df['HHS_FERT_WAVG']
    df.drop(columns=['HHS_REGION', 'HHS_FERT_WAVG', '_HHS_FERT_WAVG'], inplace=True)

    end_nulls = df.query('FERTILITY.isnull()').shape[0]

    if start_nulls == end_nulls:
        warnings.warn("The function 'impute_county_values_by_state' did not reduce the number of null values")
    elif end_nulls > start_nulls:
        raise Exception

    return df


def impute_unid_county_values_by_state(co_fert, df):
    # impute values with state (Unidentified counties only)
    # some age/race/urbanization counties are still null; use state-level
    # aggregations - "Unidentified Counties" - from the county-level file. These
    # values are the state-level average MINUS the reported county values, so
    # they are preferred over simple state-level averages, but still lack the
    # urban/rural split

    co_fert_unid = co_fert.query('COFIPS.str.endswith("999")')
    co_fert_unid.loc[:, 'STFIPS'] = co_fert_unid.COFIPS.str[:2]
    co_fert_unid = co_fert_unid.rename(columns={'FERTILITY': 'CO_FERT_UNID'})
    co_fert_unid = co_fert_unid[['STFIPS', 'RACE', 'AGE_GROUP', 'CO_FERT_UNID']]
    df['STFIPS'] = df['COFIPS'].str[:2]
    df = df.merge(right=co_fert_unid, how='left', on=['STFIPS', 'RACE', 'AGE_GROUP'])
    df.loc[df.FERTILITY.isnull(), 'FERTILITY'] = df['CO_FERT_UNID']

    return df


def impute_unid_county_values_by_hhs(df):

    co2hhs = get_co2hhs()

    # impute values with HHS regions (Unidentified counties only)
    df = df.merge(right=co2hhs, how='left', on='COFIPS')
    df.loc[(~df.FPOP.isnull()) & (~df.CO_FERT_UNID.isnull()), 'FPOP_x_FERT'] = df.FPOP * df.CO_FERT_UNID
    df.loc[~df.FPOP_x_FERT.isnull(), 'NUMERATOR'] = df.groupby(by=['HHS_REGION', 'RACE', 'AGE_GROUP'])['FPOP_x_FERT'].transform('sum')
    df.loc[~df.FPOP_x_FERT.isnull(), 'DENOMENATOR'] = df.groupby(by=['HHS_REGION', 'RACE', 'AGE_GROUP'])['FPOP'].transform('sum')
    df.eval('_HHS_FERT_WAVG = NUMERATOR / DENOMENATOR', inplace=True)
    df['HHS_FERT_WAVG'] = df.groupby(by=['HHS_REGION', 'RACE', 'AGE_GROUP'])['_HHS_FERT_WAVG'].transform('max')
    df.loc[df.FERTILITY.isnull(), 'FERTILITY'] = df['HHS_FERT_WAVG']
    df.drop(columns=['CO_FERT_UNID', 'NUMERATOR', 'DENOMENATOR', 'HHS_REGION', 'HHS_FERT_WAVG', '_HHS_FERT_WAVG'], inplace=True)

    return df


def substitute_state_averges(df):
    # NHPI 40-44 is still null in three HHS regions (1, 5, 2) so use the state
    # average for now

    stfert = get_state_level_df()
    df['STFIPS'] = df['COFIPS'].str[:2]
    df = df.merge(right=stfert, how='left', on=['STFIPS', 'RACE', 'AGE_GROUP'])
    df.loc[df.FERTILITY.isnull(), 'FERTILITY'] = df['STATE_FERTILITY']
    df.drop(columns=['STFIPS', 'STATE_FERTILITY'], inplace=True)

    return df


def substitute_hhs_averages(df):
    co2hhs = get_co2hhs()

    # still a few nulls, use HHS region averages
    hhsfert = get_hhs_level_df()
    df = df.merge(right=co2hhs, how='left', on='COFIPS')
    df = df.merge(right=hhsfert, how='left', on=['HHS_REGION', 'RACE', 'AGE_GROUP'])
    df.loc[df.FERTILITY.isnull(), 'FERTILITY'] = df['HHS_FERTILITY']
    df.drop(columns=['HHS_REGION', 'HHS_FERTILITY'], inplace=True)

    return df


def substitute_national_average(df):
    # still a few nulls, use national averages that account urban/rural
    df['UNIT'] = 1
    df.loc[(~df.FPOP.isnull()) & (~df.FERTILITY.isnull()), 'FPOP_x_FERT'] = df.FPOP * df.FERTILITY
    df.loc[~df.FPOP_x_FERT.isnull(), 'NUMERATOR'] = df.groupby(by=['UNIT', 'RACE', 'AGE_GROUP', 'URBAN_DESTINATION'])['FPOP_x_FERT'].transform('sum')
    df.loc[~df.FPOP_x_FERT.isnull(), 'DENOMENATOR'] = df.groupby(by=['UNIT', 'RACE', 'AGE_GROUP', 'URBAN_DESTINATION'])['FPOP'].transform('sum')
    df.eval('_USA_FERT_WAVG = NUMERATOR / DENOMENATOR', inplace=True)
    df['USA_FERT_WAVG'] = df.groupby(by=['UNIT', 'RACE', 'AGE_GROUP', 'URBAN_DESTINATION'])['_USA_FERT_WAVG'].transform('max')
    df.loc[df.FERTILITY.isnull(), 'FERTILITY'] = df['USA_FERT_WAVG']
    df.drop(columns=['UNIT', 'USA_FERT_WAVG', '_USA_FERT_WAVG'], inplace=True)

    return df


def main():
    '''
    Not all race/gender/age combinations are available at the state level. Use
    HHS Region, and then national rates as needed. Rates for the 85+ group are
    in a separate file, so a total of six files are needed.
    '''

    # county level fertility from CDC; lots of missing values that we'll fill in
    co_fert = get_county_level_fertility()

    # create the template Dataframe that hold all county/race/age combinations
    # and start merging information
    df = create_template()
    df = df.merge(right=co_fert, how='left', on=['COFIPS', 'RACE', 'AGE_GROUP'])

    df = imput_county_values_by_economic_area(df)
    df = impute_county_values_by_state(df)
    df = impute_county_values_by_hhs_region(df)
    df = impute_unid_county_values_by_state(co_fert=co_fert, df=df)
    df = impute_unid_county_values_by_hhs(df)
    df = substitute_state_averges(df)
    df = substitute_hhs_averages(df)
    df = substitute_national_average(df)

    df = df.loc[~df.AGE_GROUP.isin(("<15", "45-49", "50+")), :]

    # make sure COFIPS is updated and use population-weighted average if merging
    FIPS_CHANGES = get_fips_changes()
    df = df.merge(right=FIPS_CHANGES, how='left', left_on='COFIPS', right_on='OLD_FIPS')
    df.loc[~pd.isnull(df['NEW_FIPS']), 'COFIPS'] = df['NEW_FIPS']
    df.drop(labels=['OLD_FIPS', 'NEW_FIPS'], axis=1, inplace=True)

    df.eval('FPOP_x_FERT = FERTILITY * FPOP', inplace=True)
    df['NUMERATOR'] = df.groupby(by=['COFIPS', 'RACE', 'AGE_GROUP'])['FPOP_x_FERT'].transform('sum')
    df['DENOMENATOR'] = df.groupby(by=['COFIPS', 'RACE', 'AGE_GROUP'])['FPOP'].transform('sum')
    df.eval('FERTILITY_WAVG = NUMERATOR / DENOMENATOR', inplace=True)

    df = df[['COFIPS', 'RACE', 'AGE_GROUP', 'FERTILITY']]
    assert not df.isnull().any().any()

    con = sqlite3.connect(os.path.join(DATABASE_FOLDER, 'cdc.sqlite'))
    df.to_sql(name='fertility_2018_2022_county',
              con=con,
              if_exists='replace',
              index=False)
    con.close()

    print("Finished!")


if __name__ == '__main__':
    main()
