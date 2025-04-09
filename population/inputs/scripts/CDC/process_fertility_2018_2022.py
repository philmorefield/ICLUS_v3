'''
Revised: 2025-03-26

This script processes fertility rates for 2018-2022 from the CDC. County level
fertility rates are assigned to each county using the following logic:

1) If there is a rate for the county/race/age group, use that rate.
2) If the fertility rate for a county/race/age group is still undefined,
   use the "Unidentified Counties" rate from the county level data.
3) If the fertility rate for a county is still undefined, use the state-wide
   rate for that race/age cohort.
4) If the county and state rates are missing, use the HHS region rate.
5) The NHPI rates for some counties are obviously spurious (e.g., >4,000 births
   per 1,000 females). Through personal communication with CDC staff, I
   confirmed that these artifacts are due to unusually low population estimates
   from the Census. Based on histograms of fertility rates for all race/age
   cohorts, all NHPI fertility rates >300 are replaced with the HHS region
   rate.
'''

import os
import sqlite3

from itertools import product

import pandas as pd

na_values = ['Suppressed', 'Not Applicable', 'None', 'Missing', 'Not Available']


RACE_MAP = {'2106-3': 'WHITE',
            '2054-5': 'BLACK',
            '1002-5': 'AIAN',
            'A': 'ASIAN',
            'NHOPI': 'NHPI',
            'M': 'TWO_OR_MORE'}

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


def apply_county_level_fertility(df):

    # first apply county level fertility; not all cohorts will have values
    csv = os.path.join(CSV_FILES, 'Natality, 2018-2022_county.txt')
    fert = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
    fert = fert[['County of Residence Code', 'Mother\'s Single Race 6 Code', 'Age of Mother 9 Code', 'Fertility Rate']]
    fert.columns = ['COFIPS', 'RACE', 'AGE_GROUP', 'FERTILITY']
    fert.dropna(how='any', inplace=True)

    fert['COFIPS'] = fert['COFIPS'].astype(int).astype(str).str.zfill(5)
    fert['RACE'] = fert['RACE'].map(RACE_MAP)

    df = df.merge(right=fert, how='left', on=['COFIPS', 'RACE', 'AGE_GROUP'])

    # now apply the "Unidentified Counties" values
    fert_uc = fert.loc[fert.COFIPS.str.endswith('999'), :]
    fert_uc = fert_uc.rename(columns={'FERTILITY': 'FERT_UC'})
    fert_uc['STFIPS'] = fert_uc.COFIPS.str[:2]
    fert_uc = fert_uc.drop(columns='COFIPS')

    df['STFIPS'] = df['COFIPS'].str[:2]

    df = df.merge(right=fert_uc, how='left', on=['STFIPS', 'RACE', 'AGE_GROUP'])
    df.loc[df.FERTILITY.isnull(), 'FERTILITY'] = df['FERT_UC']
    df = df.drop(columns=['FERT_UC'])

    return df


def apply_state_level_fertility(df):
    # single-year age groups for 15-49, and including <15 and 50+
    csv = os.path.join(CSV_FILES, 'Natality, 2018-2022_state.txt')
    fert = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
    fert = fert[['State of Residence Code', 'Mother\'s Single Race 6 Code', 'Age of Mother 9 Code', 'Fertility Rate']]
    fert.columns = ['STFIPS', 'RACE', 'AGE_GROUP', 'STATE_FERTILITY']
    fert.dropna(how='any', inplace=True)

    fert['STFIPS'] = fert['STFIPS'].astype(int).astype(str).str.zfill(2)
    fert['RACE'] = fert['RACE'].map(RACE_MAP)

    df = df.merge(right=fert, how='left', on=['STFIPS', 'RACE', 'AGE_GROUP'])
    df.loc[df.FERTILITY.isnull(), 'FERTILITY'] = df['STATE_FERTILITY']

    # several counties show obviously spurious NHPI rates, e.g., > 1000;
    # visual inspection of other races show a limit of about 200-300, so use
    # state averages for NHPI rates above 300
    df.loc[(df.FERTILITY > 300) & (df.RACE == 'NHPI'), 'FERTILITY'] = df['STATE_FERTILITY']

    df = df.drop(columns=['STFIPS', 'STATE_FERTILITY'])

    return df


def apply_hhs_level_fertility(df):
    query = 'SELECT COFIPS, HHS AS HHS_REGION \
             FROM fips_to_urb20_bea10_hhs'
    con = sqlite3.connect(MIGRATION_DB)
    hhs = pd.read_sql_query(sql=query, con=con)
    con.close()

    # single-year age groups for 15-49, and including <15 and 50+
    csv = os.path.join(CSV_FILES, 'Natality, 2018-2022_hhs.txt')
    fert = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
    fert = fert[['HHS Region of Residence Code', 'Mother\'s Single Race 6 Code', 'Age of Mother 9 Code', 'Fertility Rate']]
    fert.columns = ['HHS_REGION', 'RACE', 'AGE_GROUP', 'HHS_FERTILITY']
    fert.dropna(how='any', inplace=True)

    fert['HHS_REGION'] = fert['HHS_REGION'].str.replace('HHS', '').astype(int)
    fert['RACE'] = fert['RACE'].map(RACE_MAP)

    # identify the HHS region for each county
    df = df.merge(right=hhs, how='left', on='COFIPS')

    # join HHS-level fertility rates
    df = df.merge(right=fert, how='left', on=['HHS_REGION', 'RACE', 'AGE_GROUP'])
    df.loc[df.FERTILITY.isnull(), 'FERTILITY'] = df['HHS_FERTILITY']

    # several counties show obviously spurious NHPI rates, e.g., > 1000;
    # visual inspection of other races show a limit of about 200-300, so use
    # HHS averages for NHPI rates above 300
    df.loc[(df.FERTILITY > 300) & (df.RACE == 'NHPI'), 'FERTILITY'] = df['HHS_FERTILITY']

    df = df.drop(columns=['HHS_REGION', 'HHS_FERTILITY'])

    return df


def create_template():
    cofips_all = get_cofips_and_state()

    ages = ['15-19',
            '20-24',
            '25-29',
            '30-34',
            '35-39',
            '40-44']

    races = list(RACE_MAP.values())
    cofips = list(cofips_all.COFIPS.values)

    df = pd.DataFrame(list(product(cofips, races, ages)),
                      columns=['COFIPS', 'RACE', 'AGE_GROUP'])

    return df


def make_fips_changes(df):
    con =sqlite3.connect(MIGRATION_DB)
    query = 'SELECT OLD_FIPS AS COFIPS, NEW_FIPS \
             FROM fips_or_name_changes'
    df_fips = pd.read_sql_query(sql=query, con=con)
    con.close()

    df = df.merge(right=df_fips,
                  how='left',
                  on='COFIPS')

    df.loc[~df.NEW_FIPS.isnull(), 'COFIPS'] = df['NEW_FIPS']
    df = df.drop(columns='NEW_FIPS')

    # TODO: this mean should be weighted by population, technically
    df = df.groupby(by=['COFIPS', 'AGE_GROUP', 'RACE'], as_index=False).mean()

    return df


def main():
    '''
    Not all race/gender/age combinations are available at the county level. Use
    state and then HHS Region rates as needed.
    '''
    # create the template Dataframe that hold all county/race/age combinations
    # and start merging information
    df = create_template()

    # county level fertility from CDC; lots of missing values that we'll fill in
    df = apply_county_level_fertility(df)
    df = apply_state_level_fertility(df)
    df = apply_hhs_level_fertility(df)
    df = make_fips_changes(df)

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
