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


BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'

CSV_FILES = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\CDC\\age')
DATABASE_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\databases')
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
    csv = os.path.join(CSV_FILES, 'Natality, 2020-2024, county.csv')
    fert = pd.read_csv(filepath_or_buffer=csv, sep=None, engine='python', na_values=na_values)
    fert = fert[['County of Residence Code', 'Age of Mother 9 Code', 'Births', 'Female Population', 'Fertility Rate']]
    fert.columns = ['COFIPS', 'AGE_GROUP', 'BIRTHS', 'FEMALE_POPULATION', 'FERTILITY']
    fert.dropna(how='any', inplace=True)

    fert['COFIPS'] = fert['COFIPS'].astype(int).astype(str).str.zfill(5)

    df = df.merge(right=fert[['COFIPS', 'AGE_GROUP', 'FERTILITY']],
                  how='left',
                  on=['COFIPS', 'AGE_GROUP'])

    # now apply the "Unidentified Counties" values
    fert_uc = fert.loc[fert.COFIPS.str.endswith('999'), ['COFIPS', 'AGE_GROUP', 'FERTILITY']]
    fert_uc = fert_uc.rename(columns={'FERTILITY': 'FERT_UC'})
    fert_uc['STFIPS'] = fert_uc.COFIPS.str[:2]
    fert_uc = fert_uc.drop(columns='COFIPS')

    df['STFIPS'] = df['COFIPS'].str[:2]

    df = df.merge(right=fert_uc, how='left', on=['STFIPS', 'AGE_GROUP'])
    df.loc[df.FERTILITY.isnull(), 'FERTILITY'] = df['FERT_UC']
    df = df.drop(columns=['FERT_UC', 'STFIPS'])
    df = df.loc[~df.COFIPS.str.endswith('999')]

    # At this point in time, some data sources report county level data for
    # CT and others report by "Planning Region". For consistency I will treat
    # CT as a single county equivalent.
    fert_ct = fert.loc[fert.COFIPS.str.startswith('09'), ['AGE_GROUP', 'BIRTHS', 'FEMALE_POPULATION']]
    fert_ct = fert_ct.groupby(by=['AGE_GROUP'], as_index=False).sum()
    fert_ct['FERTILITY'] = fert_ct['BIRTHS'] / fert_ct['FEMALE_POPULATION'] * 1000
    fert_ct['COFIPS'] = '09999'
    fert_ct = fert_ct[['COFIPS', 'AGE_GROUP', 'FERTILITY']]

    df = pd.concat(objs=[df, fert_ct], ignore_index=True, axis=0)

    return df.sort_values(by=['COFIPS', 'AGE_GROUP'])



def create_template():
    cofips_all = get_cofips_and_state()

    ages = ['15-19',
            '20-24',
            '25-29',
            '30-34',
            '35-39',
            '40-44']

    cofips = list(cofips_all.COFIPS.values)

    df = pd.DataFrame(list(product(cofips, ages)),
                      columns=['COFIPS', 'AGE_GROUP'])

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

    assert not df.isnull().any().any()

    con = sqlite3.connect(os.path.join(DATABASE_FOLDER, 'cdc.sqlite'))
    df.to_sql(name='fertility_2020_2024_county',
              con=con,
              if_exists='replace',
              index=False)
    con.close()

    df.to_csv(path_or_buf=os.path.join(DATABASE_FOLDER, 'fertility_2020_2024_county.csv'), index=False)

    print("Finished!")


if __name__ == '__main__':
    main()
