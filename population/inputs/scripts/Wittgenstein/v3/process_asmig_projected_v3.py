"""
Author:  Phil Morefield (pmorefie@gmu.edu)
Purpose:
Created:
"""
import os
import sqlite3

import numpy as np
import pandas as pd

if os.path.exists('D:\\OneDrive\\ICLUS_v3'):
    ICLUS_FOLDER = 'D:\\OneDrive\\ICLUS_v3'
else:
    ICLUS_FOLDER = 'D:\\projects\\ICLUS_v3'

DATABASES = os.path.join(ICLUS_FOLDER, 'population\\inputs\\databases')


def parse_years(s):

    s = s.split('-')
    if int(s[0]) == 2095:
        return range(2095, 2100)

    return range(int(s[0]), int(s[1]))


def interpolate_years(df):
    scenarios = df.SCENARIO.unique()

    df['NETMIG_INTERP'] = df['NETMIG']
    df.sort_values(by=['YEAR', 'SCENARIO'], inplace=True)

    df.loc[(~df.YEAR.astype(str).str.endswith('2')) &
           (~df.YEAR.astype(str).str.endswith('7')) &
           (df.YEAR != 2020), 'NETMIG_INTERP'] = np.nan

    for scenario in scenarios:
        s = df.loc[df.SCENARIO == scenario, 'NETMIG_INTERP']
        df.loc[df.SCENARIO == scenario, 'NETMIG_INTERP'] = s.interpolate(method='linear', limit_direction='both')

    assert ~df.isnull().any().any()

    return df


def combine_85plus(df):

    older = df.query('AGE_GROUP == "85-89" | AGE_GROUP == "90-94" | AGE_GROUP == "95-99" | AGE_GROUP == "100+"')
    older = older[['YEAR', 'SCENARIO', 'SEX', 'NETMIG_INTERP']]
    older = older.groupby(by=['YEAR', 'SCENARIO', 'SEX'], as_index=False).sum()
    older['AGE_GROUP'] = "85+"
    older.drop_duplicates(inplace=True)

    df.query('AGE_GROUP != "85-89" & AGE_GROUP != "90-94" & AGE_GROUP != "95-99" & AGE_GROUP != "100+"', inplace=True)
    df = pd.concat(objs=[df, older], ignore_index=True)

    return df

def decompose_to_age_groups(df):
    # v3 doesn't have immigration by age; use the v2 proportions
    # age/sex fractions are not part of v3, so substituting v2 fractions
    # since there is no SSP4 and SSP5 in v2, using SSP2 fractions instead
    con = sqlite3.connect(database=os.path.join(DATABASES, 'wittgenstein.sqlite'))
    query = 'SELECT * FROM age_specific_net_migration_v2'
    temp = pd.read_sql_query(sql=query, con=con)
    con.close()
    temp['TOTAL_IMM'] = temp.groupby(by=['YEAR', 'SCENARIO'], as_index=False)['NETMIG_INTERP'].transform('sum')
    temp.eval('IMM_FRACTION = NETMIG_INTERP / TOTAL_IMM', inplace=True)
    temp.drop(columns=['NETMIG_INTERP', 'TOTAL_IMM'], inplace=True)

    ssp2 = temp.query('SCENARIO == "SSP2"')
    ssp4 = ssp2.copy()
    ssp4['SCENARIO'] = 'SSP4'
    ssp5 = ssp4.copy()
    ssp5['SCENARIO'] = 'SSP5'

    df['NETMIG_INTERP_COHORT'] = df['NETMIG_INTERP']
    df = df.set_index(['YEAR', 'SCENARIO', 'PERIOD', 'NETMIG', 'NETMIG_INTERP'])

    temp = temp.set_index(['YEAR', 'SCENARIO', 'AGE_GROUP', 'SEX'])
    ssp4 = ssp4.set_index(['YEAR', 'SCENARIO', 'AGE_GROUP', 'SEX'])
    ssp5 = ssp5.set_index(['YEAR', 'SCENARIO', 'AGE_GROUP', 'SEX'])
    temp = pd.concat(objs=[temp, ssp4, ssp5])
    temp.columns = ['NETMIG_INTERP_COHORT']

    df = df.mul(other=temp, axis='index').reset_index()

    return df



def align_with_2020(df):

    scenarios = df.SCENARIO.unique()
    years = range(2010, 2100)

    census_avg_2324 = 2500000

    df.sort_values(by=['SCENARIO', 'YEAR', 'SEX', 'AGE_GROUP'], inplace=True)
    for scenario in scenarios:
        witt_total_2020 = None
        for year in years:

            if witt_total_2020 is None:
                # total 2020 immigration from Wittgenstein; this does NOT match the observed value from Census
                witt_total_2020 = df.query('SCENARIO == @scenario & YEAR == 2020').NETMIG_INTERP.sum()

            # total projected immigration from Wittgenstein
            witt_total_proj = df.query('SCENARIO == @scenario & YEAR == @year').NETMIG_INTERP.sum()

            # convert values to a percentage of the annual immigration from Wittgenstein
            df.loc[(df.SCENARIO == scenario) & (df.YEAR == year), 'NETMIG_INTERP'] /= witt_total_proj

            # use the delta method to base Wittgenstein changes on the actual immigration in 2020
            df.loc[(df.SCENARIO == scenario) & (df.YEAR == year), 'NETMIG_INTERP'] *= (census_avg_2324 * (witt_total_proj / witt_total_2020))

    return df


def main():
    csv = os.path.join(ICLUS_FOLDER, 'population\\inputs\\raw_files\\Wittgenstein\\v3\\wcde_asmig.csv')
    df = pd.read_csv(filepath_or_buffer=csv, skiprows=8)
    df.columns = ['SCENARIO', 'AREA', 'PERIOD', 'NETMIG']
    df = df[['SCENARIO', 'PERIOD', 'NETMIG']]

    # add years for 2010 to 2020
    for time_period in (('2015-2020'), ('2010-2015')):
        temp = df.query('PERIOD == "2020-2025"')
        temp.loc[:, 'PERIOD'] = time_period
        df = pd.concat(objs=[temp, df], ignore_index=True)

    # expand time periods
    df['YEARS'] = df['PERIOD'].apply(lambda x: parse_years(x))
    exploded = df.apply(lambda x: pd.Series(x['YEARS']), axis=1).stack().reset_index(level=1, drop=True)
    exploded.name = 'YEAR'
    df = df.drop(columns='YEARS').join(exploded)
    df['YEAR'] = df.YEAR.astype(int)
    df.reset_index(drop=True, inplace=True)
    df['NETMIG'] *= 1000.0
    df['NETMIG'] /= 5.0

    df = interpolate_years(df)
    df = decompose_to_age_groups(df)
    df = combine_85plus(df)
    df = align_with_2020(df)

    df['NETMIG'] = df['NETMIG'].astype(int)
    df['NETMIG_INTERP'] = df['NETMIG_INTERP'].astype(int)
    df['NETMIG_INTERP_COHORT'] = df['NETMIG_INTERP_COHORT'].round().astype(int)

    df = df[['YEAR', 'SCENARIO', 'AGE_GROUP', 'SEX', 'NETMIG', 'NETMIG_INTERP', 'NETMIG_INTERP_COHORT']]

    con = sqlite3.connect(database=os.path.join(DATABASES, 'wittgenstein.sqlite'))
    df.to_sql(name='age_specific_net_migration_v3',
              if_exists='replace',
              con=con,
              index=False)
    con.close()

    print("Finished!")


if __name__ == '__main__':
    main()
