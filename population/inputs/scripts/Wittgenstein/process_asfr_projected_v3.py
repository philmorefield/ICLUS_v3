"""
Author:  Phil Morefield (pmorefie@gmu.edu)
Purpose:
Created:
"""
import os
import sqlite3

from itertools import product

import numpy as np
import pandas as pd


def parse_years(s):

    s = s.split('-')
    if int(s[0]) == 2095:
        return range(2095, 2100)

    return range(int(s[0]), int(s[1]))


def interpolate_years(df):
    scenarios = df.SCENARIO.unique()
    age_groups = df.AGE_GROUP.unique()

    df['FERT_INTERP'] = df['BIRTHS_PER_K']
    df.sort_values(by=['YEAR', 'AGE_GROUP', 'SCENARIO'], inplace=True)
    values_2020 = df.loc[df.YEAR == 2020]

    df.loc[(~df.YEAR.astype(str).str.endswith('2')) &
           (~df.YEAR.astype(str).str.endswith('7')) &
           (df.YEAR != 2020), 'FERT_INTERP'] = np.nan

    # interpolate (backwards) to 2010
    # df.loc[df.YEAR < 2020, 'FERT_INTERP'] = np.nan

    for scenario, age_group in product(scenarios, age_groups):
        s = df.loc[(df.SCENARIO == scenario) & (df.AGE_GROUP == age_group), 'FERT_INTERP']
        df.loc[(df.SCENARIO == scenario) & (df.AGE_GROUP == age_group), 'FERT_INTERP'] = s.interpolate(method='linear')
        value_2020 = values_2020.loc[(df.SCENARIO == scenario) & (df.AGE_GROUP == age_group), 'FERT_INTERP'].values[0]
        df.loc[(df.SCENARIO == scenario) & (df.AGE_GROUP == age_group), 'FERT_CHANGE_MULT'] = df['FERT_INTERP'] / value_2020

    return df


def main():
    csv = 'D:\\OneDrive\\Dissertation\\data\\Wittgenstein\\v3\\wcde_asfr.csv'
    df = pd.read_csv(filepath_or_buffer=csv, skiprows=8)
    df.columns = ['SCENARIO', 'AREA', 'PERIOD', 'AGE_GROUP', 'BIRTHS_PER_K']
    df = df[['SCENARIO', 'PERIOD', 'AGE_GROUP', 'BIRTHS_PER_K']]
    df['AGE_GROUP'] = df['AGE_GROUP'].str.replace('--', '_to_')

    # # add years for 2010 to 2020
    # for time_period in (('2015-2020'), ('2010-2015')):
    #     temp = df.query('PERIOD == "2020-2025"')
    #     temp['PERIOD'] = time_period
    #     temp['BIRTHS_PER_K'] = 0.123
    #     df = pd.concat(objs=[temp, df], ignore_index=True)

    # expand time periods
    df['YEARS'] = df['PERIOD'].apply(lambda x: parse_years(x))
    exploded = df.apply(lambda x: pd.Series(x['YEARS']), axis=1).stack().reset_index(level=1, drop=True)
    exploded.name = 'YEAR'
    df = df.drop(columns='YEARS').join(exploded)
    df['YEAR'] = df.YEAR.astype(int)
    df.reset_index(drop=True, inplace=True)

    df = interpolate_years(df)

    df.query('YEAR >= 2020', inplace=True)
    df = df[['YEAR', 'SCENARIO', 'AGE_GROUP', 'FERT_CHANGE_MULT']]
    df.eval('AGE_GROUP = AGE_GROUP.str.replace("_to_", "-")', inplace=True)

    output_folder = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases'
    con = sqlite3.connect(database=os.path.join(output_folder, 'wittgenstein.sqlite'))
    df.to_sql(name='age_specific_fertility_v3', if_exists='replace', con=con, index=False)
    con.close()

    print("Finished!")

if __name__ == '__main__':
    main()
