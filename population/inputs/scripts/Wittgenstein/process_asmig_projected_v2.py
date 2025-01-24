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
    genders = ('MALE', 'FEMALE')

    df['NETMIG_INTERP'] = df['NETMIG']
    df.sort_values(by=['YEAR', 'AGE_GROUP', 'SCENARIO'], inplace=True)

    df.loc[(~df.YEAR.astype(str).str.endswith('2')) & (~df.YEAR.astype(str).str.endswith('7')), 'NETMIG_INTERP'] = np.nan

    for scenario, age_group, gender in product(scenarios, age_groups, genders):
        s = df.loc[(df.SCENARIO == scenario) & (df.AGE_GROUP == age_group) & (df.GENDER == gender), 'NETMIG_INTERP']
        df.loc[(df.SCENARIO == scenario) & (df.AGE_GROUP == age_group) & (df.GENDER == gender), 'NETMIG_INTERP'] = s.interpolate()
        value2017 = df.query('SCENARIO == @scenario & AGE_GROUP == @age_group & GENDER == @gender & YEAR == 2017')['NETMIG_INTERP'].values[0]
        value2018 = df.query('SCENARIO == @scenario & AGE_GROUP == @age_group & GENDER == @gender & YEAR == 2018')['NETMIG_INTERP'].values[0]
        increment = value2018 - value2017
        value2016 = value2017 - increment
        value2015 = value2016 - increment
        df.loc[(df['SCENARIO'] == scenario) & (df['AGE_GROUP'] == age_group) & (df['GENDER'] == gender) & (df['YEAR'] == 2016), 'NETMIG_INTERP'] = value2016
        df.loc[(df['SCENARIO'] == scenario) & (df['AGE_GROUP'] == age_group) & (df['GENDER'] == gender) & (df['YEAR'] == 2015), 'NETMIG_INTERP'] = value2015

    assert ~df.isnull().any().any()

    # this was just needed for troubleshooting/QA

    # output_folder = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases'
    # con = sqlite3.connect(database=os.path.join(output_folder, 'wittgenstein', 'wittgenstein.sqlite'))
    # df.to_sql(name='age_specific_net_migration_ORIG',
    #           if_exists='replace',
    #           con=con,
    #           index=False)
    # con.close()

    return df


def combine_85plus(df):

    older = df.query('AGE_GROUP == "85-89" | AGE_GROUP == "90-94" | AGE_GROUP == "95-99" | AGE_GROUP == "100+"')
    older = older[['YEAR', 'SCENARIO', 'GENDER', 'NETMIG_INTERP']]
    older = older.groupby(by=['YEAR', 'SCENARIO', 'GENDER'], as_index=False).sum()
    older['AGE_GROUP'] = "85+"
    older.drop_duplicates(inplace=True)

    df.query('AGE_GROUP != "85-89" & AGE_GROUP != "90-94" & AGE_GROUP != "95-99" & AGE_GROUP != "100+"', inplace=True)
    df = pd.concat(objs=[df, older], ignore_index=True)

    return df


def align_with_2020(df):

    scenarios = df.SCENARIO.unique()
    years = range(2020, 2100)

    # this is the average estimated value for 2020-2023 taken from the Census
    # Intercensal Estimates. 2020 was an outlier because of Covid/Trump and
    # immigration for these three years was generally in-line with 2010-2019
    # annual immigration.
    census_total_2020 = 1672321

    df.sort_values(by=['SCENARIO', 'YEAR', 'GENDER', 'AGE_GROUP'], inplace=True)
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
            df.loc[(df.SCENARIO == scenario) & (df.YEAR == year), 'NETMIG_INTERP'] *= (census_total_2020 * (witt_total_proj / witt_total_2020))

    return df


def main():
    csv = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\raw_files\\Wittgenstein\\wcde_asmig.csv'
    df = pd.read_csv(filepath_or_buffer=csv, skiprows=8)
    df.columns = ['SCENARIO', 'AREA', 'PERIOD', 'AGE_GROUP', 'GENDER', 'NETMIG']
    df = df[['SCENARIO', 'PERIOD', 'AGE_GROUP', 'GENDER', 'NETMIG']]
    df['GENDER'] = df['GENDER'].str.upper()
    df['AGE_GROUP'] = df['AGE_GROUP'].str.replace('--', '-')

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
    df = df[['YEAR', 'SCENARIO', 'AGE_GROUP', 'GENDER', 'NETMIG_INTERP']]

    df = combine_85plus(df)
    df = align_with_2020(df)
    df.eval('NETMIG_INTERP = NETMIG_INTERP.astype("int")', inplace=True)

    output_folder = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases'
    con = sqlite3.connect(database=os.path.join(output_folder, 'wittgenstein.sqlite'))
    df.to_sql(name='age_specific_net_migration',
              if_exists='replace',
              con=con,
              index=False)
    con.close()

    print("Finished!")


if __name__ == '__main__':
    main()
