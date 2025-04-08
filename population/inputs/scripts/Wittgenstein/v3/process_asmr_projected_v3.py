import os
import sqlite3

from itertools import product

import numpy as np
import pandas as pd


if os.path.isdir('D:\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'
elif os.path.isdir('D:\\projects\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\projects\\ICLUS_v3\\population'
else:
    raise Exception


def parse_years(s):
    s = s.split('-')
    if int(s[0]) == 2095:
        return range(2095, 2100)

    return range(int(s[0]), int(s[1]))


def interpolate_years(df):
    scenarios = df.SCENARIO.unique()
    age_groups = df.AGE_GROUP.unique()
    sexes = ('MALE', 'FEMALE')

    df['MORT_INTERP'] = df['MORT_RATE_K']
    df.sort_values(by=['YEAR', 'AGE_GROUP', 'SCENARIO'], inplace=True)
    values_2020 = df.loc[df.YEAR == 2020]

    # rates are give over five years, so assign the middle year of each time
    # period the nominal rate and interplate between them
    df.loc[(~df.YEAR.astype(str).str.endswith('2')) &
           (~df.YEAR.astype(str).str.endswith('7')) &
           (df.YEAR != 2020), 'MORT_INTERP'] = np.nan

    for scenario, age_group, sex in product(scenarios, age_groups, sexes):
        s = df.loc[(df.SCENARIO == scenario) & (df.AGE_GROUP == age_group) & (df.SEX == sex), 'MORT_INTERP']
        df.loc[(df.SCENARIO == scenario) & (df.AGE_GROUP == age_group) & (df.SEX == sex), 'MORT_INTERP'] = s.interpolate(method='linear', limit_direction='both')
        value_2020 = values_2020.loc[(df.SCENARIO == scenario) & (df.AGE_GROUP == age_group) & (df.SEX == sex), 'MORT_INTERP'].values[0]
        df.loc[(df.SCENARIO == scenario) & (df.AGE_GROUP == age_group) & (df.SEX == sex), 'MORT_CHANGE_MULT'] = df['MORT_INTERP'] / value_2020

    assert not df.MORT_CHANGE_MULT.isnull().any(), "Mortality change multiplier is null!"
    return df


def combine_85plus(df):
    WEIGHT_MAP = {'85-89': 67.45,
                  '90-94': 32.05,
                  '95-99': 0.43,
                  '100+': 0.07}

    older = df.copy().query('AGE_GROUP == "85-89" | AGE_GROUP == "90-94" | AGE_GROUP == "95-99" | AGE_GROUP == "100+"')
    older.loc[:, 'WEIGHT'] = older['AGE_GROUP'].map(WEIGHT_MAP)
    older.loc[:, 'MORT_x_WEIGHT'] = older['MORT_CHANGE_MULT'] * older['WEIGHT']
    older.loc[:, 'NUMERATOR'] = older.groupby(by=['SCENARIO', 'YEAR', 'SEX'])['MORT_x_WEIGHT'].transform('sum')
    older.loc[:, 'DENOMENATOR'] = older.groupby(by=['SCENARIO', 'YEAR', 'SEX'])['WEIGHT'].transform('sum')
    older.loc[:, 'MORT_CHANGE_MULT_WEIGHTED'] = older.eval('NUMERATOR / DENOMENATOR')

    older = older[['YEAR', 'SCENARIO', 'SEX', 'MORT_CHANGE_MULT_WEIGHTED']]
    older = older.rename(columns={'MORT_CHANGE_MULT_WEIGHTED': 'MORT_CHANGE_MULT'})
    older.groupby(by=['YEAR', 'SCENARIO', 'SEX'])['MORT_CHANGE_MULT'].max()
    older['AGE_GROUP'] = "85+"
    older.drop_duplicates(inplace=True)

    df.query('AGE_GROUP != "85-89" & AGE_GROUP != "90-94" & AGE_GROUP != "95-99" & AGE_GROUP != "100+"', inplace=True)
    df = pd.concat(objs=[df, older], ignore_index=True)

    return df


def main():
    csv = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\Wittgenstein\\v3\\wcde_asmr.csv')
    df = pd.read_csv(filepath_or_buffer=csv, skiprows=8)
    df.columns = ['SCENARIO', 'AREA', 'PERIOD', 'AGE_GROUP', 'SEX', 'SURV_RATIO']
    df = df[['SCENARIO', 'PERIOD', 'AGE_GROUP', 'SEX', 'SURV_RATIO']]
    df['SEX'] = df['SEX'].str.upper()
    df['AGE_GROUP'] = df['AGE_GROUP'].str.replace('--', '-')
    df.query('AGE_GROUP != "Newborn"', inplace=True)

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

    # Some cohorts (mostly FEMALE) have a survival ratio of 1.0 in some
    # scenarios (i.e., mortality is 0). This doesn't make sense conceptually,
    # but it also creates numerical issues when calculating a change factor,
    # so we will set those survival ratios to 0.999 to ease the calculations.
    df.loc[df.SURV_RATIO == 1.0, 'SURV_RATIO'] = 0.999

    df['MORT_RATE_K'] = (1.0 - df.SURV_RATIO) * 1000.0
    df = interpolate_years(df)
    df = combine_85plus(df)

    df = df[['YEAR', 'SCENARIO', 'AGE_GROUP', 'SEX', 'SURV_RATIO', 'MORT_RATE_K', 'MORT_INTERP', 'MORT_CHANGE_MULT']]

    output_folder = os.path.join(BASE_FOLDER, 'inputs\\databases')
    con = sqlite3.connect(database=os.path.join(output_folder, 'wittgenstein.sqlite'))
    df.to_sql(name='age_specific_mortality_v3',
              if_exists='replace',
              con=con,
              index=False)
    con.close()

    print("Finished!")


if __name__ == '__main__':
    main()
