"""
Author:  Phil Morefield
Purpose:
Created:
"""
import os
import sqlite3

import pandas as pd

FILE_PATH = 'D:\\OneDrive\\Dissertation\\data\\Census\\intercensal_population\\1980_to_1989'
FILE_NAME = 'pe-02.csv'

DATABASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases'
MIGRATION_DB = os.path.join(DATABASE_FOLDER, 'MIGRATION.sqlite')
POPULATION_DB = os.path.join(DATABASE_FOLDER, 'POPULATION.sqlite')

NEW_COLS = ('YEAR', 'COFIPS', 'RACE_SEX', 'LT_5', '5_9', '10_14', '15_19', '20_24',
            '25_29', '30_34', '35_39', '40_44', '45_49', '50_54', '55_59', '60_64',
            '65_69', '70_74', '75_79', '80_84', '85_OVER')

COLUMN_NAME_MAP = {'Year of Estimate': 'YEAR',
                   'FIPS State and County Codes': 'COFIPS',
                   'Race/Sex Indicator': 'RACE_SEX'}


def main():
    con = sqlite3.connect(MIGRATION_DB)
    fips_changes = pd.read_sql(sql='SELECT OLD_FIPS, NEW_FIPS FROM fips_or_name_changes',
                               con=con)
    valid_df = pd.read_sql(sql='SELECT * FROM valid_cyfips',
                           con=con)
    con.close()

    csv = os.path.join(FILE_PATH, FILE_NAME)
    df = pd.read_csv(filepath_or_buffer=csv, skiprows=5, encoding='latin1')
    df.dropna(axis=0, how='any', inplace=True)

    df.rename(COLUMN_NAME_MAP, axis=1, inplace=True)

    df['COFIPS'] = df['COFIPS'].astype(int).astype(str).str.zfill(5)
    df['YEAR'] = df['YEAR'].astype(int)
    df.iloc[:, 3:] = df.iloc[:, 3:].astype(int)
    df.columns = NEW_COLS
    df = df.melt(id_vars=['YEAR', 'COFIPS', 'RACE_SEX'],
                 var_name='AGE_GROUP',
                 value_name='POPULATION')
    df['POPULATION'] = df['POPULATION'].astype(int)

    assert not df.isnull().any().any()

    # update using FIPS_CHANGES
    df = df.merge(right=fips_changes, how='left', left_on='COFIPS', right_on='OLD_FIPS', copy=False)
    df.loc[~pd.isnull(df['NEW_FIPS']), 'COFIPS'] = df['NEW_FIPS']
    df.drop(labels=['OLD_FIPS', 'NEW_FIPS'], axis=1, inplace=True)
    df = df.groupby(by=['YEAR', 'COFIPS', 'RACE_SEX', 'AGE_GROUP'], as_index=False).sum()

    df = df[df.COFIPS.isin(valid_df.CYFIPS.to_list())]

    df['RACE'] = 'OTHER'
    df['SEX'] = 'MALE'

    df.loc[df.RACE_SEX.str.contains('Black'), 'RACE'] = 'BLACK'
    df.loc[df.RACE_SEX.str.contains('White'), 'RACE'] = 'WHITE'
    df.loc[df.RACE_SEX.str.contains('female'), 'SEX'] = 'FEMALE'
    df = df[['COFIPS', 'YEAR', 'RACE', 'SEX', 'AGE_GROUP', 'POPULATION']]

    df = df.set_index(['COFIPS', 'YEAR', 'RACE', 'SEX', 'AGE_GROUP'])
    df = df.unstack(level='YEAR', fill_value=0)
    df = df.droplevel(level=0, axis=1)
    df.columns.name = None
    df.reset_index(inplace=True)

    con = sqlite3.connect(POPULATION_DB)
    df.to_sql(name='county_population_ageracegender_1980_to_1989',
              con=con,
              if_exists='replace',
              index=False)
    con.close()


if __name__ == '__main__':
    main()
