"""
Author:  Phil Morefield
Purpose:
Created:
"""
import os
import sqlite3

import pandas as pd

FILE_PATH = 'D:\\OneDrive\\Dissertation\\data\\Census\\intercensal_population\\1990_to_1999'

DATABASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases'
MIGRATION_DB = os.path.join(DATABASE_FOLDER, 'MIGRATION.sqlite')
POPULATION_DB = os.path.join(DATABASE_FOLDER, 'POPULATION.sqlite')

AGE_GROUP_MAP = {0: 'LT_1',
                 1: '1_4',
                 2: '5_9',
                 3: '10_14',
                 4: '15_19',
                 5: '20_24',
                 6: '25_29',
                 7: '30_34',
                 8: '35_39',
                 9: '40_44',
                 10: '45_49',
                 11: '50_54',
                 12: '55_59',
                 13: '60_64',
                 14: '65_69',
                 15: '70_74',
                 16: '75_79',
                 17: '80_84',
                 18: '85_OVER'}

RACE_SEX_MAP = {1: 'WHITE MALE',
                2: 'WHITE FEMALE',
                3: 'BLACK MALE',
                4: 'BLACK FEMALE',
                5: 'AIAN MALE',
                6: 'AIAN FEMALE',
                7: 'API MALE',
                8: 'API FEMALE'}

ETHNICITY_MAP = {1: 'NOT_HISPANIC',
                 2: 'HISPANIC'}

COLUMNS = ['YEAR', 'COFIPS', 'AGE_GROUP', 'RACE_SEX', 'ETHNICITY', 'POPULATION']


def main():
    con = sqlite3.connect(MIGRATION_DB)
    fips_changes = pd.read_sql(sql='SELECT OLD_FIPS, NEW_FIPS FROM fips_or_name_changes',
                               con=con)
    valid_df = pd.read_sql(sql='SELECT * FROM valid_cyfips',
                           con=con)
    con.close()

    df = None

    for year in range(1990, 2000):
        file_name = f'stch-icen{year}.txt'
        csv = os.path.join(FILE_PATH, file_name)
        # temp = pd.read_csv(filepath_or_buffer=csv, sep='\s+', names=COLUMNS)
        temp = pd.read_csv(filepath_or_buffer=csv, names=COLUMNS)

        temp['YEAR'] = year
        temp['COFIPS'] = temp['COFIPS'].astype(int).astype(str).str.zfill(5)
        temp['AGE_GROUP'] = temp['AGE_GROUP'].map(AGE_GROUP_MAP)
        temp['RACE_SEX'] = temp['RACE_SEX'].map(RACE_SEX_MAP)
        temp['ETHNICITY'] = temp['ETHNICITY'].map(ETHNICITY_MAP)
        temp['RACE'] = temp['RACE_SEX'].str.split().str[0]
        temp['SEX'] = temp['RACE_SEX'].str.split().str[1]
        temp = temp.drop(columns=['RACE_SEX'])

        assert not temp.isnull().any().any()

        # update using FIPS_CHANGES
        temp = temp.merge(right=fips_changes,
                          how='left',
                          left_on='COFIPS',
                          right_on='OLD_FIPS',
                          copy=False)
        temp.loc[~pd.isnull(temp['NEW_FIPS']), 'COFIPS'] = temp['NEW_FIPS']
        temp.drop(labels=['OLD_FIPS', 'NEW_FIPS'], axis=1, inplace=True)
        temp = temp.groupby(by=['YEAR', 'COFIPS', 'RACE', 'ETHNICITY', 'AGE_GROUP', 'SEX'], as_index=False).sum()
        temp = temp[temp.COFIPS.isin(valid_df.CYFIPS.to_list())]

        if df is None:
            df = temp.copy()
        else:
            df = pd.concat([df, temp], axis=0, ignore_index=True)

    con = sqlite3.connect(POPULATION_DB)
    df.to_sql(name='county_population_ageracegender_1990_to_1999',
              con=con,
              if_exists='replace',
              index=False)
    con.close()


if __name__ == '__main__':
    main()
