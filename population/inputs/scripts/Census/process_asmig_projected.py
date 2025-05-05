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
INPUT_FOLDER = os.path.join(ICLUS_FOLDER, 'population\\inputs\\raw_files\\Census\\2023\\projections\\immigration')

HISTORICAL_IMMIGRATION_MAP = {2010: 174416,
                              2011: 774165,
                              2012: 838671,
                              2013: 830168,
                              2014: 926546,
                              2015: 1041605,
                              2016: 1046709,
                              2017: 930409,
                              2018: 701823,
                              2019: 595348,
                              2020: 19885,
                              2021: 376004,
                              2022: 1693535,
                              2023: 2294209,
                              2024: 2786119}

AGE_GROUPS = ['0-4',
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
              '80-84',
              '85-100']

SCENARIOS = ['hi', 'mid', 'low', 'zero']

RACE_HISP_MAP = {1: 'WHITE',
                 2: 'BLACK',
                 3: 'AIAN',
                 4: 'ASIAN',
                 5: 'NHPI',
                 6: 'TWO_OR_MORE',
                 7: 'NON_HISPANIC',
                 8: 'HISPANIC',
                 9: 'NH_WHITE'}

def add_historical_immigration(df):
    '''
    Prepend historical immigration values for 2010-2024 using race proportions
    from 2023.

    The 2023 Census projections have projected immigration for 2023 and 2024,
    so we will overwrite those values with the historical values.
    '''
    for year in range(2010, 2025):
        hist_net_mig = HISTORICAL_IMMIGRATION_MAP[year]

        temp = df.query('YEAR == 2023')
        temp.loc[:, 'YEAR'] = year
        temp = temp.set_index(['YEAR', 'SEX', 'AGE_GROUP'])
        temp = temp.div(temp.sum().sum()) * hist_net_mig

        if year in (2023, 2024): # overwrite existing 2023 and 2024 values
            df = df.query(f'YEAR != {year}')
        df = pd.concat(objs=[df, temp.reset_index()], ignore_index=True)

    return df


def impute_projected_immigration(df, scenario):
    '''
    Detailed (i.e., age/race) immigration projections only exted to 2060.
    Append projections for the last 40 years using the total projected
    immigration (which IS available through 2100) and proportions from 2060.
    '''

    csv = os.path.join(INPUT_FOLDER, f'np2023_d4_{scenario}.csv')
    future_immigration = pd.read_csv(filepath_or_buffer=csv)
    future_immigration = future_immigration.query('RACE_HISP == 0 & SEX == 0')
    future_immigration = future_immigration[['YEAR', 'TOTAL_NIM']]

    for year in range(2061, 2100):
        future_total = future_immigration.query(f'YEAR == {year}')['TOTAL_NIM'].values[0]

        temp = df.query('YEAR == 2060')
        temp.loc[:, 'YEAR'] = year
        temp = temp.set_index(['YEAR', 'SEX', 'AGE_GROUP'])
        temp = temp.div(temp.sum().sum()) * future_total
        df = pd.concat(objs=[df, temp.reset_index()], ignore_index=True)

    return df


def main():
    for scenario in SCENARIOS:
        csv = os.path.join(INPUT_FOLDER, f'np2023_d4_{scenario}.csv')
        df = pd.read_csv(filepath_or_buffer=csv)
        df = df.query('RACE_HISP > 0 & SEX > 0').drop(columns='TOTAL_NIM')

        df['RACE_HISP'] = df['RACE_HISP'].map(RACE_HISP_MAP)
        df['SEX'] = df['SEX'].map({1: 'MALE', 2: 'FEMALE'})
        df = df.melt(id_vars=['RACE_HISP', 'SEX', 'YEAR'], var_name='AGE', value_name='NETMIG')
        df.loc[:, 'AGE'] = df.loc[:, 'AGE'].str.replace('NIM_', '').astype(int)
        df = df.pivot(columns='RACE_HISP', index=['YEAR', 'SEX', 'AGE'])
        df = df.droplevel(level=0, axis='columns')
        df.columns.name = None
        df['HISP_WHITE'] = df['WHITE'] - df['NH_WHITE']
        df.drop(columns=['WHITE', 'HISPANIC', 'NON_HISPANIC'], inplace=True)
        df.reset_index(inplace=True)

        for age_group in AGE_GROUPS:
            age1, age2 = age_group.split('-')
            df.loc[(df.AGE >= int(age1)) & (df.AGE <= int(age2)), 'AGE_GROUP'] = age_group

        df.drop(columns='AGE', inplace=True)
        df = df.groupby(['YEAR', 'SEX', 'AGE_GROUP'], as_index=False).sum()

        df = add_historical_immigration(df)
        df = impute_projected_immigration(df, scenario)
        df = df.sort_values(by=['YEAR', 'AGE_GROUP', 'SEX'])
        df['AGE_GROUP'] = df['AGE_GROUP'].str.replace('85-100', '85+')

        con = sqlite3.connect(database=os.path.join(DATABASES, 'census.sqlite'))
        df.to_sql(name=f'census_np2023_asmig_{scenario}',
                if_exists='replace',
                con=con,
                index=False)
        con.close()

    print("Finished!")


if __name__ == '__main__':
    main()
