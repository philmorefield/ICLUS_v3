# -*- coding: utf-8 -*-
import sqlite3

import pandas as pd


CSV = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\raw_files\\Census\\2023\\projections\\np2023_a2.csv'
OUTPUT_DB = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases\\census.sqlite'

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


def main():
    df = pd.read_csv(filepath_or_buffer=CSV)
    df.query('NATIVITY == 0 & GROUP == 0', inplace=True)
    df.drop(columns='GROUP', inplace=True)

    df = df.melt(id_vars='YEAR', var_name='AGE', value_name='MORT')
    df.loc[:, 'AGE'] = df.loc[:, 'AGE'].str.replace('ASMR_', '').astype(int)
    df = df.sort_values(by=['YEAR', 'AGE'])

    for age_group in AGE_GROUPS:
        age1, age2 = age_group.split('-')
        df.loc[(df.AGE >= int(age1)) & (df.AGE <= int(age2)), 'AGE_GROUP'] = age_group

    df.drop(columns='AGE', inplace=True)
    df = df.groupby(['YEAR', 'AGE_GROUP'], as_index=False).mean()

    for age_group in AGE_GROUPS:
        temp2020 = df.loc[(df.YEAR == 2023) & (df.AGE_GROUP == age_group)]
        temp2020.loc[:, 'YEAR'] = 2020

        temp2021 = df.loc[(df.YEAR == 2023) & (df.AGE_GROUP == age_group)]
        temp2021.loc[:, 'YEAR'] = 2021

        temp2022 = df.loc[(df.YEAR == 2023) & (df.AGE_GROUP == age_group)]
        temp2022.loc[:, 'YEAR'] = 2022

        df = pd.concat(objs=[temp2020, temp2021, temp2022, df], ignore_index=True)

    df.sort_values(by=['AGE_GROUP', 'YEAR'], inplace=True)

    for age_group in AGE_GROUPS:
        value2020 = df.loc[(df.YEAR == 2020) & (df.AGE_GROUP == age_group), 'MORT'].values[0]
        df.loc[df.AGE_GROUP == age_group, 'MORT_MULTIPLIER'] = df.loc[df.AGE_GROUP == age_group, 'MORT'] / value2020

    con = sqlite3.connect(database=OUTPUT_DB)
    df.to_sql(name='census_np2023_asmr',
              con=con,
              if_exists='replace',
              index=False)
    con.close()


if __name__ == '__main__':
    main()
