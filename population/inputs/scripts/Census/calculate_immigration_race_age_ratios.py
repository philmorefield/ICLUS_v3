import os
import sqlite3

import pandas as pd


if os.path.exists('D:\\OneDrive\\ICLUS_v3'):
    ICLUS_FOLDER = 'D:\\OneDrive\\ICLUS_v3'
else:
    ICLUS_FOLDER = 'D:\\projects\\ICLUS_v3'

DATABASES = os.path.join(ICLUS_FOLDER, 'population\\inputs\\databases')

def main():

    race_map = {2: 'BLACK',
                3: 'AIAN',
                4: 'ASIAN',
                5: 'NHPI',
                6: 'TWO_OR_MORE',
                9: 'NH_WHITE',
                10: 'HISP_WHITE'}

    sex_map = {1: 'MALE',
               2: 'FEMALE'}

    for scenario in ('low', 'mid', 'high'):
        p = os.path.join(ICLUS_FOLDER, 'population\\inputs\\raw_files\\Census\\2017\\projections')
        f = f'np2017_d4_{scenario}.csv'

        df = pd.read_csv(filepath_or_buffer=os.path.join(p, f))
        df.query('SEX > 0 & RACE_HISP > 0', inplace=True)
        df.drop(columns='TOTAL_NIM', inplace=True)

        nhwhite = df.query('RACE_HISP == 9')
        nhwhite = nhwhite.drop(columns='RACE_HISP').set_index(keys=['SEX', 'YEAR'])

        white = df.query('RACE_HISP == 1')
        white = white.drop(columns='RACE_HISP').set_index(keys=['SEX', 'YEAR'])

        hisp_white = white.sub(other=nhwhite).reset_index()
        hisp_white['RACE_HISP'] = 10

        df = df.loc[~df.RACE_HISP.isin((1, 7, 8))]
        df = pd.concat(objs=[df, hisp_white], ignore_index=True)
        df['RACE_HISP'] = df['RACE_HISP'].map(race_map)
        df['SEX'] = df['SEX'].map(sex_map)
        df.set_index(keys=['YEAR', 'RACE_HISP', 'SEX'], inplace=True)
        df = df.melt(var_name='AGE', value_name='NIM', ignore_index=False)
        df['AGE'] = df['AGE'].str.replace('NIM_', '').astype(int)

        df['AGE_GROUP'] = 0
        df.loc[df.AGE <= 4, 'AGE_GROUP'] = '0-4'
        df.loc[(df.AGE >= 5) & (df.AGE <= 9), 'AGE_GROUP'] = '5-9'
        df.loc[(df.AGE >= 10) & (df.AGE <= 14), 'AGE_GROUP'] = '10-14'
        df.loc[(df.AGE >= 15) & (df.AGE <= 19), 'AGE_GROUP'] = '15-19'
        df.loc[(df.AGE >= 20) & (df.AGE <= 24), 'AGE_GROUP'] = '20-24'
        df.loc[(df.AGE >= 25) & (df.AGE <= 29), 'AGE_GROUP'] = '25-29'
        df.loc[(df.AGE >= 30) & (df.AGE <= 34), 'AGE_GROUP'] = '30-34'
        df.loc[(df.AGE >= 35) & (df.AGE <= 39), 'AGE_GROUP'] = '35-39'
        df.loc[(df.AGE >= 40) & (df.AGE <= 44), 'AGE_GROUP'] = '40-44'
        df.loc[(df.AGE >= 45) & (df.AGE <= 49), 'AGE_GROUP'] = '45-49'
        df.loc[(df.AGE >= 50) & (df.AGE <= 54), 'AGE_GROUP'] = '50-54'
        df.loc[(df.AGE >= 55) & (df.AGE <= 59), 'AGE_GROUP'] = '55-59'
        df.loc[(df.AGE >= 60) & (df.AGE <= 64), 'AGE_GROUP'] = '60-64'
        df.loc[(df.AGE >= 65) & (df.AGE <= 69), 'AGE_GROUP'] = '65-69'
        df.loc[(df.AGE >= 70) & (df.AGE <= 74), 'AGE_GROUP'] = '70-74'
        df.loc[(df.AGE >= 75) & (df.AGE <= 79), 'AGE_GROUP'] = '75-79'
        df.loc[(df.AGE >= 80) & (df.AGE <= 84), 'AGE_GROUP'] = '80-84'
        df.loc[df.AGE >= 85, 'AGE_GROUP'] = '85+'

        df = df.drop(columns='AGE').reset_index()
        df = df.groupby(by=['YEAR', 'RACE_HISP', 'SEX', 'AGE_GROUP'], as_index=False).sum()
        df['ANN_TOTAL_NIM'] = df.groupby(by=['YEAR', 'AGE_GROUP', 'SEX'])['NIM'].transform(sum)
        df['ANN_NIM_FRAC'] = df['NIM'] / df['ANN_TOTAL_NIM']

        df = df[['YEAR', 'RACE_HISP', 'SEX', 'AGE_GROUP', 'ANN_NIM_FRAC']]

        for year in (2015, 2016):
            new = df.query('YEAR == 2017')
            new['YEAR'] = year
            df = pd.concat(objs=[df, new], ignore_index=True)

        for year in range(2061, 2101):
            new = df.query('YEAR == 2060')
            new['YEAR'] = year
            df = pd.concat(objs=[df, new], ignore_index=True)

        df.sort_values(by=['YEAR', 'AGE_GROUP', 'SEX'], inplace=True)
        df = df.pivot(index=['YEAR', 'SEX', 'AGE_GROUP'], columns='RACE_HISP')
        df.columns = df.columns.droplevel(0)
        df.columns.name = None
        df.reset_index(inplace=True)

        db = os.path.join(DATABASES, 'census.sqlite')
        con = sqlite3.connect(db)

        df.to_sql(name=f'annual_immigration_fraction_{scenario}',
                  con=con,
                  if_exists='replace',
                  index=False)
        con.close()


if __name__ == '__main__':
    main()
