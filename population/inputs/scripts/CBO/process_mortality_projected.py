import os
import sqlite3

import pandas as pd

CBO_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\raw_files\\CBO'
CSV_FOLDER = 'demographic_projections_2025_9\\CSV files'
CSV_FILE = 'mortalityRates_byYearAgeSex.csv'
OUTPUT_DB = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases\\cbo.sqlite'

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
    df = pd.read_csv(filepath_or_buffer=os.path.join(CBO_FOLDER, CSV_FOLDER, CSV_FILE))
    df.columns = ['YEAR', 'AGE', 'SEX', 'ASMR']
    df.SEX = df.SEX.str.upper()

    # bin rows by age group
    df['AGE_GROUP'] = '0-4'
    df.loc[df.AGE >= 85, 'AGE_GROUP'] = '85+'
    df.loc[df.AGE.between(80, 84), 'AGE_GROUP'] = '80-84'
    df.loc[df.AGE.between(75, 79), 'AGE_GROUP'] = '75-79'
    df.loc[df.AGE.between(70, 74), 'AGE_GROUP'] = '70-74'
    df.loc[df.AGE.between(65, 69), 'AGE_GROUP'] = '65-69'
    df.loc[df.AGE.between(60, 64), 'AGE_GROUP'] = '60-64'
    df.loc[df.AGE.between(55, 59), 'AGE_GROUP'] = '55-59'
    df.loc[df.AGE.between(50, 54), 'AGE_GROUP'] = '50-54'
    df.loc[df.AGE.between(45, 49), 'AGE_GROUP'] = '45-49'
    df.loc[df.AGE.between(40, 44), 'AGE_GROUP'] = '40-44'
    df.loc[df.AGE.between(35, 39), 'AGE_GROUP'] = '35-39'
    df.loc[df.AGE.between(30, 34), 'AGE_GROUP'] = '30-34'
    df.loc[df.AGE.between(25, 29), 'AGE_GROUP'] = '25-29'
    df.loc[df.AGE.between(20, 24), 'AGE_GROUP'] = '20-24'
    df.loc[df.AGE.between(15, 19), 'AGE_GROUP'] = '15-19'
    df.loc[df.AGE.between(10, 14), 'AGE_GROUP'] = '10-14'
    df.loc[df.AGE.between(5, 9), 'AGE_GROUP'] = '5-9'

    df = df.drop(columns='AGE')
    df = df.groupby(by=['YEAR', 'AGE_GROUP', 'SEX'], as_index=False).mean()

    # calculate the 2021-2024 average ASMR by age and sex
    df_asmr_base = df.loc[df.YEAR.isin([2021, 2022, 2023, 2024])].drop(columns='YEAR')
    df_asmr_base['ASMR_BASE'] = df_asmr_base.groupby(['AGE_GROUP', 'SEX'])['ASMR'].transform('mean')
    df_asmr_base = df_asmr_base.drop(columns='ASMR')

    # starting with 2025, calculate the % change from the 2019-2023 average ASMR
    df = df.query('YEAR >= 2025').pivot_table(index=['AGE_GROUP', 'SEX'], columns='YEAR')
    df.columns.name = None
    df.columns = df.columns.droplevel(0)
    df.columns = [f'ASDR_{col}' for col in df.columns]
    df = df.merge(df_asmr_base, on=['AGE_GROUP', 'SEX'], how='left')

    for year in range(2025, 2099):
        df[f'ASDR_{year}'] = df[f'ASDR_{year}'] / df['ASMR_BASE']
    df = df.drop(columns='ASMR_BASE')

    con = sqlite3.connect(database=OUTPUT_DB)
    df.to_sql(name='cbo_mortality',
              con=con,
              if_exists='replace',
              index=False)
    con.close()


if __name__ == '__main__':
    main()
