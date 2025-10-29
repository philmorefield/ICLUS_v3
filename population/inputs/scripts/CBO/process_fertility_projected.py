import os
import sqlite3

import pandas as pd


BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'

CBO_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\CBO')
CSV_FOLDER = 'demographic_projections_2025_9\\CSV files'
CSV_FILE = 'fertilityRates_byYearAgePlace.csv'
OUTPUT_DB = os.path.join(BASE_FOLDER, 'inputs\\databases\\cbo.sqlite')

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
    df.columns = ['YEAR', 'AGE', 'PLACE', 'ASFR']
    df = df.query('PLACE == "all" & AGE >= 14').drop(columns='PLACE')


    # bin rows by age group
    df['AGE_GROUP'] = '15-19'
    df.loc[df.AGE.between(45, 49), 'AGE_GROUP'] = '45-49'
    df.loc[df.AGE.between(40, 44), 'AGE_GROUP'] = '40-44'
    df.loc[df.AGE.between(35, 39), 'AGE_GROUP'] = '35-39'
    df.loc[df.AGE.between(30, 34), 'AGE_GROUP'] = '30-34'
    df.loc[df.AGE.between(25, 29), 'AGE_GROUP'] = '25-29'
    df.loc[df.AGE.between(20, 24), 'AGE_GROUP'] = '20-24'

    df = df.drop(columns='AGE')
    df = df.groupby(by=['YEAR', 'AGE_GROUP'], as_index=False).mean()

    # CBO ASFR starts at 2025; use that as the baseline average, i.e., the
    # change factor for 2025 will be 1.0
    df_asfr_base = df.loc[df.YEAR == 2025].drop(columns='YEAR')
    df_asfr_base = df_asfr_base.groupby(by='AGE_GROUP', as_index=False).mean()
    df_asfr_base = df_asfr_base.rename(columns={'ASFR': 'ASFR_BASE'})

    # starting with 2025, calculate the % change from the 2019-2023 average ASFR
    df = df.query('YEAR >= 2025').pivot_table(index='AGE_GROUP', columns='YEAR')
    df.columns.name = None
    df.columns = df.columns.droplevel(0)
    df.columns = [f'ASFR_{col}' for col in df.columns]
    df = df.merge(df_asfr_base, on='AGE_GROUP', how='left')

    for year in range(2025, 2099):
        df[f'ASFR_{year}'] = df[f'ASFR_{year}'] / df['ASFR_BASE']
    df = df.drop(columns='ASFR_BASE')

    con = sqlite3.connect(database=OUTPUT_DB)
    df.to_sql(name='cbo_fertility',
              con=con,
              if_exists='replace',
              index=False)
    con.close()


if __name__ == '__main__':
    main()
