'''
Processes gross migration flows from IRS for tax years 2019/20.

'''

import glob
from os import path
import sqlite3

import pandas as pd

names = ['ORIGIN_STFIPS',
         'ORIGIN_COFIPS',
         'DESTINATION_STFIPS',
         'DESTINATION_COFIPS',
         'DESTINATION_STATE',
         'DESTINATION_NAME',
         'RETURNS',
         'EXEMPTIONS',
         'AGGREGATE_AGI']

db_columns = ['ORIGIN_FIPS',
              'ORIGIN_NAME',
              'ORIGIN_STATE',
              'DESTINATION_FIPS',
              'DESTINATION_NAME',
              'DESTINATION_STATE',
              'EXEMPTIONS',
              'RETURNS',
              'YEAR',
              'AGGREGATE_AGI']

con = sqlite3.connect('D:\\OneDrive\\Dissertation\\databases\\migration.sqlite')
# c = con.cursor()

for origin_year in range(2019, 2020):
    # c.execute('DELETE FROM irs_county_to_county_raw WHERE year = {}'.format(origin_year))
    destination_year = origin_year + 1
    data_dir = f'D:\\OneDrive\\Dissertation\\data\\IRS\\{origin_year}_{destination_year}'
    o_year = str(origin_year)[-2:]
    d_year = str(destination_year)[-2:]
    raw_files = glob.glob(path.join(data_dir, f'countyoutflow{o_year}{d_year}.csv'))

    for raw_file in raw_files:
        df = pd.read_csv(filepath_or_buffer=raw_file,
                         names=names,
                         skiprows=1,
                         encoding='latin-1')

        # these appear to be suppressed values
        if df['RETURNS'].dtype == 'object':
            df = df[df['RETURNS'] != 'd']
            df['RETURNS'] = df['RETURNS'].astype('str').str.replace('r', '')
        if df['EXEMPTIONS'].dtype == 'object':
            df = df[df['EXEMPTIONS'] != 'd']
            df['EXEMPTIONS'] = df['EXEMPTIONS'].astype('str').str.replace('r', '')
        if df['AGGREGATE_AGI'].dtype == 'object':
            df = df[df['AGGREGATE_AGI'] != 'd']
            df['AGGREGATE_AGI'] = df['AGGREGATE_AGI'].astype('str').str.replace('r', '')

        df['ORIGIN_STFIPS'] = df['ORIGIN_STFIPS'].astype('str').str.zfill(2)
        df['ORIGIN_COFIPS'] = df['ORIGIN_COFIPS'].astype('str').str.zfill(3)
        df['ORIGIN_FIPS'] = df['ORIGIN_STFIPS'] + df['ORIGIN_COFIPS']
        df.drop(labels=['ORIGIN_STFIPS', 'ORIGIN_COFIPS'], axis=1, inplace=True)

        df['DESTINATION_STFIPS'] = df['DESTINATION_STFIPS'].astype('str').str.zfill(2)
        df['DESTINATION_COFIPS'] = df['DESTINATION_COFIPS'].astype('str').str.zfill(3)
        df['DESTINATION_FIPS'] = df['DESTINATION_STFIPS'] + df['DESTINATION_COFIPS']
        df['DESTINATION_STATE'] = df['DESTINATION_STATE'].str.upper()
        df.drop(labels=['DESTINATION_STFIPS', 'DESTINATION_COFIPS'], axis=1, inplace=True)

        df['ORIGIN_YEAR'] = origin_year
        df['DESTINATION_YEAR'] = destination_year
        df['AGGREGATE_AGI'] = df['AGGREGATE_AGI'] * 1000
        df['ORIGIN_NAME'] = ''
        df['ORIGIN_STATE'] = ''

        df.to_sql(name='irs_raw_import',
                  con=con,
                  if_exists='append',
                  index=False)

    print(f"Done with {origin_year}...")
# c.close()
