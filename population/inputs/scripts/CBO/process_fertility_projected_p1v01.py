import os
# import sqlite3

import pandas as pd


BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'

CBO_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\CBO')
CSV_FOLDER = '57059-2025-09-Demographic-Projections\\CSV files'
CSV_FILE = 'fertilityRates_byYearAgePlace.csv'
DATABASE_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\databases')
# OUTPUT_DB = os.path.join(BASE_FOLDER, 'inputs\\databases\\cbo.sqlite')


def main():
    df = pd.read_csv(filepath_or_buffer=os.path.join(CBO_FOLDER, CSV_FOLDER, CSV_FILE))
    df.columns = ['YEAR', 'AGE', 'PLACE', 'ASFR']
    df = df.query('PLACE == "all" & AGE >= 14 & AGE <= 49').drop(columns='PLACE')

    # CBO ASFR starts at 2025; use that as the baseline average, i.e., the
    # change factor for 2025 will be 1.0
    df_asfr_base = df.loc[df.YEAR == 2025].drop(columns='YEAR')
    df_asfr_base = df_asfr_base.groupby(by='AGE', as_index=False).mean()
    df_asfr_base = df_asfr_base.rename(columns={'ASFR': 'ASFR_BASE'})

    # starting with 2025, calculate the % change from the 2019-2023 average ASFR
    df = df.query('YEAR >= 2025').pivot_table(index='AGE', columns='YEAR')
    df.columns.name = None
    df.columns = df.columns.droplevel(0)
    df.columns = [f'ASFR_{col}' for col in df.columns]
    df = df.merge(df_asfr_base, on='AGE', how='left')

    for year in range(2025, 2099):
        df[f'ASFR_{year}'] = df[f'ASFR_{year}'] / df['ASFR_BASE']
    df = df.drop(columns='ASFR_BASE')

    # con = sqlite3.connect(database=OUTPUT_DB)
    # df.to_sql(name='cbo_fertility',
    #           con=con,
    #           if_exists='replace',
    #           index=False)
    # con.close()

    df.to_csv(os.path.join(DATABASE_FOLDER, 'cbo_fertility_p1v01.csv'),
              index=False)


if __name__ == '__main__':
    main()
