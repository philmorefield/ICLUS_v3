"""
Author:  Phil Morefield
Purpose:
Created:
"""
import os
import sqlite3

import pandas as pd

CENSUS_FOLDER = 'D:\\OneDrive\\Dissertation\\data\\Census\\intercensal_population'
MIGRATION_DB = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases\\migration.sqlite'
POPULATION_DB = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases\\population.sqlite'


def main():
    query = 'SELECT OLD_FIPS, NEW_FIPS FROM fips_or_name_changes'
    con = sqlite3.connect(MIGRATION_DB)
    fips_changes = pd.read_sql(sql=query, con=con)
    con.close()

    csv = os.path.join(CENSUS_FOLDER, '2000_to_2010', 'co-est00int-tot.csv')
    df = pd.read_csv(filepath_or_buffer=csv, encoding='latin1')

    df['COFIPS'] = df['STATE'].astype(str).str.zfill(2) + df['COUNTY'].astype(str).str.zfill(3)
    df.drop(columns=['STATE', 'COUNTY'], inplace=True)
    columns = ['COFIPS', 'ESTIMATESBASE2000', 'POPESTIMATE2000'] + [f'POPESTIMATE{year}' for year in range(2001, 2011)]
    df = df[columns]

    df = df.merge(right=fips_changes, how='left', left_on='COFIPS', right_on='OLD_FIPS', copy=False)
    df.loc[~pd.isnull(df['NEW_FIPS']), 'COFIPS'] = df['NEW_FIPS']
    df.drop(columns=['OLD_FIPS', 'NEW_FIPS'], inplace=True)
    df = df.groupby(by='COFIPS', as_index=False).sum()

    for col in df.columns:
        if 'POPESTIMATE' in col:
            df.rename(columns={col: col[-4:]}, inplace=True)

    con = sqlite3.connect(POPULATION_DB)
    df.to_sql(name='county_population_2000_to_2010',
              con=con,
              if_exists='replace',
              index=False)
    con.close()


if __name__ == '__main__':
    main()
