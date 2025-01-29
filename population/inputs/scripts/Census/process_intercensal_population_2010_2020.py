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

    csv = os.path.join(CENSUS_FOLDER, '2010_to_2020', 'co-est2020-alldata.csv')
    df = pd.read_csv(filepath_or_buffer=csv, encoding='latin1')

    df['COFIPS'] = df['STATE'].astype(str).str.zfill(2) + df['COUNTY'].astype(str).str.zfill(3)
    df.drop(columns=['STATE', 'COUNTY'], inplace=True)
    columns = ['COFIPS', 'CENSUS2010POP', 'ESTIMATESBASE2010', 'POPESTIMATE2010'] + [f'POPESTIMATE{year}' for year in range(2011, 2021)]
    df = df[columns]

    df = df.merge(right=fips_changes, how='left', left_on='COFIPS', right_on='OLD_FIPS', copy=False)
    df.loc[~pd.isnull(df['NEW_FIPS']), 'COFIPS'] = df['NEW_FIPS']
    df.drop(columns=['OLD_FIPS', 'NEW_FIPS'], inplace=True)
    df = df.groupby(by='COFIPS', as_index=False).sum()

    for col in df.columns:
        if 'POPESTIMATE' in col:
            df.rename(columns={col: col[-4:]}, inplace=True)

    con = sqlite3.connect(POPULATION_DB)
    df.to_sql(name='county_population_2010_to_2020',
              con=con,
              if_exists='replace',
              index=False)
    con.close()


if __name__ == '__main__':
    main()
