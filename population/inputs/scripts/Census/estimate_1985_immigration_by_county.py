'''
Prepare ZINB regression inputs for Census 1990 data.
'''
import os
import sqlite3

import numpy as np
import pandas as pd

from matplotlib import pyplot as plt

BASE_FOLDER = 'D:\\projects\\ICLUS_v3\\population'
if os.path.isdir('D:\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'

DATABASE_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\databases')
MIGRATION_DB = os.path.join(DATABASE_FOLDER, 'migration.sqlite')
POPULATION_DB = os.path.join(DATABASE_FOLDER, 'population.sqlite')
ANALYSIS_DB = os.path.join(DATABASE_FOLDER, 'analysis.sqlite')
CENSUS_CSV_PATH = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\Census')


def make_fips_changes(df):
    con =sqlite3.connect(MIGRATION_DB)
    query = 'SELECT OLD_FIPS AS COFIPS, NEW_FIPS \
             FROM fips_or_name_changes'
    df_fips = pd.read_sql_query(sql=query, con=con)
    con.close()

    df = df.merge(right=df_fips,
                  how='left',
                  on='COFIPS')

    df.loc[~df.NEW_FIPS.isnull(), 'COFIPS'] = df['NEW_FIPS']
    df = df.drop(columns='NEW_FIPS')
    df = df.groupby(by='COFIPS', as_index=False).sum()

    return df



def get_1990_census_immigration():
    # historical immigration, 1990
    txt = os.path.join(CENSUS_CSV_PATH, '1990\\intercensal\\99c8_00_immigration.txt')
    imm90 = pd.read_fwf(txt, header=None, skiprows=4, encoding='latin-1')
    columns = ['BLOCK', 'COFIPS'] + [f'NETIMM{year}' for year in range(1999, 1989, -1)] + ['NETIMM1990APRIL', 'CYNAME']
    imm90.columns = columns
    imm90 = imm90[['COFIPS', 'NETIMM1990']]
    imm90['COFIPS'] = imm90['COFIPS'].astype(str).str.zfill(5)
    imm90['NETIMM1990'] = imm90['NETIMM1990'].str.replace(',', '').astype(int)
    imm90 = make_fips_changes(imm90)

    # historical migration, 1990
    txt = os.path.join(CENSUS_CSV_PATH, '1990\\intercensal\\99c8_00_migration.txt')
    mig90 = pd.read_fwf(txt, header=None, skiprows=3, encoding='latin-1')
    columns = ['BLOCK', 'COFIPS'] + [f'NETMIG{year}' for year in range(1999, 1989, -1)] + ['NETMIG1990APRIL', 'CYNAME']
    mig90.columns = columns
    mig90 = mig90[['COFIPS', 'NETMIG1990']]
    mig90['COFIPS'] = mig90['COFIPS'].astype(str).str.zfill(5)
    mig90['NETMIG1990'] = mig90['NETMIG1990'].str.replace(',', '').astype(int)
    mig90 = make_fips_changes(mig90)

    df = pd.merge(left=imm90,
                  right=mig90,
                  how='outer',
                  on='COFIPS')

    df = df.loc[~df['COFIPS'].str.startswith('000'), :]

    df['NETIMM1990'] = np.log(df['NETIMM1990'].fillna(0).astype(int) + 1)
    df['NETMIG1990'] = np.log(df['NETMIG1990'].fillna(0).astype(int) + 1)

    return df


def main():
    df = get_1990_census_immigration()



if __name__ == '__main__':
    main()
