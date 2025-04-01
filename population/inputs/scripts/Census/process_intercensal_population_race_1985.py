'''
This script estimates the 1985 population by county and race by averaging the
1980 and 1990 decennial census populations.
'''

import os
import sqlite3

import pandas as pd

if os.path.isdir('D:\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'
elif os.path.isdir('D:\\projects\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\projects\\ICLUS_v3\\population'
else:
    raise Exception("BASE_FOLDER not found!")

CENSUS_FOLDER = os.path.join(BASE_FOLDER, 'inputs', 'raw_files', 'Census')
POPULATION_DB = os.path.join(BASE_FOLDER, 'inputs', 'databases', 'population.sqlite')
INPUT_FOLDER = os.path.join(BASE_FOLDER, 'inputs')
MIG_DB = os.path.join(INPUT_FOLDER, 'databases', 'migration.sqlite')
OUTPUT_FOLDER = os.path.join(BASE_FOLDER, 'outputs')


def update_cofips(df):
    con = sqlite3.connect(database=MIG_DB)
    query = 'SELECT OLD_FIPS, NEW_FIPS \
             FROM fips_or_name_changes'
    df_fips = pd.read_sql(sql=query, con=con)
    con.close()

    df = df.merge(right=df_fips,
                  how='left',
                  left_on='COFIPS',
                  right_on='OLD_FIPS')
    df.loc[~df.NEW_FIPS.isnull(), 'COFIPS'] = df.NEW_FIPS
    df.drop(columns=['OLD_FIPS', 'NEW_FIPS'], inplace=True)

    df = df.groupby(by=['COFIPS'], as_index=False).sum()

    return df

def get_1980_population():
    usecols = ['State Code',
               'County Code',
               'White',
               'Black',
               'American Indian, Eskimo, and Aleut',
               'Asian and Pacific Islander',
               'Other races']

    names = ['STFIPS', 'COFIPS', 'WHITE', 'BLACK', 'AIAN', 'API', 'OTHER']
    csv = os.path.join(CENSUS_FOLDER, '1980', 'nhgis0014_ds116_1980_county.csv')
    df = pd.read_csv(filepath_or_buffer=csv, usecols=usecols, skiprows=1, encoding='latin1')
    df.columns = names

    df['STFIPS'] = df['STFIPS'].astype(str).str.zfill(2)
    df['COFIPS'] = df['COFIPS'].astype(str).str.zfill(3)
    df['COFIPS' ] = df['STFIPS'] + df['COFIPS']
    df = df.drop(columns=['STFIPS'])

    df = update_cofips(df)
    df = df.melt(id_vars='COFIPS', var_name='RACE', value_name='POP1980')

    return df

def get_1990_population():
    usecols = ['State Code',
               'County Code',
               'White',
               'Black',
               'American Indian, Eskimo, or Aleut',
               'Asian or Pacific Islander',
               'Other race']

    names = ['COFIPS', 'STFIPS', 'WHITE', 'BLACK', 'AIAN', 'API', 'OTHER']
    csv = os.path.join(CENSUS_FOLDER, '1990', 'nhgis0015_ds120_1990_county.csv')
    df = pd.read_csv(filepath_or_buffer=csv, usecols=usecols, skiprows=1, encoding='latin1')
    df.columns = names

    df['STFIPS'] = df['STFIPS'].astype(str).str.zfill(2)
    df['COFIPS'] = df['COFIPS'].astype(str).str.zfill(3)
    df['COFIPS' ] = df['STFIPS'] + df['COFIPS']
    df = df.drop(columns=['STFIPS'])

    df = update_cofips(df)
    df = df.melt(id_vars='COFIPS', var_name='RACE', value_name='POP1990')

    return df

def estimate_1985_population(df80, df90):
    ''''
    This function estimates the 1985 population by taking the average of the
    1980 and 1990 populations. There are a handful of 1990 counties that didn't
    exist in 1980, which I'm simply excluding since these data will be used for
    estimating regression coefficients and there will still be millions of
    observations in the dataset.

    To see which counties are missing, use an outer join when merging:

    df = df80.merge(right=df90, how='outer', on=['COFIPS', 'RACE'])
    '''''

    df = df80.merge(right=df90, how='inner', on=['COFIPS', 'RACE'])
    df['POP1985'] = (df['POP1980'] + df['POP1990']) / 2.0
    df['POP1985'] = df['POP1985'].round().astype(int)
    df = df[['COFIPS', 'RACE', 'POP1985']]

    df = df.pivot(columns='RACE', index='COFIPS').droplevel(level=0, axis=1)
    df = df.fillna(value=0).astype(int).reset_index()
    df.index.name = None
    df.columns.name = None

    return df

def main():
    df80 = get_1980_population()
    df90 = get_1990_population()
    df = estimate_1985_population(df80, df90)

    con = sqlite3.connect(database=POPULATION_DB)
    df.to_sql(name='county_population_race_1985',
              if_exists='replace',
              con=con,
              index=False)
    con.close()

    print("\nFinished!\n")


if __name__ == '__main__':
    main()
