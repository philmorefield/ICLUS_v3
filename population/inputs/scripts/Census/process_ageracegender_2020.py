import os
import sqlite3

import pandas as pd
import polars as pl

if os.path.isdir('D:\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'
elif os.path.isdir('D:\\projects\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\projects\\ICLUS_v3\\population'
else:
    raise Exception

INPUT_FOLDER = os.path.join(BASE_FOLDER, 'inputs')
MIG_DB = os.path.join(INPUT_FOLDER, 'databases', 'migration.sqlite')
OUTPUT_FOLDER = os.path.join(BASE_FOLDER, 'outputs')

AGE_GROUP_MAP = {1: '0-4',
                 2: '5-9',
                 3: '10-14',
                 4: '15-19',
                 5: '20-24',
                 6: '25-29',
                 7: '30-34',
                 8: '35-39',
                 9: '40-44',
                 10: '45-49',
                 11: '50-54',
                 12: '55-59',
                 13: '60-64',
                 14: '65-69',
                 15: '70-74',
                 16: '75-79',
                 17: '80-84',
                 18: '85+'}

RACE_MAP = {'WA': 'WHITE',
            'BA': 'BLACK',
            'IA': 'AIAN',
            'AA': 'ASIAN',
            'NA': 'NHPI',
            'TOM': 'TWO_OR_MORE'}


def make_fips_changes(df=None):
    '''
    TODO: Add docstring
    '''

    uri = f'sqlite:{MIG_DB}'
    query = 'SELECT OLD_FIPS, NEW_FIPS \
             FROM fips_or_name_changes'
    df_fips = pl.read_database_uri(query=query, uri=uri)

    df = df.join(other=df_fips,
                 how='left',
                 left_on='GEOID',
                 right_on='OLD_FIPS',
                 coalesce=True)

    df = df.with_columns(pl.when(pl.col('NEW_FIPS').is_not_null())
                         .then(pl.col('NEW_FIPS'))
                         .otherwise(pl.col('GEOID')).alias('GEOID'))
    df = df.drop('NEW_FIPS')
    df = df.group_by(['GEOID', 'AGE_GROUP', 'RACE', 'GENDER']).agg(pl.col('POPULATION').sum())

    return df


def get_valid_fips():
    db = 'D:\\OneDrive\\Dissertation\\databases\\migration.sqlite'
    con = sqlite3.connect(db)
    query = 'SELECT CYFIPS, CYNAME FROM valid_cyfips'
    df = pd.read_sql(sql=query, con=con)
    con.close()

    df = make_fips_changes(df=df)

    return df

def main():
    valid_fips = get_valid_fips()

    csv = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\raw_files\\Census\\cc-est2023-alldata.csv'
    df = pd.read_csv(filepath_or_buffer=csv, encoding='latin1')
    df = df[['STATE', 'COUNTY', 'YEAR', 'AGEGRP', 'WA_MALE', 'WA_FEMALE', 'BA_MALE', 'BA_FEMALE', 'IA_MALE', 'IA_FEMALE', 'AA_MALE', 'AA_FEMALE', 'NA_MALE', 'NA_FEMALE', 'TOM_MALE', 'TOM_FEMALE']]
    df.query('YEAR == 2 & AGEGRP >= 1', inplace=True)  # estimate for 7/1/2020

    df.rename(columns={'AGEGRP': 'AGE_GROUP'}, inplace=True)
    df['AGE_GROUP'] = df['AGE_GROUP'].map(AGE_GROUP_MAP)
    df['GEOID'] = df.STATE.astype(str).str.zfill(2) + df.COUNTY.astype(str).str.zfill(3)
    df.drop(columns=['STATE', 'COUNTY', 'YEAR'], inplace=True)

    df = df.melt(id_vars=['GEOID', 'AGE_GROUP'], var_name='RACE_GENDER', value_name='POPULATION')
    df['RACE'] = df['RACE_GENDER'].apply(lambda x: x.split('_')[0])
    df['GENDER'] = df['RACE_GENDER'].apply(lambda x: x.split('_')[1])
    df.drop(columns='RACE_GENDER', inplace=True)
    df.RACE = df.RACE.map(RACE_MAP)
    df = df[['GEOID', 'AGE_GROUP', 'RACE', 'GENDER', 'POPULATION']]
    df['POPULATION'] = df['POPULATION'].astype(int)

    output_folder = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases'
    db = os.path.join(output_folder, 'population.sqlite')
    con = sqlite3.connect(database=db)
    df.to_sql(name='county_population_ageracegender_2020',
              if_exists='replace',
              con=con, index=False)
    con.close()

    print("Finished!")


if __name__ == '__main__':
    main()
