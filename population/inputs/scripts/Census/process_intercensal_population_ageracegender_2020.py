import os
import sqlite3

import pandas as pd

IP_FOLDER = 'D:\\OneDrive\\Dissertation\\data\\Census\\intercensal_population'

ANALYSIS_DB = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases\\analysis.sqlite'
INPUT_DB = 'D:\\OneDrive\\paper_2_HOMO\\input\\paper_2_inputs.sqlite'
MIGRATION_DB = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases\\migration.sqlite'
POPULATION_DB = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases\\population.sqlite'

AGE_MAP = {1: '0_TO_4',
           2: '5_TO_9',
           3: '10_TO_14',
           4: '15_TO_19',
           5: '20_TO_24',
           6: '25_TO_29',
           7: '30_TO_34',
           8: '35_TO_39',
           9: '40_TO_44',
           10: '45_TO_49',
           11: '50_TO_54',
           12: '55_TO_59',
           13: '60_TO_64',
           14: '65_TO_69',
           15: '70_TO_74',
           16: '75_TO_79',
           17: '80_TO_84',
           18: '85_AND_OVER'}

YEAR_MAP = {3: 2010,
            4: 2011,
            5: 2012,
            6: 2013,
            7: 2014,
            8: 2015,
            9: 2016,
            10: 2017,
            11: 2018,
            12: 2019,
            13: 2020}

RACE_SEX_MAP = {'WA_MALE': 'WHITE_MALE',
                'WA_FEMALE': 'WHITE_FEMALE',
                'BA_MALE': 'BLACK_MALE',
                'BA_FEMALE': 'BLACK_FEMALE',
                'IA_MALE': 'AIAN_MALE',
                'IA_FEMALE': 'AIAN_FEMALE',
                'AA_MALE': 'ASIAN_MALE',
                'AA_FEMALE': 'ASIAN_FEMALE',
                'NA_MALE': 'NHPI_MALE',
                'NA_FEMALE': 'NHPI_FEMALE',
                'TOM_MALE': 'TWO_OR_MORE_MALE',
                'TOM_FEMALE': 'TWO_OR_MORE_FEMALE'}

def main():
    con = sqlite3.connect(MIGRATION_DB)
    query = 'SELECT OLD_FIPS, NEW_FIPS FROM fips_or_name_changes'
    fips_changes = pd.read_sql(sql=query, con=con)
    con.close()

    csv = os.path.join(IP_FOLDER, '2010_to_2020', 'CC-EST2020-ALLDATA.csv')
    df = pd.read_csv(filepath_or_buffer=csv, encoding='latin1', low_memory=False)
    df.query('SUMLEV == 50 & YEAR >=3 & AGEGRP >= 1', inplace=True)
    df.drop(columns=['SUMLEV', 'TOT_POP', 'TOT_MALE', 'TOT_FEMALE'], inplace=True)
    df = df.rename(columns={'AGEGRP': 'AGE_GROUP'})

    df['COFIPS'] = df['STATE'].astype('str').str.zfill(2) + df['COUNTY'].astype(str).str.zfill(3)
    df.drop(columns=['STATE', 'STNAME', 'COUNTY', 'CTYNAME'], inplace=True)
    df.set_index(keys=['COFIPS', 'YEAR', 'AGE_GROUP'], inplace=True)
    df = df.astype(int)
    df.reset_index(inplace=True)

    df = df.merge(right=fips_changes, how='left', left_on='COFIPS', right_on='OLD_FIPS', copy=False)
    df.loc[~pd.isnull(df['NEW_FIPS']), 'COFIPS'] = df['NEW_FIPS']
    df.drop(labels=['OLD_FIPS', 'NEW_FIPS'], axis=1, inplace=True)
    df = df.groupby(by=['COFIPS', 'YEAR', 'AGE_GROUP'], as_index=False).sum()

    df['YEAR'] = df['YEAR'].replace(to_replace=YEAR_MAP)
    df['AGE_GROUP'] = df['AGE_GROUP'].replace(to_replace=AGE_MAP)
    df = df[['COFIPS', 'YEAR', 'AGE_GROUP'] + list(RACE_SEX_MAP.keys())]

    df = df.melt(id_vars=['COFIPS', 'YEAR', 'AGE_GROUP'], var_name='RACE', value_name='POPULATION')
    df['RACE'] = df['RACE'].replace(to_replace=RACE_SEX_MAP)
    df['SEX'] = df['RACE'].str.split('_').str[-1]
    df['RACE'] = df['RACE'].str.replace('_MALE', '').str.replace('_FEMALE', '')

    assert not df.isnull().any().any()

    con = sqlite3.connect(POPULATION_DB)
    df.to_sql(name='county_population_ageracegender_2010_to_2020',
              con=con,
              if_exists='replace',
              index=False)
    con.close()


if __name__ == '__main__':
    main()
