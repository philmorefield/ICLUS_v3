import glob
import itertools
import os
import sqlite3

import numpy as np
import pandas as pd

DATABASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases'
MIGRATION_DB = os.path.join(DATABASE_FOLDER, 'migration.sqlite')
POPULATION_DB = os.path.join(DATABASE_FOLDER, 'population.sqlite')
ANALYSIS_DB = os.path.join(DATABASE_FOLDER, 'analysis.sqlite')

RACE_CODE_MAP = {1: 'WHITE',
                 2: 'BLACK',
                 3: 'AIAN',
                 4: 'API',
                 5: 'OTHER'}

AGE_CODE_MAP = {1: '0_TO_4',
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
                18: '85_TO_115'}

COLUMNS = ['ORIGIN_FIPS', 'DESTINATION_FIPS', 'RACE', 'AGE', 'SEX_CODE', 'NATIVITY_CODE', 'POVSTAT_CODE', 'EDU_CODE', 'FLOW']


def update_migration_fips(df):
    con = sqlite3.connect(MIGRATION_DB)

    # update the FIPS codes
    query = 'SELECT OLD_FIPS, NEW_FIPS \
             FROM fips_or_name_changes'
    change_df = pd.read_sql(sql=query, con=con)
    con.close()

    df = df.merge(right=change_df, how='left', left_on='ORIGIN_FIPS', right_on='OLD_FIPS', copy=False)
    df.loc[~df.NEW_FIPS.isnull(), 'ORIGIN_FIPS'] = df['NEW_FIPS']
    df.drop(labels=['OLD_FIPS', 'NEW_FIPS'], axis=1, inplace=True)

    df = df.merge(right=change_df, how='left', left_on='DESTINATION_FIPS', right_on='OLD_FIPS', copy=False)
    df.loc[~df.NEW_FIPS.isnull(), 'DESTINATION_FIPS'] = df['NEW_FIPS']
    df.drop(labels=['OLD_FIPS', 'NEW_FIPS'], axis=1, inplace=True)

    group_columns = COLUMNS.copy()
    group_columns.remove('FLOW')
    df = df.groupby(by=group_columns, as_index=False).sum()

    return df


def main():
    input_folder  = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\raw_files\\Census\\1990\\p1'
    file_list = glob.glob(os.path.join(input_folder, '*', '*'))
    df = None

    for f in file_list:
        temp = pd.read_csv(filepath_or_buffer=f,
                           delim_whitespace=True,
                           header=None,
                           names=('COL1', 'POPIN', 'FLOW'),
                           dtype={'COL1': 'str', 'POPIN': 'int', 'FLOW': 'int'})
        temp['ORIGIN_FIPS'] = temp['COL1'].str[:5]
        temp['DESTINATION_FIPS'] = temp['COL1'].str[5:10]
        temp['RACE_CODE'] = temp['COL1'].str[10].astype(int)
        temp['SEX_CODE'] = temp['COL1'].str[11]
        temp['NATIVITY_CODE'] = temp['COL1'].str[12]
        temp['POVSTAT_CODE'] = temp['COL1'].str[13]
        temp['EDU_CODE'] = temp['COL1'].str[14]
        temp['AGE_CODE'] = temp['COL1'].str[15:17].astype(int)
        temp['RACE'] = temp['RACE_CODE'].map(RACE_CODE_MAP)
        temp['AGE'] = temp['AGE_CODE'].map(AGE_CODE_MAP)
        temp.loc[temp.DESTINATION_FIPS == '00000', 'FLOW'] = temp['POPIN']

        if df is None:
            df = temp.copy()
        else:
            df = pd.concat(objs=(df, temp), ignore_index=True)

    df = df[COLUMNS]
    df = update_migration_fips(df)

    df = df.query('DESTINATION_FIPS != "99999"')  # abroad
    df = df.query('DESTINATION_FIPS != "00000"')  # non-movers
    df = df.query('ORIGIN_FIPS != DESTINATION_FIPS')  # non-movers
    assert not df.isnull().any().any()

    con = sqlite3.connect(MIGRATION_DB)
    df.to_sql(name='census_enhanced_migration_1985_1990',
              con=con,
              if_exists='replace',
              index=False)
    con.close()

    print("\nFinished!")


if __name__ == '__main__':
    main()
