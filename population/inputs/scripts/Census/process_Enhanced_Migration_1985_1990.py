import glob
import itertools
import os
import sqlite3

import numpy as np
import pandas as pd

data_dir = 'C:\\Users\\Phil\\OneDrive\\Dissertation\\data\\Census\\1990\\p1'
file_list = glob.glob(os.path.join(data_dir, '*', '*'))

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


def get_euclidean_distance(year):
    # print("get_euclidean_distance()")
    # print(time.ctime(), "\n")

    if year <= 2000:
        distance_table = 2000
    else:
        distance_table = 2010

    conn = sqlite3.connect('C:\\Users\\Phil\\Dissertation_C_Drive\\analysis\\analysis_db.sqlite')
    distance_df = pd.read_sql_query(f'SELECT "ORIGIN_FIPS", "DESTINATION_FIPS", "Dij" from county_to_county_distance_{distance_table}', con=conn)
    conn.close()

    assert not distance_df.isnull().any().any()

    return distance_df


def expand_missing_flows(df):
    print("expand_missing_flows()")

    fips = list(set(list(df.ORIGIN_FIPS) + list(df.DESTINATION_FIPS)))
    prod = itertools.product(fips, fips, range(1, 6), range(1, 18))
    new_df = pd.DataFrame.from_records(data=list(prod),
                                       columns=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'RACE_CODE', 'AGE_CODE'])
    new_df = new_df.loc[new_df.ORIGIN_FIPS != new_df.DESTINATION_FIPS, :]
    new_df['RACE'] = new_df['RACE_CODE'].map(RACE_CODE_MAP)
    new_df['AGE'] = new_df['AGE_CODE'].map(AGE_CODE_MAP)
    new_df = new_df[['ORIGIN_FIPS', 'DESTINATION_FIPS', 'RACE', 'AGE']]
    # new_df.sort_values(by=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'RACE', 'AGE'], inplace=True)
    new_df.reset_index(drop=True, inplace=True)

    # new_df = new_df.loc[~new_df.ORIGIN_FIPS.isin(['02999', '15999']), :]
    # new_df = new_df.loc[~new_df.DESTINATION_FIPS.isin(['02999', '15999']), :]

    new_df = new_df.merge(right=df, how='left', on=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'RACE', 'AGE'], copy=False)
    new_df['FLOW'].fillna(0, inplace=True)
    new_df['FLOW'] = new_df['FLOW'].astype('int')

    assert not new_df.isnull().any().any()

    return new_df


def update_fips(df):
    print("update_fips()")

    conn = sqlite3.connect('C:\\Users\\Phil\\OneDrive\\Dissertation\\data\\migration_db.sqlite')

    # update the FIPS codes
    change_df = pd.read_sql_query('SELECT "OLD_FIPS", "NEW_FIPS" FROM fips_or_name_changes', con=conn)
    conn.close()

    df = df.merge(right=change_df, how='left', left_on='ORIGIN_FIPS', right_on='OLD_FIPS', copy=False)
    df.loc[~df.NEW_FIPS.isnull(), 'ORIGIN_FIPS'] = df['NEW_FIPS']
    df.drop(labels=['OLD_FIPS', 'NEW_FIPS'], axis=1, inplace=True)

    df = df.merge(right=change_df, how='left', left_on='DESTINATION_FIPS', right_on='OLD_FIPS', copy=False)
    df.loc[~df.NEW_FIPS.isnull(), 'DESTINATION_FIPS'] = df['NEW_FIPS']
    df.drop(labels=['OLD_FIPS', 'NEW_FIPS'], axis=1, inplace=True)

    df = df[['ORIGIN_FIPS', 'DESTINATION_FIPS', 'FLOW']].groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS'], as_index=False).sum()

    # add mi and nj
    df_pop = df[['ORIGIN_FIPS', 'FLOW']].groupby('ORIGIN_FIPS', as_index=False).sum()

    # remove non-movers
    df = df.loc[df.DESTINATION_FIPS != '00000', :]

    # ignore people that were overseas in 1985
    df = df.loc[df.DESTINATION_FIPS != '99999', :]

    # ignore the now-defunct Yellowstone National Park
    df = df.loc[df.DESTINATION_FIPS != '30113', :]
    df = df.loc[df.ORIGIN_FIPS != '30113', :]

    # remove intra-county movers
    df = df.loc[df.ORIGIN_FIPS != df.DESTINATION_FIPS, :]

    df_pop.rename({'ORIGIN_FIPS': 'COFIPS', 'FLOW': 'mi'}, axis=1, inplace=True)
    df = df.merge(right=df_pop[['COFIPS', 'mi']],
                  how='left',
                  left_on='ORIGIN_FIPS',
                  right_on='COFIPS',
                  copy=False)
    df.drop('COFIPS', axis=1, inplace=True)

    df_pop.rename({'mi': 'nj'}, axis=1, inplace=True)
    df = df.merge(right=df_pop[['COFIPS', 'nj']],
                  how='left',
                  left_on='DESTINATION_FIPS',
                  right_on='COFIPS',
                  copy=False)
    df.drop('COFIPS', axis=1, inplace=True)

    # add Dij
    distance_df = get_euclidean_distance(year=1985)
    df = df.merge(right=distance_df, how='left', on=['ORIGIN_FIPS', 'DESTINATION_FIPS'], copy=False)

    assert not df.isnull().any().any()

    return df


def get_annual_census_population(year, DOMESTIC_ONLY=True, CONUS_ONLY=False):
    # print("get_annual_census_population()")
    # print(time.ctime(), "\n")

    assert 1980 <= year <= 2015

    db = 'C:\\Users\\Phil\\OneDrive\\Dissertation\\data\\population_db.sqlite'
    query = 'SELECT "COFIPS", "{}" FROM {}'

    if year <= 1989:
        table = 'county_population_1980_to_1989'
    elif year <= 1999:
        table = 'county_population_1990_to_1999'
    elif year <= 2010:
        table = 'county_population_2000_to_2010'
    else:
        table = 'county_population_2010_to_2016'

    conn = sqlite3.connect(db)
    census_df = pd.read_sql_query(query.format(year, table), con=conn)
    conn.close()
    census_df.rename({str(year): 'POPULATION'}, axis=1, inplace=True)

    if DOMESTIC_ONLY is True:
        # ignore APO/FPO, FOREIGN, and ALL OTHER FLOWS
        census_df = census_df[~census_df['COFIPS'].isin(['57005', '57009', '59999'])]

    if CONUS_ONLY is True:
        # ignore HAWAII and ALASKA
        census_df = census_df[~census_df['COFIPS'].isin(['15999', '02999'])]

    assert not census_df.isnull().any().any()

    return census_df


def main():
    df = None
    output_name = 'gravity_inputs_{}_{}_Census1990'

    for f in file_list:
        temp = pd.read_csv(filepath_or_buffer=f,
                           delim_whitespace=True,
                           header=None,
                           names=('COL1', 'POPIN', 'FLOW'),
                           dtype={'COL1': 'str', 'POPIN': 'int', 'FLOW': 'int'})
        temp['ORIGIN_FIPS'] = temp['COL1'].str[:5]
        temp['DESTINATION_FIPS'] = temp['COL1'].str[5:10]
        temp['RACE_CODE'] = temp['COL1'].str[10].astype('int')
        temp['SEX_CODE'] = temp['COL1'].str[11]
        temp['NATIVITY_CODE'] = temp['COL1'].str[12]
        temp['POVSTAT_CODE'] = temp['COL1'].str[13]
        temp['EDUC_CODE'] = temp['COL1'].str[14]
        temp['AGE_CODE'] = temp['COL1'].str[15:17].astype('int')
        temp['RACE'] = temp['RACE_CODE'].map(RACE_CODE_MAP)
        temp['AGE'] = temp['AGE_CODE'].map(AGE_CODE_MAP)

        temp.loc[temp.DESTINATION_FIPS == '00000', 'FLOW'] = temp['POPIN']

        if df is None:
            df = temp.copy()
        else:
            df = pd.concat(objs=(df, temp), ignore_index=True)

    keep_cols = ['ORIGIN_FIPS', 'DESTINATION_FIPS', 'FLOW']
    df = df[keep_cols].groupby(['ORIGIN_FIPS', 'DESTINATION_FIPS'], as_index=False).sum()
    # df = expand_missing_flows(df)
    df = update_fips(df)
    # census_df = get_annual_census_population(year=1985)

    df.sort_values(by='Dij', inplace=True)
    df['sij'] = df.groupby('ORIGIN_FIPS')['nj'].transform(lambda x: x.shift(1).cumsum()).fillna(0).astype('int')

    columns = ['ORIGIN_FIPS', 'DESTINATION_FIPS', 'Dij', 'sij', 'mi', 'nj', 'FLOW']
    df['random_number'] = np.random.random(len(df))
    train = df.loc[df['random_number'] <= 0.8, :]
    test = df.loc[df['random_number'] > 0.8, :]

    db = 'C:\\Users\\Phil\\OneDrive\\Dissertation\\analysis\\part_1\\inputs\\analysis_part_1_inputs.sqlite'
    conn = sqlite3.connect(db)

    train[columns].to_sql(name=output_name.format('1990', 'train'), con=conn, if_exists='replace', index=False)
    test[columns].to_sql(name=output_name.format('1990', 'test'), con=conn, if_exists='replace', index=False)

    conn.close()

    print("Finished!")


if __name__ == '__main__':
    main()
