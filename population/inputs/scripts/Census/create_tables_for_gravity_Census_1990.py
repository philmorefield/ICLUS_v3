'''
20200204 - update to divide migration flows by five, i.e., annual average
'''

import os
import sqlite3

import pandas as pd

DATABASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases'


con_outer = sqlite3.connect(os.path.join(DATABASE_FOLDER, 'migration.sqlite'))
FIPS_CHANGES = pd.read_sql_query('SELECT OLD_FIPS, NEW_FIPS FROM fips_or_name_changes', con=con_outer)
con_outer.close()

RACE_MAP = {'Black female': 'BLACK',
            'Black male': 'BLACK',
            'White female': 'WHITE',
            'White male': 'WHITE',
            'Other races female': 'OTHER',
            'Other races male': 'OTHER'}


def get_migration():
    db = os.path.join(DATABASE_FOLDER, 'migration.sqlite')
    con = sqlite3.connect(db)
    df = pd.read_sql_query('SELECT "ORIGIN_FIPS", "DESTINATION_FIPS", "FLOW", "RACE" FROM census_enhanced_migration_1985_1990', con=con)
    con.close()

    # ignore intra-county movers
    df = df.loc[df.ORIGIN_FIPS != df.DESTINATION_FIPS, :]

    # ignore non-movers (over age 5)
    df = df.loc[df.DESTINATION_FIPS != '00000', :]

    # ignore movers that were abroad in 1990
    df = df.loc[df.DESTINATION_FIPS != '99999', :]

    # make sure ORIGIN_FIPS is updated
    df = df.merge(right=FIPS_CHANGES, how='left', left_on='ORIGIN_FIPS', right_on='OLD_FIPS', copy=False)
    df.loc[~pd.isnull(df['NEW_FIPS']), 'ORIGIN_FIPS'] = df['NEW_FIPS']
    df.drop(labels=['OLD_FIPS', 'NEW_FIPS'], axis=1, inplace=True)

    df = df.merge(right=FIPS_CHANGES, how='left', left_on='DESTINATION_FIPS', right_on='OLD_FIPS', copy=False)
    df.loc[~pd.isnull(df['NEW_FIPS']), 'DESTINATION_FIPS'] = df['NEW_FIPS']
    df.drop(labels=['OLD_FIPS', 'NEW_FIPS'], axis=1, inplace=True)

    df = df.groupby(['ORIGIN_FIPS', 'DESTINATION_FIPS', 'RACE']).sum()
    assert not df.isnull().any().any()

    df = df.unstack(level=-1, fill_value=0)
    df.columns = df.columns.get_level_values(1)
    df.columns.name = None
    df = df.astype(int)

    df['OTHER'] = df.AIAN + df.API + df.OTHER
    df = df[['WHITE', 'BLACK', 'OTHER']]
    # df = (df / 5.0).round().astype(int)

    df.reset_index(inplace=True)

    return df


def get_euclidean_distance(year):
    print("get_euclidean_distance()")

    if year <= 2000:
        distance_table = 2000
    else:
        distance_table = 2010

    con = sqlite3.connect(os.path.join(DATABASE_FOLDER, 'analysis.sqlite'))
    df = pd.read_sql_query(f'SELECT "ORIGIN_FIPS", "DESTINATION_FIPS", "Dij" \
                             FROM county_to_county_distance_{distance_table}',
                           con=con)
    con.close()

    assert not df.isnull().any().any()

    return df


def get_population(year):
    print("get_population()")

    assert 1983 <= year <= 2015

    db = os.path.join(DATABASE_FOLDER, 'population.sqlite')

    if year <= 1989:
        table = 'county_population_agerace_1980_to_1989'
    else:
        raise Exception

    con = sqlite3.connect(db)
    df = pd.read_sql_query(f'SELECT * FROM {table} \
                             WHERE "YEAR" = {year}', con=con)
    con.close()

    df['POPULATION'] = df.iloc[:, 3:].sum(axis=1).astype(int)
    df['RACE_SEX'].replace(to_replace=RACE_MAP, inplace=True)
    df.rename(columns={'RACE_SEX': 'RACE'}, inplace=True)
    df = df[['COFIPS', 'RACE', 'POPULATION']].groupby(by=['COFIPS', 'RACE'], as_index=False).sum()

    df = df.merge(right=FIPS_CHANGES, how='left', left_on='COFIPS', right_on='OLD_FIPS', copy=False)
    df.loc[~df.NEW_FIPS.isnull(), 'COFIPS'] = df['NEW_FIPS']
    df.drop(labels=['OLD_FIPS', 'NEW_FIPS'], axis=1, inplace=True)
    df = df.groupby(by=['COFIPS', 'RACE'], as_index=False).sum()

    # ignore APO/FPO, FOREIGN, and ALL OTHER FLOWS
    df = df[~df['COFIPS'].isin(['57005', '57009', '59999'])]

    df = df.pivot(index='COFIPS', columns='RACE', values='POPULATION')
    df.fillna(value=0, inplace=True)
    df.columns.name = None

    assert not df.isnull().any().any()

    df.reset_index(inplace=True)

    return df


def update_fips(df):
    print("update_fips()")

    # remove non-movers
    df = df.loc[df.DESTINATION_FIPS != '00000', :]

    # ignore people that were overseas in 1985
    df = df.loc[df.DESTINATION_FIPS != '99999', :]

    # ignore the now-defunct Yellowstone National Park
    df = df.loc[df.DESTINATION_FIPS != '30113', :]
    df = df.loc[df.ORIGIN_FIPS != '30113', :]

    # remove intra-county movers
    df = df.loc[df.ORIGIN_FIPS != df.DESTINATION_FIPS, :]

    assert not df.isnull().any().any()

    return df


def merge_dataframes(distance_df=None, population_df=None, migration_df=None, race=None):
    """Combines DataFrames holding distance, population, and migration data

    Args:

    Returns:
        A DataFrame.

    Raises:

    """
    print("merge_dataframes()")

    migration_df.rename(columns={'BLACK': 'MIG_BLACK',
                                 'OTHER': 'MIG_OTHER',
                                 'WHITE': 'MIG_WHITE'},
                        inplace=True)

    population_df.rename(columns={'BLACK': 'POP_BLACK',
                                  'OTHER': 'POP_OTHER',
                                  'WHITE': 'POP_WHITE'},
                         inplace=True)

    df = distance_df.merge(right=migration_df, how='left', on=['ORIGIN_FIPS', 'DESTINATION_FIPS'], copy=False)
    df = df.merge(right=population_df, how='left', left_on='ORIGIN_FIPS', right_on='COFIPS', copy=False)
    df.drop(['COFIPS'], axis=1, inplace=True)
    df.rename(columns={f'POP_{race}': 'mi', f'MIG_{race}': 'FLOW'}, inplace=True)

    df = df.merge(right=population_df, how='left', left_on='DESTINATION_FIPS', right_on='COFIPS', copy=False)
    df.drop('COFIPS', axis=1, inplace=True)
    df.rename(columns={f'POP_{race}': 'nj'}, inplace=True)
    df['FLOW'] = df.FLOW.fillna(0).astype(int)

    assert not df.isnull().any().any()

    return df


def main():
    migration_df = get_migration()
    distance_df = get_euclidean_distance(year=1985)
    population_df = get_population(year=1985)

    for race in ('WHITE', 'BLACK', 'OTHER'):

        output_name = 'gravity_inputs_{}_Census_1990_{}'

        migration_cols = ['ORIGIN_FIPS', 'DESTINATION_FIPS', race]
        df = merge_dataframes(distance_df=distance_df,
                              population_df=population_df[['COFIPS', race]],
                              migration_df=migration_df[migration_cols],
                              race=race)

        # variables calculated from the perspective of the origin
        df.sort_values(by=['ORIGIN_FIPS', 'Dij'], inplace=True)

        # intervening opportunities
        df['sij'] = df.groupby('ORIGIN_FIPS')['nj'].transform(lambda x: x.shift(1).cumsum()).fillna(0).astype(int)

        # distance-weighted Intervening Opportunities
        df['PPD'] = df.nj / df.Dij  # 'people per unit of distance'
        m = df.groupby('ORIGIN_FIPS')['PPD'].transform(sum)
        df['Tij'] = df.groupby('ORIGIN_FIPS')['PPD'].transform(lambda x: x.shift(1).cumsum()).fillna(0).round().astype(int)

        # competing destinations
        df['Aij'] = (m - df.PPD).astype(int)

        # variables calculated from the persepctive of the destination
        df.sort_values(by=['DESTINATION_FIPS', 'Dij'], inplace=True)

        # competing migrants
        df['Cij'] = df.groupby('DESTINATION_FIPS')['mi'].transform(lambda x: x.shift(1).cumsum()).fillna(0).astype(int)

        # localized competing destinations
        df['Lij'] = df.groupby('DESTINATION_FIPS')['PPD'].transform(lambda x: x.shift(1).cumsum()).fillna(0).round().astype(int)

        columns = ['ORIGIN_FIPS', 'DESTINATION_FIPS', 'Dij', 'sij', 'Tij', 'Cij', 'mi', 'nj', 'FLOW']
        df.sort_values(by=['ORIGIN_FIPS', 'DESTINATION_FIPS'], inplace=True)
        df = df.loc[:, columns]

        db = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases\\census.sqlite'
        con = sqlite3.connect(db)

        df.to_sql(name=output_name.format('5_year', race),
                  con=con,
                  if_exists='replace',
                  index=False)

        con.close()

        print(f"Finished {race}")

    print("Finished!")


if __name__ == '__main__':
    main()
