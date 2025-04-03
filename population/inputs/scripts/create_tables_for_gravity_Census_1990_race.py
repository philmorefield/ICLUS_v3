'''
Prepare ZINB regression inputs for Census 1990 data.
'''

import os
import sqlite3

import pandas as pd

DATABASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases'
MIGRATION_DB = os.path.join(DATABASE_FOLDER, 'migration.sqlite')
POPULATION_DB = os.path.join(DATABASE_FOLDER, 'population.sqlite')
ANALYSIS_DB = os.path.join(DATABASE_FOLDER, 'analysis.sqlite')

AGE_GROUPS = ('5_TO_9',
              '10_TO_14',
              '15_TO_19',
              '20_TO_24',
              '25_TO_29',
              '30_TO_34',
              '35_TO_39',
              '40_TO_44',
              '45_TO_49',
              '50_TO_54',
              '55_TO_59',
              '60_TO_64',
              '65_TO_69',
              '70_TO_74',
              '75_TO_79',
              '80_TO_84',
              '85_TO_115')


def get_migration(race, age_group):
    con = sqlite3.connect(MIGRATION_DB)
    query = f'SELECT ORIGIN_FIPS, DESTINATION_FIPS, FLOW \
              FROM census_enhanced_migration_1985_1990 \
              WHERE RACE == "{race}" AND AGE == "{age_group}"'
    df = pd.read_sql(sql=query, con=con)
    con.close()
    df = df.groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS'], as_index=False).sum()
    df['FLOW'] = (df['FLOW'] / 5.0).round().astype(int)

    return df


def get_euclidean_distance():
    query = 'SELECT ORIGIN_FIPS, DESTINATION_FIPS, Dij \
             FROM county_to_county_distance_2020'
    con = sqlite3.connect(ANALYSIS_DB)
    df = pd.read_sql_query(sql=query, con=con)
    con.close()

    assert not df.isnull().any().any()

    return df


def get_census_population(race):
    query = f'SELECT COFIPS, {race} \
              FROM county_population_race_1985'
    con = sqlite3.connect(POPULATION_DB)
    df = pd.read_sql(sql=query, con=con)
    con.close()

    df = df.rename(columns={race: 'POPULATION_1985'})

    return df


def merge_dataframes(distance_df, population_df, migration_df, dummy_df):
    df = distance_df.merge(right=migration_df, how='left', on=['ORIGIN_FIPS', 'DESTINATION_FIPS'], copy=False)
    df = df.fillna(value=0)
    df = df.merge(right=population_df, how='inner', left_on='ORIGIN_FIPS', right_on='COFIPS', copy=False)
    df = df.rename(columns={'POPULATION_1985': 'Pi'})
    df = df.drop(columns=['COFIPS'])

    df = df.merge(right=population_df, how='left', left_on='DESTINATION_FIPS', right_on='COFIPS', copy=False)
    df = df.drop(columns='COFIPS')
    df = df.rename(columns={'POPULATION_1985': 'Pj'})

    # there are a few counties that can't be included, and excluding them won't
    # hurt our sample size in any way.
    #
    # Missing counties: 02164, 02188, 04012, 08014, 35006
    df = df.loc[~df.Pj.isnull()]

    # join the labor market and urban dummy variables
    df = df.merge(right=dummy_df, how='left', on=['ORIGIN_FIPS', 'DESTINATION_FIPS'], copy=False)

    # calculate total BEA population minus destination, "Pj_star"
    df.loc[df['SAME_LABOR_MARKET'] == 1, 'Pj_star'] = df['Pj']
    df['Pj_star'] = df['Pj_star'].fillna(value=0)
    df['Pj_star'] = df.groupby(by='ORIGIN_FIPS')['Pj_star'].transform('sum').astype(int)
    temp = df[['ORIGIN_FIPS', 'Pj_star']]
    df.drop(columns=['Pj_star'], inplace=True)
    temp = temp.drop_duplicates(ignore_index=True)
    temp.rename(columns={'ORIGIN_FIPS': 'DESTINATION_FIPS'}, inplace=True)
    df = df.merge(right=temp, how='left', on='DESTINATION_FIPS')

    return df


def label_intra_labor_market_moves(df):
    query = 'SELECT COFIPS, BEA10 \
             FROM fips_to_urb20_bea10_hhs'
    con = sqlite3.connect(MIGRATION_DB)
    lm_df = pd.read_sql_query(sql=query, con=con)
    con.close()

    df = df.merge(right=lm_df,
                  how='left',
                  left_on='ORIGIN_FIPS',
                  right_on='COFIPS',
                  copy=False)
    df.rename(columns={'BEA10': 'ORIGIN_BEA10'}, inplace=True)
    df.drop(columns='COFIPS', inplace=True)

    df = df.merge(right=lm_df,
                  how='left',
                  left_on='DESTINATION_FIPS',
                  right_on='COFIPS')
    df.rename(columns={'BEA10': 'DESTINATION_BEA10'}, inplace=True)
    df.drop(columns='COFIPS', inplace=True)
    assert not df.isnull().any().any()
    df['SAME_LABOR_MARKET'] = 0
    df.loc[df.ORIGIN_BEA10 == df.DESTINATION_BEA10, 'SAME_LABOR_MARKET'] = 1
    assert not df.isnull().any().any()
    df.drop(columns=['ORIGIN_BEA10', 'DESTINATION_BEA10'], inplace=True)

    return df


def label_urban_destinations(df):

    df = df.drop(columns=['Dij'])

    query = 'SELECT COFIPS, URBANDESTINATION20 \
             FROM fips_to_urb20_bea10_hhs'
    con = sqlite3.connect(MIGRATION_DB)
    urb_df = pd.read_sql_query(sql=query, con=con)
    con.close()

    df = df.merge(right=urb_df,
                  how='left',
                  left_on='DESTINATION_FIPS',
                  right_on='COFIPS')
    df['URBANDESTINATION20'] = df['URBANDESTINATION20'].astype(int)
    df.drop(columns='COFIPS', inplace=True)

    return df


def main():
    distance_df = get_euclidean_distance()
    dummy_df = label_intra_labor_market_moves(distance_df)
    dummy_df = label_urban_destinations(dummy_df)

    for race in ('WHITE', 'BLACK', 'AIAN', 'API', 'OTHER'):
        print(f"\nStarting {race}...")
        for age_group in AGE_GROUPS:
            population_df = get_census_population(race=race)
            migration_df = get_migration(race=race, age_group=age_group)
            df = merge_dataframes(distance_df=distance_df,
                                  population_df=population_df,
                                  migration_df=migration_df,
                                  dummy_df=dummy_df)

            # variables calculated from the perspective of the origin
            df.sort_values(by=['ORIGIN_FIPS', 'Dij'], inplace=True)

            # distance-weighted Intervening Opportunities
            df['PPD'] = df.Pj / df.Dij  # 'people per unit of distance'
            df['Tij'] = df.groupby('ORIGIN_FIPS')['PPD'].transform(lambda x: x.shift(1).cumsum()).fillna(0).round().astype(int)

            # variables calculated from the persepctive of the destination
            df.sort_values(by=['DESTINATION_FIPS', 'Dij'], inplace=True)

            # competing migrants
            df['Cij'] = df.groupby('DESTINATION_FIPS')['Pi'].transform(lambda x: x.shift(1).cumsum()).fillna(0).astype(int)

            columns = ['ORIGIN_FIPS', 'DESTINATION_FIPS', 'Dij', 'SAME_LABOR_MARKET', 'URBANDESTINATION20', 'Tij', 'Cij', 'Pi', 'Pj', 'Pj_star', 'FLOW']
            df.sort_values(by=['ORIGIN_FIPS', 'DESTINATION_FIPS'], inplace=True)
            df = df.loc[:, columns]
            df['Pj'] = df['Pj'].astype(int)
            df['FLOW'] = df['FLOW'].astype(int)

            db = os.path.join(DATABASE_FOLDER, 'zeroinflated_regression.sqlite')
            con = sqlite3.connect(db)
            df.to_sql(name=f'zinb_inputs_Census_1990_{race}_{age_group}',
                    con=con,
                    if_exists='replace',
                    index=False)
            con.close()

            print(f"\tFinished {age_group}...")

    print("\nFinished!")


if __name__ == '__main__':
    main()
