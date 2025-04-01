'''
20200204 - update to divide migration flows by five, i.e., annual average

To be added:

 - destination population weighted by correlation coefficients from 2_c_i_3
 - same-race population in destination economic area minus same-race population
   in destination county
'''

import os
import sqlite3

import numpy as np
import pandas as pd

ICLUS_FOLDER = 'D:\\OneDrive\\ICLUS_v3'
ANALYSIS_DB = os.path.join(ICLUS_FOLDER, 'population', 'inputs', 'databases', 'analysis.sqlite')
MIGRATION_DB = os.path.join(ICLUS_FOLDER, 'population', 'inputs', 'databases',  'migration.sqlite')

# FIPS_CHANGES = pd.read_sql_query(sql=query1, con=con1)


AGE_GROUPS = ('0_TO_9',
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
              '85_AND_OVER')


def get_migration(race=None, age_group=None):
    db = os.path.join(DATABASE_FOLDER, 'migration.sqlite')
    if age_group == '0_TO_9':
        query = f'SELECT ORIGIN_FIPS, DESTINATION_FIPS, FLOW, RACE, AGE \
                  FROM census_enhanced_migration_1985_1990 \
                  WHERE RACE = "{race}" \
                  AND AGE IN ("0_TO_4", "5_TO_9")'
    elif age_group == '85_AND_OVER':
        query = f'SELECT ORIGIN_FIPS, DESTINATION_FIPS, FLOW, RACE, AGE \
                  FROM census_enhanced_migration_1985_1990 \
                  WHERE RACE = "{race}" \
                  AND AGE = "85_TO_115"'
    else:
        query = f'SELECT ORIGIN_FIPS, DESTINATION_FIPS, FLOW, RACE, AGE \
                  FROM census_enhanced_migration_1985_1990 \
                  WHERE RACE = "{race}" \
                  AND AGE = "{age_group}"'
    con = sqlite3.connect(db)
    df = pd.read_sql_query(sql=query, con=con)
    con.close()

    # ignore intra-county movers
    df = df.loc[df['ORIGIN_FIPS'] != df['DESTINATION_FIPS'], :]

    # ignore non-movers (over age 5)
    df = df.loc[df.DESTINATION_FIPS != '00000', :]

    # ignore movers that were abroad in 1990
    df = df.loc[df.DESTINATION_FIPS != '99999', :]

    # this is only really needed for the youngest and oldest age groups
    df['AGE'] = age_group

    # make sure ORIGIN_FIPS is updated
    df = df.merge(right=FIPS_CHANGES, how='left', left_on='ORIGIN_FIPS', right_on='OLD_FIPS', copy=False)
    df.loc[~pd.isnull(df['NEW_FIPS']), 'ORIGIN_FIPS'] = df['NEW_FIPS']
    df.drop(labels=['OLD_FIPS', 'NEW_FIPS'], axis=1, inplace=True)

    df = df.merge(right=FIPS_CHANGES, how='left', left_on='DESTINATION_FIPS', right_on='OLD_FIPS', copy=False)
    df.loc[~pd.isnull(df['NEW_FIPS']), 'DESTINATION_FIPS'] = df['NEW_FIPS']
    df.drop(labels=['OLD_FIPS', 'NEW_FIPS'], axis=1, inplace=True)

    df = df.groupby(['ORIGIN_FIPS', 'DESTINATION_FIPS', 'RACE', 'AGE']).sum()
    assert not df.isnull().any().any()

    df = df.unstack(level='RACE', fill_value=0)
    df.columns = df.columns.get_level_values(1)
    df.columns.name = None
    df = df.astype(int)

    df = (df / 5.0).round(0).astype(int)

    df.reset_index(inplace=True)

    return df


def get_euclidean_distance():
    query = 'SELECT ORIGIN_FIPS, DESTINATION_FIPS, Dij \
             FROM county_to_county_distance_2020'
    con = sqlite3.connect(ANALYSIS_DB)
    df = pd.read_sql_query(sql=query, con=con)
    con.close()

    assert not df.isnull().any().any()

    return df


def get_population(race=None, age_group=None):
    print("get_population()")

    RACE_MAP = {'Black female': 'BLACK',
                'Black male': 'BLACK',
                'White female': 'WHITE',
                'White male': 'WHITE',
                'Other races female': 'OTHER',
                'Other races male': 'OTHER'}

    db = os.path.join(DATABASE_FOLDER, 'population.sqlite')

    if age_group == '0_TO_9':
        query = 'SELECT COFIPS, RACE_SEX, LT_5, "5_TO_9" \
                 FROM county_population_agerace_1980_to_1989 \
                 WHERE YEAR = 1985'
    else:
        query = f'SELECT COFIPS, RACE_SEX, "{age_group}" \
                  FROM county_population_agerace_1980_to_1989 \
                  WHERE YEAR = 1985'

    con = sqlite3.connect(db)
    df = pd.read_sql_query(sql=query, con=con)
    con.close()

    if age_group == '0_TO_9':
        df['POPULATION'] = df['LT_5'] + df['5_TO_9']
        df.drop(columns=['LT_5', '5_TO_9'], inplace=True)
    else:
        df.rename(columns={age_group: 'POPULATION'}, inplace=True)

    df['RACE_SEX'].replace(to_replace=RACE_MAP, inplace=True)
    df.rename(columns={'RACE_SEX': 'RACE'}, inplace=True)
    df = df.loc[df['RACE'] == race, :]
    df = df.groupby(by=['COFIPS', 'RACE'], as_index=False).sum()

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


def get_age_weighted_population(race=None, age_group=None):
    print("get_age_weighted_population()")

    RACE_MAP = {'Black female': 'BLACK',
                'Black male': 'BLACK',
                'White female': 'WHITE',
                'White male': 'WHITE',
                'Other races female': 'OTHER',
                'Other races male': 'OTHER'}

    db = os.path.join(DATABASE_FOLDER, 'population.sqlite')

    con = sqlite3.connect(db)
    df = pd.read_sql_query('SELECT * \
                            FROM county_population_agerace_1980_to_1989 \
                            WHERE YEAR = 1985', con=con)
    con.close()
    df.drop(columns='YEAR', inplace=True)

    df['RACE_SEX'].replace(to_replace=RACE_MAP, inplace=True)
    df.rename(columns={'RACE_SEX': 'RACE'}, inplace=True)
    df = df.groupby(by=['COFIPS', 'RACE'], as_index=False).sum()

    df = df.merge(right=FIPS_CHANGES, how='left', left_on='COFIPS', right_on='OLD_FIPS', copy=False)
    df.loc[~df.NEW_FIPS.isnull(), 'COFIPS'] = df['NEW_FIPS']
    df.drop(labels=['OLD_FIPS', 'NEW_FIPS'], axis=1, inplace=True)
    df = df.groupby(by=['COFIPS', 'RACE'], as_index=False).sum()

    # ignore APO/FPO, FOREIGN, and ALL OTHER FLOWS
    df = df[~df['COFIPS'].isin(['57005', '57009', '59999'])]

    # adjust columns names so they match migration
    df.rename(columns={'LT_5': '0_TO_9', '5_TO_9': '0_TO_9'}, inplace=True)

    df = df.melt(id_vars=['COFIPS', 'RACE'], var_name='AGE_GROUP', value_name='POPULATION')
    df = df.groupby(by=['COFIPS', 'RACE', 'AGE_GROUP'], as_index=False).sum()
    df = df.loc[df.RACE == race, :]
    assert not df.isnull().any().any()

    # retrieve the race-age weights
    if age_group == '0_TO_9':
        age_group = '5_TO_9'
    db = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_2_c_i_3', 'outputs', 'correlations.sqlite')
    con = sqlite3.connect(db)
    query = f'SELECT POPULATION_AGE_GROUP AS AGE_GROUP, RHO AS WEIGHT \
              FROM Census1990_ALL_AGE_CORRELATIONS \
              WHERE MIGRATION_AGE_GROUP = "{age_group}"'
    weights = pd.read_sql(sql=query, con=con)
    con.close()
    weights.loc[weights.AGE_GROUP == '5_TO_9', 'AGE_GROUP'] = '0_TO_9'

    df = df.merge(right=weights, how='left', on='AGE_GROUP', copy=False)
    assert not df.isnull().any().any()

    df['WEIGHT'] = df['WEIGHT'] / df['WEIGHT'].max()  # standardize weights so that the largest weight is 1.0

    # numerator
    df['WEIGHT_x_POPULATION'] = df['WEIGHT'] * df['POPULATION']
    df['SUM_WEIGHT_x_POPULATION'] = df.groupby('COFIPS')['WEIGHT_x_POPULATION'].transform('sum')

    # denomenator
    df['SUM_WEIGHTS'] = df.groupby(by='COFIPS')['WEIGHT'].transform('sum')

    # quotient
    df['WEIGHTED_POPULATION'] = df['SUM_WEIGHT_x_POPULATION'] / df['SUM_WEIGHTS']

    df = df[['COFIPS', 'WEIGHTED_POPULATION']].groupby(by='COFIPS', as_index=False).max()
    df['WEIGHTED_POPULATION'] = df['WEIGHTED_POPULATION'].round().astype(int)

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


def merge_dataframes(distance_df=None, population_df=None, migration_df=None,
                     intra_labor_market_df=None, urban_counties_df=None,
                     age_weighted_population_df=None, race=None):
    """Combines DataFrames holding distance, population, and migration data

    Args:
        distance_df: A DataFrame.
        census_df: A DataFrame.
        irs_df: A DataFrame.

    Returns:
        A DataFrame.

    Raises:

    """
    print("merge_dataframes()")

    migration_df.rename(columns={race: f'MIG_{race}'},
                        inplace=True)

    population_df.rename(columns={race: f'POP_{race}'},
                         inplace=True)

    # origin population
    df = distance_df.merge(right=migration_df,
                           how='left',
                           on=['ORIGIN_FIPS', 'DESTINATION_FIPS'])
    df = df.merge(right=population_df,
                  how='left',
                  left_on='ORIGIN_FIPS',
                  right_on='COFIPS')
    df.drop(['COFIPS'], axis=1, inplace=True)
    df.rename(columns={f'POP_{race}': 'mi', f'MIG_{race}': 'FLOW'}, inplace=True)
    df['FLOW'] = df.FLOW.fillna(0).astype(int)

    # destination population (i.e., age group-weighted population)
    df = df.merge(right=age_weighted_population_df,
                  how='left',
                  left_on='DESTINATION_FIPS',
                  right_on='COFIPS')
    df.drop(columns='COFIPS', inplace=True)
    df.rename(columns={'WEIGHTED_POPULATION': 'nj'}, inplace=True)

    # label intra-labor market moves
    df = df.merge(right=intra_labor_market_df,
                  how='left',
                  left_on='ORIGIN_FIPS',
                  right_on='COFIPS',
                  copy=False)
    df.rename(columns={'BEA10': 'ORIGIN_BEA10'}, inplace=True)
    df.drop(columns='COFIPS', inplace=True)

    df = df.merge(right=intra_labor_market_df,
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

    # calculate total BEA population minus destination
    df.loc[df['SAME_LABOR_MARKET'] == 1, 'nj_star'] = df['nj']
    df['nj_star'].fillna(value=0, inplace=True)
    df['nj_star'] = df.groupby(by='ORIGIN_FIPS')['nj_star'].transform('sum').astype(int)
    dw = df[['ORIGIN_FIPS', 'nj_star']]
    df.drop(columns=['nj_star'], inplace=True)
    dw.drop_duplicates(inplace=True, ignore_index=True)
    dw.rename(columns={'ORIGIN_FIPS': 'DESTINATION_FIPS'}, inplace=True)
    df = df.merge(right=dw, how='left', on='DESTINATION_FIPS')

    # label destinations that overlap Census Urban Areas (not including Urban
    # Clusters)
    df = df.merge(right=urban_counties_df,
                  how='left',
                  left_on='DESTINATION_FIPS',
                  right_on='GEOID')
    df['URBAN_DESTINATION'] = df['URBAN_DESTINATION'].astype(int)
    df.drop(columns='GEOID', inplace=True)

    assert not df.isnull().any().any()

    return df


def get_intra_labor_market_moves():
    db = os.path.join(DATABASE_FOLDER, 'migration.sqlite')
    query = 'SELECT COFIPS, BEA10 \
             FROM county_to_BEA10'
    con = sqlite3.connect(db)
    df = pd.read_sql_query(sql=query, con=con)
    con.close()

    return df


def get_urban_counties():
    db = os.path.join(DATABASE_FOLDER, 'migration.sqlite')
    query = 'SELECT GEOID, UA10 AS URBAN_DESTINATION \
             FROM ua10_counties'
    con = sqlite3.connect(db)
    df = pd.read_sql_query(sql=query, con=con)
    con.close()

    # make sure GEOID is updated
    df = df.merge(right=FIPS_CHANGES, how='left', left_on='GEOID', right_on='OLD_FIPS')
    df.loc[~pd.isnull(df['NEW_FIPS']), 'GEOID'] = df['NEW_FIPS']
    df.drop(labels=['OLD_FIPS', 'NEW_FIPS'], axis=1, inplace=True)

    df = df.groupby(by='GEOID', as_index=False).max()

    return df


def main():
    distance_df = get_euclidean_distance()
    intra_labor_market_df = get_intra_labor_market_moves()
    urban_counties_df = get_urban_counties()

    for race in ('WHITE', 'BLACK', 'OTHER'):
        for age_group in AGE_GROUPS:
            population_df = get_population(race=race, age_group=age_group)
            migration_df = get_migration(race=race, age_group=age_group)
            age_weighted_population_df = get_age_weighted_population(race=race, age_group=age_group)
            np.random.seed(20121020)

            output_name = 'gravity_inputs_{}_Census_1990_{}_{}'

            migration_cols = ['ORIGIN_FIPS', 'DESTINATION_FIPS', race]
            df = merge_dataframes(distance_df=distance_df,
                                  population_df=population_df[['COFIPS', race]],
                                  migration_df=migration_df[migration_cols],
                                  intra_labor_market_df=intra_labor_market_df,
                                  urban_counties_df=urban_counties_df,
                                  age_weighted_population_df=age_weighted_population_df,
                                  race=race)

            # variables calculated from the perspective of the origin
            df.sort_values(by=['ORIGIN_FIPS', 'Dij'], inplace=True)

            # Intervening Opportunities
            df['sij'] = df.groupby('ORIGIN_FIPS')['nj'].transform(lambda x: x.shift(1).cumsum()).fillna(0).astype(int)

            # distance-weighted Intervening Opportunities
            df['PPD'] = df.nj / df.Dij  # 'people per unit of distance'
            m = df.groupby('ORIGIN_FIPS')['PPD'].transform('sum')
            df['Tij'] = df.groupby('ORIGIN_FIPS')['PPD'].transform(lambda x: x.shift(1).cumsum()).fillna(0)

            # variables calculated from the persepctive of the destination
            df.sort_values(by=['DESTINATION_FIPS', 'Dij'], inplace=True)

            # Competing Migrants
            df['Cij'] = df.groupby('DESTINATION_FIPS')['mi'].transform(lambda x: x.shift(1).cumsum()).fillna(0).astype(int)

            columns = ['ORIGIN_FIPS', 'DESTINATION_FIPS', 'Tij', 'Cij', 'mi', 'nj', 'FLOW']
            df.sort_values(by=['ORIGIN_FIPS', 'DESTINATION_FIPS'], inplace=True)

            df['random_number'] = np.random.random(len(df))
            train = df.loc[df['random_number'] <= 0.8, columns]
            test = df.loc[df['random_number'] > 0.8, columns]

            db = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_2_c_ii_3_a', 'inputs', 'part_2_c_ii_3_a_inputs.sqlite')
            con = sqlite3.connect(db)

            train.to_sql(name=output_name.format('train', race, age_group),
                         con=con,
                         if_exists='replace',
                         index=False)
            test.to_sql(name=output_name.format('test', race, age_group),
                        con=con,
                        if_exists='replace',
                        index=False)

            con.close()

            print(race, "+", age_group, " complete....")

    print("Finished!")


if __name__ == '__main__':
    main()
