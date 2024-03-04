'''
TODO: Docstring
'''
import os
import sqlite3

import numpy as np
import pandas as pd


if os.path.isdir('D:\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'
elif os.path.isdir('D:\\projects\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\projects\\ICLUS_v3\\population'
else:
    raise Exception
INPUT_FOLDER = os.path.join(BASE_FOLDER, 'inputs')
OUTPUT_FOLDER = os.path.join(BASE_FOLDER, 'outputs')
# ANALYSIS_FOLDER = os.path.join(DISSERTATION_FOLDER, 'analysis')
# DATABASE_FOLDER = os.path.join(DISSERTATION_FOLDER, 'databases')
# PART_3_FOLDER = os.path.join(DISSERTATION_FOLDER, 'analysis\\part_3')
# PART_4_FOLDER = os.path.join(DISSERTATION_FOLDER, 'analysis\\part_4')
# ACS_DB = os.path.join(PART_3_FOLDER, 'inputs\\acs\\acs.sqlite')
# CENSUS_DB = os.path.join(PART_3_FOLDER, 'inputs\\census\\census.sqlite')

AGE_GROUPS = ('0-4', '5-9', '10-14', '15-19', '20-24', '25-29', '30-34',
              '35-39', '40-44', '45-49', '50-54', '55-59', '60-64', '65-69',
              '70-74', '75-79', '80-84', '85+')

COLUMN_MAP = {'count_.Intercept.': 'c_int',
              'count_ln_mi': 'c_ln_mi',
              'count_ln_nj': 'c_ln_nj',
              'count_ln_Cij': 'c_ln_Cij',
              'count_ln_Lij': 'c_ln_Lij',
              'count_ln_sij': 'c_ln_sij',
              'count_ln_Tij': 'c_ln_Tij',
              'count_ln_nj_star': 'c_ln_nj_star',
              'count_factor.SAME_LABOR_MARKET.1': 'c_same_labor_market',
              'count_factor.URBAN_DESTINATION.1': 'c_urban_destination',
              'zero_.Intercept.': 'z_int',
              'zero_ln_mi': 'z_ln_mi',
              'zero_ln_nj': 'z_ln_nj',
              'zero_ln_Cij': 'z_ln_Cij',
              'zero_ln_Lij': 'z_ln_Lij',
              'zero_ln_sij': 'z_ln_sij',
              'zero_ln_Tij': 'z_ln_Tij',
              'zero_ln_nj_star': 'z_ln_nj_star',
              'zero_factor.SAME_LABOR_MARKET.1': 'z_same_labor_market',
              'zero_factor.URBAN_DESTINATION.1': 'z_urban_destination'}


def get_fips_changes():
    '''
    TODO: Docstring
    '''
    query = 'SELECT OLD_FIPS, NEW_FIPS \
             FROM fips_or_name_changes'
    db = os.path.join(BASE_FOLDER, 'inputs', 'databases', 'migration.sqlite')
    con = sqlite3.connect(database=db, timeout=30)
    df = pd.read_sql(sql=query, con=con)
    con.close()

    return df


def get_age_weighted_population(race_pop, age_group):
    '''
    TODO: Docstring
    '''

    # the Census data don't include migration for <5, so substitue the 5-9
    # age group correlation
    age_group_label = age_group.replace('-', '_TO_')
    if age_group == '0-4':
        age_group_label = '5_TO_9'
    if age_group == '85+':
        age_group_label = '85_AND_OVER'

    db = os.path.join(INPUT_FOLDER, 'part_2_c_i_3', 'correlations.sqlite')
    con = sqlite3.connect(database=db, timeout=30)
    query = f'SELECT POPULATION_AGE_GROUP AS AGE_GROUP, RHO AS WEIGHT \
              FROM Census1990_ALL_AGE_CORRELATIONS \
              WHERE MIGRATION_AGE_GROUP = "{age_group_label}"'
    weights = pd.read_sql(sql=query, con=con)
    con.close()

    # rename some things and add the 0-4 age group
    weights.AGE_GROUP = weights.AGE_GROUP.str.replace('_TO_', '-')
    weights.AGE_GROUP = weights.AGE_GROUP.str.replace('85_AND_OVER', '85+')
    val_5_9 = weights.query('AGE_GROUP == "5-9"').WEIGHT.values[0]
    row = pd.DataFrame.from_dict(data={'AGE_GROUP': ('0-4',), 'WEIGHT': (val_5_9,)})
    weights = pd.concat(objs=[weights, row],
                        ignore_index=True,
                        verify_integrity=True).set_index(keys='AGE_GROUP')
    weights['WEIGHT'] = weights['WEIGHT'] / weights['WEIGHT'].max()  # standardize weights so that the largest weight is 1.0

    df = race_pop.reset_index().merge(right=weights,
                                      how='left',
                                      on='AGE_GROUP',
                                      copy=False)
    assert not df.isnull().any().any()

    # numerator
    df['WEIGHT_x_POPULATION'] = df['WEIGHT'] * df['VALUE']
    df['SUM_WEIGHT_x_POPULATION'] = df.groupby('GEOID')['WEIGHT_x_POPULATION'].transform('sum')

    # denomenator
    df['SUM_WEIGHTS'] = df.groupby(by='GEOID')['WEIGHT'].transform('sum')

    # quotient
    df['WEIGHTED_POPULATION'] = df['SUM_WEIGHT_x_POPULATION'] / df['SUM_WEIGHTS']

    df = df[['GEOID', 'WEIGHTED_POPULATION']].groupby(by='GEOID').max()
    df['WEIGHTED_POPULATION'] = df['WEIGHTED_POPULATION'].round().astype(int)

    return df


def get_intra_labor_market_moves():
    db = os.path.join(INPUT_FOLDER, 'databases', 'migration.sqlite')
    query = 'SELECT COFIPS, BEA10 \
             FROM county_to_BEA10'
    con = sqlite3.connect(database=db, timeout=30)
    df = pd.read_sql(sql=query, con=con)
    con.close()

    return df


class migration_2_c_ii_3_a():
    '''
    Pull the coefficients of a zeroinflated negative bionomical regression model
    fit to 1990 Census data.
    '''
    def __init__(self):

        self.model_name = '2_c_ii_3_a'

        self.current_pop = None
        self.coefs = None
        self.fips_changes = None

        self.ALPHA = 0.05

        self.retrieve_coefficients()
        self.fips_changes = get_fips_changes()
        self.intra_labor_market = get_intra_labor_market_moves()
        self.urban_counties = self.get_urban_counties()
        self.distance = self.get_euclidean_distance()

    def retrieve_coefficients(self):
        '''
        Query a SQLite database for the correct coefficients, format them and
        then set the result as self.coefficients
        '''
        p = os.path.join(INPUT_FOLDER, 'part_2_c_ii_3_a')
        db = os.path.join(p, 'zeroinfl_outputs.sqlite')
        con = sqlite3.connect(database=db, timeout=30)
        query = 'SELECT * FROM coefficients_Census_1990'
        coefs = pd.read_sql(sql=query, con=con)
        coefs['AGE_GROUP'] = coefs['AGE_GROUP'].str.replace('_TO_', '-')
        coefs['AGE_GROUP'] = coefs['AGE_GROUP'].str.replace('85_AND_OVER', '85+')
        coefs.set_index(keys=['RACE', 'AGE_GROUP'], inplace=True)
        coefs.drop(columns=['DIST', 'LINK'], inplace=True)
        coefs.rename(columns=COLUMN_MAP, inplace=True)
        coefs = coefs.melt(var_name='VARIABLE',
                           value_name='COEFF',
                           ignore_index=False)
        coefs.set_index(keys='VARIABLE', append=True, inplace=True)

        query = 'SELECT * FROM significance_Census_1990'
        sigs = pd.read_sql(sql=query, con=con)
        con.close()
        sigs['AGE_GROUP'] = sigs['AGE_GROUP'].str.replace('_TO_', '-')
        sigs['AGE_GROUP'] = sigs['AGE_GROUP'].str.replace('85_AND_OVER', '85+')
        sigs.set_index(keys=['RACE', 'AGE_GROUP'], inplace=True)
        sigs.drop(columns=['DIST', 'LINK', 'CONVERGED'], inplace=True)
        sigs.rename(columns=COLUMN_MAP, inplace=True)
        sigs = sigs.melt(var_name='VARIABLE',
                         value_name='P_VALUE',
                         ignore_index=False)
        sigs.set_index(keys='VARIABLE', append=True, inplace=True)

        df = coefs.join(other=sigs)

        # for now keep all of the intercepts regardless of statistical significance;
        # otherwise set the coefficient to 0 when P_VALUE >- ALPHA
        # df.loc[(df.P_VALUE >= self.ALPHA) & (df.AGE_GROUP != '85+') & (~df.index.get_level_values('VARIABLE').isin(['c_int', 'z_int'])), 'COEFF'] = 0.0
        self.coefs = df.copy()

    def compute_migrants(self, race):
        '''
        Placeholder
        '''
        race_pop = self.current_pop.query('RACE == @race').copy()
        gross_migration_flows = None

        #for age_group in ('0-4', '20-24', '85+'):
        for age_group in AGE_GROUPS:
            print(f"\t\t{age_group}")
            df = self.distance.copy()
            age_weighted_population = get_age_weighted_population(race_pop=race_pop, age_group=age_group)

            age_pop = race_pop.query('AGE_GROUP == @age_group').groupby(by='GEOID').sum()
            df = self.compute_spatial_variables(age_pop=age_pop, age_weighted_population=age_weighted_population)

            race_label = race
            if race not in ('BLACK', 'WHITE'):
                race_label = 'OTHER'

            age_group_label = age_group
            if age_group in ('0-4', '5-9'):
                age_group_label = '0-9'

            coefs = self.coefs.query('RACE == @race_label & AGE_GROUP == @age_group_label')

            assert coefs.shape == (16, 2)

            # calculate the zero model first
            z_int = coefs.query('VARIABLE == "z_int"').COEFF.iloc[0]
            z_Pi = coefs.query('VARIABLE == "z_ln_mi"').COEFF.iloc[0]
            z_Pj = coefs.query('VARIABLE == "z_ln_nj"').COEFF.iloc[0]
            z_Cij = coefs.query('VARIABLE == "z_ln_Cij"').COEFF.iloc[0]
            z_Tij = coefs.query('VARIABLE == "z_ln_Tij"').COEFF.iloc[0]
            z_Pj_star = coefs.query('VARIABLE == "z_ln_nj_star"').COEFF.iloc[0]
            z_labor = coefs.query('VARIABLE == "z_same_labor_market"').COEFF.iloc[0]
            z_urban = coefs.query('VARIABLE == "z_urban_destination"').COEFF.iloc[0]

            df['ZERO_RESULT'] = 1 - np.exp(-np.exp(z_int +
                                                   (z_Pi * np.log(df['Pi'] + 1)) +
                                                   (z_Pj * np.log(df['Pj'] + 1)) +
                                                   (z_Cij * np.log(df['Cij'] + df['Pj'] + 1)) +
                                                   (z_Tij * np.log(df['Tij'] + 1)) +
                                                   (z_Pj_star * np.log(df['Pj_star'] + 1)) +
                                                   (z_labor * df['SAME_LABOR_MARKET']) +
                                                   (z_urban * df['URBAN_DESTINATION'])))

            # calculate the count model
            c_int = coefs.query('VARIABLE == "c_int"').COEFF.iloc[0]
            c_Pi = coefs.query('VARIABLE == "c_ln_mi"').COEFF.iloc[0]
            c_Pj = coefs.query('VARIABLE == "c_ln_nj"').COEFF.iloc[0]
            c_Cij = coefs.query('VARIABLE == "c_ln_Cij"').COEFF.iloc[0]
            c_Tij = coefs.query('VARIABLE == "c_ln_Tij"').COEFF.iloc[0]
            c_Pj_star = coefs.query('VARIABLE == "c_ln_nj_star"').COEFF.iloc[0]
            c_labor = coefs.query('VARIABLE == "c_same_labor_market"').COEFF.iloc[0]
            c_urban = coefs.query('VARIABLE == "c_urban_destination"').COEFF.iloc[0]

            df['COUNT_RESULT'] = np.exp(c_int +
                                        (c_Pi * np.log(df['Pi'] + 1)) +
                                        (c_Pj * np.log(df['Pj'] + 1)) +
                                        (c_Cij * np.log(df['Cij'] + df['Pj'] + 1)) +
                                        (c_Tij * np.log(df['Tij'] + 1)) +
                                        (c_Pj_star * np.log(df['Pj_star'] + 1)) +
                                        (c_labor * df['SAME_LABOR_MARKET']) +
                                        (c_urban * df['URBAN_DESTINATION']))
            # this rounding step preserves >99% of the total migration just calculated
            df['MIGRATION'] = ((1 - df['ZERO_RESULT']) * df['COUNT_RESULT']).round(2)
            df = df.loc[:, ['ORIGIN_FIPS', 'DESTINATION_FIPS', 'MIGRATION']]
            df.set_index(keys=['ORIGIN_FIPS', 'DESTINATION_FIPS'], inplace=True)
            df.columns = ['MIGRATION']
            df['AGE_GROUP'] = age_group
            df.set_index(keys='AGE_GROUP',
                         append=True,
                         inplace=True,
                         verify_integrity=True)

            if gross_migration_flows is None:
                gross_migration_flows = df.copy()
            else:
                gross_migration_flows = pd.concat(objs=[gross_migration_flows, df],
                                                  verify_integrity=True,
                                                  copy=False)

        return gross_migration_flows

    def compute_spatial_variables(self, age_pop, age_weighted_population):
        '''
        Placeholder
        '''
        # origin population
        df = self.distance.merge(right=age_pop,
                                 how='left',
                                 left_on='ORIGIN_FIPS',
                                 right_on='GEOID')
        df.rename(columns={'VALUE': 'Pi'}, inplace=True)

        # destination population (i.e., age group-weighted population)
        df = df.merge(right=age_weighted_population,
                      how='left',
                      left_on='DESTINATION_FIPS',
                      right_on='GEOID')
        df.rename(columns={'WEIGHTED_POPULATION': 'Pj'}, inplace=True)

        # calculate total BEA population minus destination
        df.loc[df['SAME_LABOR_MARKET'] == 1, 'Pj_star'] = df['Pj']
        df['Pj_star'].fillna(value=0, inplace=True)
        df['Pj_star'] = df.groupby(by='ORIGIN_FIPS')['Pj_star'].transform('sum').astype(int)
        dw = df[['ORIGIN_FIPS', 'Pj_star']].copy()
        df.drop(columns=['Pj_star'], inplace=True)
        dw.drop_duplicates(inplace=True, ignore_index=True)
        dw.rename(columns={'ORIGIN_FIPS': 'DESTINATION_FIPS'}, inplace=True)
        df = df.merge(right=dw, how='left', on='DESTINATION_FIPS')

        assert not df.isnull().any().any()

        # distance-weighted Intervening Opportunities
        df.sort_values(by=['ORIGIN_FIPS', 'Dij'], inplace=True)
        df['PPD'] = df.Pj / df.Dij  # 'people per unit of distance'
        df['Tij'] = df.groupby('ORIGIN_FIPS')['PPD'].transform(lambda x: x.shift(1).cumsum()).fillna(0)

        # Competing Migrants
        df.sort_values(by=['DESTINATION_FIPS', 'Dij'], inplace=True)
        df['Cij'] = df.groupby('DESTINATION_FIPS')['Pi'].transform(lambda x: x.shift(1).cumsum()).fillna(0).astype(int)

        return df

    def get_euclidean_distance(self):
        db = os.path.join(INPUT_FOLDER, 'databases', 'analysis.sqlite')
        con = sqlite3.connect(db)
        query = 'SELECT ORIGIN_FIPS, DESTINATION_FIPS, Dij \
                 FROM county_to_county_distance_2010'
        df = pd.read_sql(sql=query, con=con)
        con.close()
        assert not df.isnull().any().any()

        # label intra-labor market moves
        df = df.merge(right=self.intra_labor_market,
                      how='left',
                      left_on='ORIGIN_FIPS',
                      right_on='COFIPS',
                      copy=False)
        df.rename(columns={'BEA10': 'ORIGIN_BEA10'}, inplace=True)
        df.drop(columns='COFIPS', inplace=True)

        df = df.merge(right=self.intra_labor_market,
                      how='left',
                      left_on='DESTINATION_FIPS',
                      right_on='COFIPS')
        df.rename(columns={'BEA10': 'DESTINATION_BEA10'}, inplace=True)
        assert not df.isnull().any().any()
        df['SAME_LABOR_MARKET'] = 0
        df.loc[df.ORIGIN_BEA10 == df.DESTINATION_BEA10, 'SAME_LABOR_MARKET'] = 1
        assert not df.isnull().any().any()
        df.drop(columns=['COFIPS', 'ORIGIN_BEA10', 'DESTINATION_BEA10'], inplace=True)

        # label destinations that overlap Census Urban Areas (not including
        # Urban Clusters)
        df = df.merge(right=self.urban_counties,
                      how='left',
                      left_on='DESTINATION_FIPS',
                      right_on='GEOID')
        df['URBAN_DESTINATION'] = df['URBAN_DESTINATION'].astype(int)
        df.drop(columns='GEOID', inplace=True)
        assert not df.isnull().any().any()

        return df

    def get_urban_counties(self):
        db = os.path.join(INPUT_FOLDER, 'databases', 'migration.sqlite')
        query = 'SELECT GEOID, UA10 AS URBAN_DESTINATION \
                 FROM ua10_counties'
        con = sqlite3.connect(db)
        df = pd.read_sql(sql=query, con=con)
        con.close()

        # make sure GEOID is updated
        df = df.merge(right=self.fips_changes,
                      how='left',
                      left_on='GEOID',
                      right_on='OLD_FIPS')
        df.loc[~pd.isnull(df['NEW_FIPS']), 'GEOID'] = df['NEW_FIPS']
        df.drop(labels=['OLD_FIPS', 'NEW_FIPS'], axis=1, inplace=True)

        df = df.groupby(by='GEOID', as_index=False).max()

        return df
