'''
TODO: Docstring
'''
import os

import numpy as np
import polars as pl


BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'
if os.path.isdir('D:\\projects\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\projects\\ICLUS_v3\\population'
INPUT_FOLDER = os.path.join(BASE_FOLDER, 'inputs')
OUTPUT_FOLDER = os.path.join(BASE_FOLDER, 'outputs')
ANALYSIS_DB = os.path.join(INPUT_FOLDER, 'databases', 'analysis.sqlite')
MIGRATION_DB = os.path.join(INPUT_FOLDER, 'databases', 'migration.sqlite')

AGE_GROUPS = ('0-4', '5-9', '10-14', '15-19', '20-24', '25-29', '30-34',
              '35-39', '40-44', '45-49', '50-54', '55-59', '60-64', '65-69',
              '70-74', '75-79', '80-84', '85+')

COLUMN_MAP = {'count_.Intercept.': 'c_int',
              'count_ln_mi': 'c_ln_mi',
              'count_ln_nj': 'c_ln_nj',
              'count_ln_Cij': 'c_ln_Cij',
              'count_ln_Tij': 'c_ln_Tij',
              'count_ln_nj_star': 'c_ln_nj_star',
              'count_factor.SAME_LABOR_MARKET.1': 'c_same_labor_market',
              'count_factor.URBAN_DESTINATION.1': 'c_urban_destination',
              'zero_.Intercept.': 'z_int',
              'zero_ln_mi': 'z_ln_mi',
              'zero_ln_nj': 'z_ln_nj',
              'zero_ln_Cij': 'z_ln_Cij',
              'zero_ln_Tij': 'z_ln_Tij',
              'zero_ln_nj_star': 'z_ln_nj_star',
              'zero_factor.SAME_LABOR_MARKET.1': 'z_same_labor_market',
              'zero_factor.URBAN_DESTINATION.1': 'z_urban_destination'}


class migration_plum_v3():
    '''
    Pull the coefficients of a zeroinflated negative bionomical regression model
    fit to 1990 Census data.
    '''
    def __init__(self):

        self.model_name = 'PLUMv0'

        self.current_pop = None
        self.coefs = None


        self.ALPHA = 0.05

        self.retrieve_coefficients()
        self.intra_labor_market = self.get_intra_labor_market_moves()
        self.urban_counties = self.get_urban_counties()
        self.distance = self.get_euclidean_distance()

    def retrieve_coefficients(self):
        '''
        Query a SQLite database for the correct coefficients, format them and
        then set the result as self.coefficients
        '''
        # set up the regression coefficients
        # uri = f"sqlite:{os.path.join(INPUT_FOLDER, 'databases', 'zeroinflated_regression.sqlite')}"
        uri = r"sqlite: \D:\\OneDrive\\ICLUS_v3\\population\\inputs\\old\\part_2_c_ii_3_a\\zeroinfl_outputs.sqlite"
        query = 'SELECT * FROM coefficients_Census_1990'
        coefs = pl.read_database_uri(query=query, uri=uri)

        coefs = pl.read_database_uri(query=query, uri=uri)
        coefs = (coefs.with_columns(pl.col('AGE_GROUP').str.replace('_TO_', '-')
                      .alias('AGE_GROUP')))
        coefs = (coefs.with_columns(pl.col('AGE_GROUP').str.replace('85_AND_OVER', '85+')
                      .alias('AGE_GROUP')))
        coefs = coefs.drop(['DIST', 'LINK'])
        coefs.rename(COLUMN_MAP)
        coefs = coefs.melt(id_vars=['RACE', 'AGE_GROUP'],
                           variable_name='VARIABLE',
                           value_name='COEFF')

        # set up the signifcance terms, which vary somewhat from race to race
        query = 'SELECT * FROM significance_Census_1990'
        sigs = pl.read_database_uri(query=query, uri=uri)
        sigs = (sigs.with_columns(pl.col('AGE_GROUP').str.replace('_TO_', '-')
                   .alias('AGE_GROUP')))
        sigs = (sigs.with_columns(pl.col('AGE_GROUP').str.replace('85_AND_OVER', '85+')
                    .alias('AGE_GROUP')))
        sigs = sigs.drop(['DIST', 'LINK', 'CONVERGED'])
        sigs.rename(COLUMN_MAP)
        sigs = sigs.melt(id_vars=['RACE', 'AGE_GROUP'],
                         variable_name='VARIABLE',
                         value_name='P_VALUE')

        df = coefs.join(other=sigs,
                        on=['RACE', 'AGE_GROUP', 'VARIABLE'],
                        how='left')

        # for now keep all of the intercepts regardless of statistical significance;
        # otherwise set the coefficient to 0 when P_VALUE >- ALPHA
        # df.loc[(df.P_VALUE >= self.ALPHA) & (df.AGE_GROUP != '85+') & (~df.index.get_level_values('VARIABLE').isin(['c_int', 'z_int'])), 'COEFF'] = 0.0
        self.coefs = df.clone()

    def compute_migrants(self, race):
        '''
        Placeholder
        '''
        race_pop = (self.current_pop
                    .filter(pl.col('RACE') == race)
                    .select(['GEOID', 'POPULATION'])
                    .group_by('GEOID')
                    .sum())

        gross_migration_flows = None

        # for age_group in ('50-54',):
        for age_group in AGE_GROUPS:
            print(f"\t\t{age_group}")

            age_pop = (self.current_pop
                       .filter((pl.col('RACE') == race) & (pl.col('AGE_GROUP') == age_group))
                       .select(['GEOID', 'POPULATION'])
                       .group_by('GEOID')
                       .sum())

            df = self.compute_spatial_variables(age_pop=age_pop, race_pop=race_pop)

            race_label = race
            if race not in ('BLACK', 'WHITE'):
                race_label = 'OTHER'

            age_group_label = age_group
            if age_group in ('0-4', '5-9'):
                age_group_label = '0-9'

            coefs = self.coefs.filter((pl.col('RACE') == race_label) & (pl.col('AGE_GROUP') == age_group_label))
            assert coefs.shape == (16, 5)

            # calculate the zero model first
            z_int = coefs.filter(pl.col('VARIABLE') == 'zero_.Intercept.')['COEFF'][0]
            z_Pi = coefs.filter(pl.col('VARIABLE') == 'zero_ln_mi')['COEFF'][0]
            z_Pj = coefs.filter(pl.col('VARIABLE') == 'zero_ln_nj')['COEFF'][0]
            z_Cij = coefs.filter(pl.col('VARIABLE') == 'zero_ln_Cij')['COEFF'][0]
            z_Tij = coefs.filter(pl.col('VARIABLE') == 'zero_ln_Tij')['COEFF'][0]
            z_Pj_star = coefs.filter(pl.col('VARIABLE') == 'zero_ln_nj_star')['COEFF'][0]

            z_labor = coefs.filter(pl.col('VARIABLE') == 'zero_factor.SAME_LABOR_MARKET.1')['COEFF'][0]
            z_urban = coefs.filter(pl.col('VARIABLE') == 'zero_factor.URBAN_DESTINATION.1')['COEFF'][0]

            df = df.with_columns(1 - np.exp(-np.exp(z_int +
                                                   (z_Pi * np.log(pl.col('Pi') + 1)) +
                                                   (z_Pj * np.log(pl.col('Pj') + 1)) +
                                                   (z_Cij * np.log(pl.col('Cij') + pl.col('Pj') + 1)) +
                                                   (z_Tij * np.log(pl.col('Tij') + 1)) +
                                                   (z_Pj_star * np.log(pl.col('Pj_star') + 1)) +
                                                   (z_labor * pl.col('SAME_LABOR_MARKET')) +
                                                   (z_urban * pl.col('URBANDESTINATION20')))))
            df = df.rename({'literal': 'ZERO_RESULT'})

            # calculate the count model
            c_int = coefs.filter(pl.col('VARIABLE') == 'count_.Intercept.')['COEFF'][0]
            c_Pi = coefs.filter(pl.col('VARIABLE') == 'count_ln_mi')['COEFF'][0]
            c_Pj = coefs.filter(pl.col('VARIABLE') == 'count_ln_nj')['COEFF'][0]
            c_Cij = coefs.filter(pl.col('VARIABLE') == 'count_ln_Cij')['COEFF'][0]
            c_Tij = coefs.filter(pl.col('VARIABLE') == 'count_ln_Tij')['COEFF'][0]
            c_Pj_star = coefs.filter(pl.col('VARIABLE') == 'count_ln_nj_star')['COEFF'][0]
            c_labor = coefs.filter(pl.col('VARIABLE') == 'count_factor.SAME_LABOR_MARKET.1')['COEFF'][0]
            c_urban = coefs.filter(pl.col('VARIABLE') == 'count_factor.URBAN_DESTINATION.1')['COEFF'][0]

            df = df.with_columns(np.exp(c_int +
                                       (c_Pi * np.log(pl.col('Pi') + 1)) +
                                       (c_Pj * np.log(pl.col('Pj') + 1)) +
                                       (c_Cij * np.log(pl.col('Cij') + pl.col('Pj') + 1)) +
                                       (c_Tij * np.log(pl.col('Tij') + 1)) +
                                       (c_Pj_star * np.log(pl.col('Pj_star') + 1)) +
                                       (c_labor * pl.col('SAME_LABOR_MARKET')) +
                                       (c_urban * pl.col('URBANDESTINATION20'))))
            df = df.rename({'literal': 'COUNT_RESULT'})  #TODO: polars bug

            # this rounding step preserves >99% of the total migration just calculated
            df = df.with_columns(((1 - pl.col('ZERO_RESULT')) * pl.col('COUNT_RESULT')).round(0).cast(pl.Int32).alias('MIGRATION'))
            df = df.select(['ORIGIN_FIPS', 'DESTINATION_FIPS', 'MIGRATION'])
            df = df.with_columns(pl.lit(age_group).cast(pl.Enum(AGE_GROUPS)).alias('AGE_GROUP'))

            df = df.collect()
            assert df.shape[0] == 9781256

            if gross_migration_flows is None:
                gross_migration_flows = df.clone()
            else:
                gross_migration_flows = pl.concat(items=[gross_migration_flows, df])

        return gross_migration_flows

    def compute_spatial_variables(self, age_pop, race_pop):
        '''
        Placeholder
        '''
        # origin population
        df = self.distance.join(other=age_pop,
                                how='left',
                                left_on='ORIGIN_FIPS',
                                right_on='GEOID').lazy()
        df = df.rename({'POPULATION': 'Pi'})

        # destination population (i.e., same race population, all age groups)
        df = df.join(other=race_pop.lazy(),
                     how='left',
                     left_on='DESTINATION_FIPS',
                     right_on='GEOID').lazy()
        df = df.rename({'POPULATION': 'Pj'})

        # calculate total BEA population minus destination
        df = df.with_columns(pl.when(pl.col('SAME_LABOR_MARKET') == 1)
                               .then(pl.col('Pj'))
                               .otherwise(0)
                               .alias('Pj_star')).lazy()

        df = df.with_columns(pl.col('Pj_star')
                               .sum()
                               .over('ORIGIN_FIPS')
                               .alias('Pj_star')).lazy()

        temp = (df.select(['ORIGIN_FIPS', 'Pj_star'])
                .unique()
                .rename({'ORIGIN_FIPS': 'DESTINATION_FIPS'}))
        df = df.drop('Pj_star')
        df = df.join(other=temp, on='DESTINATION_FIPS', how='left')

        assert sum(df.null_count().collect()).item() == 0

        # distance-weighted Intervening Opportunities
        df = (df.sort(by=['ORIGIN_FIPS', 'Dij'])
                .with_columns((pl.col('Pj') / pl.col('Dij'))
                .alias('PPD')))

        df = df.with_columns(pl.col('PPD')
                             .shift(n=1, fill_value=0)
                             .cum_sum()
                             .over('ORIGIN_FIPS')
                             .alias('Tij')).lazy()

        # Competing Migrants
        df = (df.sort(by=['DESTINATION_FIPS', 'Dij'])
                .with_columns(pl.col('Pi')
                              .shift(n=1, fill_value=0)
                              .cum_sum()
                              .over('DESTINATION_FIPS')
                              .alias('Cij'))).lazy()

        return df

    def get_euclidean_distance(self):
        uri = f'sqlite:{ANALYSIS_DB}'
        query = 'SELECT ORIGIN_FIPS, DESTINATION_FIPS, Dij \
                 FROM county_to_county_distance_2010'
        df = pl.read_database_uri(query=query, uri=uri)

        # label intra-labor market moves
        df = df.join(other=self.intra_labor_market,
                     how='left',
                     left_on='ORIGIN_FIPS',
                     right_on='COFIPS')
        df = df.rename({'BEA10': 'ORIGIN_BEA10'})

        df = df.join(other=self.intra_labor_market,
                     how='left',
                     left_on='DESTINATION_FIPS',
                     right_on='COFIPS')
        df = df.rename({'BEA10': 'DESTINATION_BEA10'})
        assert sum(df.null_count()).item() == 0

        df = df.with_columns(pl.lit(0).alias('SAME_LABOR_MARKET'))
        df = df.with_columns(pl.when(pl.col('ORIGIN_BEA10') == pl.col('DESTINATION_BEA10'))
                               .then(1)
                               .otherwise(0)
                               .alias('SAME_LABOR_MARKET'))
        assert sum(df.null_count()).item() == 0

        # label destinations that overlap Census Urban Areas (not including
        # Urban Clusters)
        df = df.join(other=self.urban_counties,
                      how='left',
                      left_on='DESTINATION_FIPS',
                      right_on='COFIPS')
        assert sum(df.null_count()).item() == 0

        return df

    def get_urban_counties(self):
        uri = f'sqlite:{MIGRATION_DB}'
        query = 'SELECT COFIPS, URBANDESTINATION20 \
                 FROM fips_to_urb20_bea10_hhs'
        df = pl.read_database_uri(query=query, uri=uri)

        return df

    def get_intra_labor_market_moves(self):
        uri = f'sqlite:{MIGRATION_DB}'
        query = 'SELECT COFIPS, BEA10 \
                FROM fips_to_urb20_bea10_hhs'
        df = pl.read_database_uri(query=query, uri=uri)

        return df
