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
              'count_ln_Pi': 'c_ln_Pi',
              'count_ln_Pj': 'c_ln_Pj',
              'count_ln_Cij': 'c_ln_Cij',
              'count_ln_Tij': 'c_ln_Tij',
              'count_ln_Pj_star': 'c_ln_Pj_star',
              'count_factor.SAME_LABOR_MARKET.1': 'c_same_labor_market',
              'count_factor.MICRODEST20.1': 'c_micro_destination',
              'count_factor.METRODEST20.1': 'c_metro_destination',
              'zero_.Intercept.': 'z_int',
              'zero_ln_Pi': 'z_ln_Pi',
              'zero_ln_Pj': 'z_ln_Pj',
              'zero_ln_Cij': 'z_ln_Cij',
              'zero_ln_Tij': 'z_ln_Tij',
              'zero_ln_Pj_star': 'z_ln_Pj_star',
              'zero_factor.SAME_LABOR_MARKET.1': 'z_same_labor_market',
              'zero_factor.MICRODEST20.1': 'z_micro_destination',
              'zero_factor.METRODEST20.1': 'z_metro_destination'}

COEF_RACE_MAP = {'WHITE': 'WHITE',
                 'BLACK': 'BLACK',
                 'ASIAN': 'API',
                 'AIAN': 'AIAN',
                 'NHPI': 'API',
                 'TWO_OR_MORE': 'OTHER'}

class migration_plum_v3():
    '''
    Pull the coefficients of a zeroinflated negative bionomical regression model
    fit to 1990 Census data.
    '''
    def __init__(self):

        self.model_name = 'PLUMv0'

        self.current_pop = None
        self.coefs = None


        self.alpha = 0.05

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
        uri = f"sqlite:{os.path.join(OUTPUT_FOLDER, 'zinb_regression_outputs.sqlite')}"
        query = 'SELECT * FROM coefficients_Census_1990'
        coefs = pl.read_database_uri(query=query, uri=uri)

        coefs = pl.read_database_uri(query=query, uri=uri)
        coefs = (coefs.with_columns(pl.col('AGE_GROUP').str.replace('_TO_', '-')
                      .alias('AGE_GROUP')))
        coefs.rename(COLUMN_MAP)
        coefs = coefs.melt(id_vars=['RACE', 'AGE_GROUP'],
                           variable_name='VARIABLE',
                           value_name='COEFF')

        # set up the signifcance terms, which vary somewhat from race to race
        query = 'SELECT * FROM significance_Census_1990'
        sigs = pl.read_database_uri(query=query, uri=uri)
        sigs = (sigs.with_columns(pl.col('AGE_GROUP').str.replace('_TO_', '-')
                   .alias('AGE_GROUP')))
        sigs = sigs.drop(['CONVERGED'])
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

            age_group_label = age_group
            if age_group == '0-4':
                age_group_label = '5-9'

            if age_group == '85+':
                age_group_label = '85-115'

            coefs = self.coefs.filter((pl.col('RACE') == COEF_RACE_MAP[race]) & (pl.col('AGE_GROUP') == age_group_label))
            assert coefs.shape == (18, 5)

            # calculate the zero model first
            z_int = coefs.filter(pl.col('VARIABLE') == 'zero_.Intercept.')['COEFF'][0]
            z_pi = coefs.filter(pl.col('VARIABLE') == 'zero_ln_Pi')['COEFF'][0]
            z_pj = coefs.filter(pl.col('VARIABLE') == 'zero_ln_Pj')['COEFF'][0]
            z_cij = coefs.filter(pl.col('VARIABLE') == 'zero_ln_Cij')['COEFF'][0]
            z_tij = coefs.filter(pl.col('VARIABLE') == 'zero_ln_Tij')['COEFF'][0]
            z_pj_star = coefs.filter(pl.col('VARIABLE') == 'zero_ln_Pj_star')['COEFF'][0]

            z_labor = coefs.filter(pl.col('VARIABLE') == 'zero_factor.SAME_LABOR_MARKET.1')['COEFF'][0]
            z_micro = coefs.filter(pl.col('VARIABLE') == 'zero_factor.MICRODEST20.1')['COEFF'][0]
            z_metro = coefs.filter(pl.col('VARIABLE') == 'zero_factor.METRODEST20.1')['COEFF'][0]

            df = df.with_columns(1 - np.exp(-np.exp(z_int +
                                                   (z_pi * np.log(pl.col('Pi') + 1)) +
                                                   (z_pj * np.log(pl.col('Pj') + 1)) +
                                                   (z_cij * np.log(pl.col('Cij') + pl.col('Pj') + 1)) +
                                                   (z_tij * np.log(pl.col('Tij') + 1)) +
                                                   (z_pj_star * np.log(pl.col('Pj_star') + 1)) +
                                                   (z_labor * pl.col('SAME_LABOR_MARKET')) +
                                                   (z_micro * pl.col('MICRO_DESTINATION20')) +
                                                   (z_metro * pl.col('METRO_DESTINATION20')))))
            df = df.rename({'literal': 'ZERO_RESULT'})

            # calculate the count model
            c_int = coefs.filter(pl.col('VARIABLE') == 'count_.Intercept.')['COEFF'][0]
            c_pi = coefs.filter(pl.col('VARIABLE') == 'count_ln_Pi')['COEFF'][0]
            c_pj = coefs.filter(pl.col('VARIABLE') == 'count_ln_Pj')['COEFF'][0]
            c_cij = coefs.filter(pl.col('VARIABLE') == 'count_ln_Cij')['COEFF'][0]
            c_tij = coefs.filter(pl.col('VARIABLE') == 'count_ln_Tij')['COEFF'][0]
            c_pj_star = coefs.filter(pl.col('VARIABLE') == 'count_ln_Pj_star')['COEFF'][0]
            c_labor = coefs.filter(pl.col('VARIABLE') == 'count_factor.SAME_LABOR_MARKET.1')['COEFF'][0]
            c_micro = coefs.filter(pl.col('VARIABLE') == 'count_factor.MICRODEST20.1')['COEFF'][0]
            c_metro = coefs.filter(pl.col('VARIABLE') == 'count_factor.METRODEST20.1')['COEFF'][0]

            df = df.with_columns(np.exp(c_int +
                                       (c_pi * np.log(pl.col('Pi') + 1)) +
                                       (c_pj * np.log(pl.col('Pj') + 1)) +
                                       (c_cij * np.log(pl.col('Cij') + pl.col('Pj') + 1)) +
                                       (c_tij * np.log(pl.col('Tij') + 1)) +
                                       (c_pj_star * np.log(pl.col('Pj_star') + 1)) +
                                       (c_labor * pl.col('SAME_LABOR_MARKET')) +
                                       (c_micro * pl.col('MICRO_DESTINATION20')) +
                                       (c_metro * pl.col('METRO_DESTINATION20'))))
            df = df.rename({'literal': 'COUNT_RESULT'})  #TODO: polars bug?

            # this rounding step preserves >99% of the total migration just calculated
            # df = df.with_columns(((1 - pl.col('ZERO_RESULT')) * pl.col('COUNT_RESULT')).round(0).cast(pl.Int32).alias('MIGRATION'))
            df = df.with_columns(((1 - pl.col('ZERO_RESULT')) * pl.col('COUNT_RESULT')).alias('MIGRATION'))
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
        # Destination counties are identified as rural, micropolitan, or
        # metropolitan using values 1, 2, and 3, respectively.
        uri = f'sqlite:{MIGRATION_DB}'
        query = 'SELECT COFIPS, URBANDESTINATION20 \
                 FROM fips_to_urb20_bea10_hhs'
        df = pl.read_database_uri(query=query, uri=uri)

        df = df.with_columns(pl.lit(0).alias('MICRO_DESTINATION20'))
        df = df.with_columns(pl.lit(0).alias('METRO_DESTINATION20'))

        df = df.with_columns(pl.when(pl.col('URBANDESTINATION20') == 2)
                               .then(pl.lit(1))
                               .alias('MICRO_DESTINATION20'))
        df = df.with_columns(pl.col("MICRO_DESTINATION20").fill_null(strategy="zero"))

        df = df.with_columns(pl.when(pl.col('URBANDESTINATION20') == 3)
                               .then(pl.lit(1))
                               .alias('METRO_DESTINATION20'))
        df = df.with_columns(pl.col("METRO_DESTINATION20").fill_null(strategy="zero"))
        df = df.drop('URBANDESTINATION20')

        return df

    def get_intra_labor_market_moves(self):
        uri = f'sqlite:{MIGRATION_DB}'
        query = 'SELECT COFIPS, BEA10 \
                FROM fips_to_urb20_bea10_hhs'
        df = pl.read_database_uri(query=query, uri=uri)

        return df
