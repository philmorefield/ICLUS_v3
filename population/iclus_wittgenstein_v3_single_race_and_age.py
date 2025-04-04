"""
Author:  Phil Morefield
Purpose: Create county-level population projections using Wittgenstein v3
         projections
Created: April 1st, 2025
"""
import os
# import sqlite3
import time

from datetime import datetime
from itertools import product

import numpy as np
import polars as pl

from iclus_migration_v3_single_race_and_age import migration_plum_v3 as MigrationModel


if os.path.isdir('D:\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'
elif os.path.isdir('D:\\projects\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\projects\\ICLUS_v3\\population'
else:
    raise Exception

d = datetime.now()
TIME_STAMP = f'{d.year}{d.month}{d.day}{d.hour}{d.minute}{d.second}'

INPUT_FOLDER = os.path.join(BASE_FOLDER, 'inputs')
OUTPUT_FOLDER = os.path.join(BASE_FOLDER, 'outputs')
OUTPUT_DATABASE = os.path.join(OUTPUT_FOLDER, f'wittgenstein_v3_{TIME_STAMP}.sqlite')
POP_DB = os.path.join(INPUT_FOLDER, 'databases', 'population.sqlite')
MIG_DB = os.path.join(INPUT_FOLDER, 'databases', 'migration.sqlite')
CDC_DB = os.path.join(INPUT_FOLDER, 'databases', 'cdc.sqlite')
WITT_DB = os.path.join(INPUT_FOLDER, 'databases', 'wittgenstein.sqlite')
CENSUS_DB = os.path.join(INPUT_FOLDER, 'databases', 'census.sqlite')
ACS_DB = os.path.join(INPUT_FOLDER, 'databases', 'acs.sqlite')

ETHNICITIES = ('HISPANIC', 'NONHISPANIC')
SEXES = ('MALE', 'FEMALE')
RACES = ('WHITE', 'BLACK', 'ASIAN', 'AIAN', 'NHPI', 'TWO_OR_MORE')
AGE_GROUPS = ('0-4', '5-9', '10-14', '15-19', '20-24', '25-29', '30-34',
              '35-39', '40-44', '45-49', '50-54', '55-59', '60-64', '65-69',
              '70-74', '75-79', '80-84', '85+')


def make_fips_changes(df):
    '''
    TODO: Is this function still needed?
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
    df = df.group_by(['GEOID', 'AGE_GROUP', 'RACE', 'SEX']).agg(pl.col('POPULATION').sum())

    return df


def set_launch_population(launch_year):
    '''
    2020 launch population is taken from Census 2020-2023 Intercensal Population
    Estimates.
    '''

    if launch_year != 2020:
        raise Exception("Invalid launch year!")

    uri = f'sqlite:{POP_DB}'
    query = 'SELECT * FROM county_population_ageracesex_2020'
    df = pl.read_database_uri(query=query, uri=uri)

    df = df.with_columns(pl.col('AGE_GROUP').cast(pl.Enum(AGE_GROUPS)))
    df = df.sort(['GEOID', 'RACE', 'AGE_GROUP', 'SEX'])

    #assert df.shape[0] == 675648
    return df


# def retrieve_baseline_migration_estimate():
#     p = os.path.join(INPUT_FOLDER, 'part_4')
#     db = os.path.join(p, 'baseline_migration_2015_2_c_ii_3_a.sqlite')
#     con = sqlite3.connect(db, timeout=60)
#     query = 'SELECT ORIGIN_FIPS, DESTINATION_FIPS, MIGRATION as BASELINE\
#              FROM gross_migration_by_race_2015'
#     df = pl.read_sql(sql=query, con=con, index_col=['ORIGIN_FIPS', 'DESTINATION_FIPS'])
#     con.close()

#     TOTAL_IN = df.group_by(by='DESTINATION_FIPS')['BASELINE'].sum()
#     TOTAL_OUT = df.group_by(by='ORIGIN_FIPS')['BASELINE'].sum()
#     df = pl.DataFrame(data=TOTAL_IN.sub(other=TOTAL_OUT, axis='index'))
#     # df = df.round().astype(int)
#     df.index.rename(name='GEOID', inplace=True)
#     df.columns = ['MIGRATION']

#     return df


# def retrieve_intercensal_migration():
#     p = os.path.join(INPUT_FOLDER, 'part_5')
#     db = os.path.join(p, 'part_5_inputs.sqlite')
#     con = sqlite3.connect(db, timeout=60)
#     query = 'SELECT COFIPS AS GEOID, DOMESTICMIG2015 AS MIGRATION FROM baseline_net_migration_2015'
#     df = pl.read_sql(sql=query, con=con, index_col='GEOID')
#     con.close()

#     return df


def main(scenario):
    '''
    TODO: Add docstring
    '''
    model = Projector(scenario=scenario)
    model.run()


class Projector():
    '''
    TODO: Add docstring
    '''
    def __init__(self, scenario):

        # time-related attributes
        self.launch_year = 2020
        self.current_projection_year = self.launch_year + 1

        # scenario-related attributes
        self.scenario = scenario
        if self.scenario not in ('SSP1', 'SSP2', 'SSP3'):
            raise Exception("Invalid scenario!")

        # population-related attributes
        self.current_pop = None
        self.population_time_series = None

        # immigration-related attributes
        self.immigrants = None

        # mortality-related attributes
        self.deaths = None

        # migration-related attributes
        self.net_migration = None

        # fertility-related attributes
        self.births = None

    def run(self, launch_year=2020, final_projection_year=2025):
        '''
        TODO:
        '''
        self.current_pop = set_launch_population(launch_year=launch_year)

        while self.current_projection_year <= final_projection_year:
            print("##############")
            print("###        ###")
            print(f"###  {self.current_projection_year}  ###")
            print("###        ###")
            print("##############")
            print(f"{time.ctime()}")
            print(f"Total population (start): {self.current_pop.select('POPULATION').sum()[0, 0]:,}\n")

            ############
            ## DEATHS ##
            ############

            self.mortality()  # creates self.death
            self.current_pop = (self.current_pop.join(self.deaths,
                                                      on=['GEOID', 'AGE_GROUP', 'RACE', 'SEX'],
                                                      how='left',
                                                      coalesce=True)
                                .with_columns(pl.col('POPULATION') - pl.col('DEATHS')
                                .alias('POPULATION'))
                                .drop('DEATHS'))
            assert self.current_pop.shape == (675648, 5)
            assert sum(self.current_pop.null_count()).item() == 0
            # self.current_pop.clip(lower=0, inplace=True)
            assert self.current_pop.filter(pl.col('POPULATION') < 0).shape[0] == 0
            self.deaths = None

            #################
            ## IMMIGRATION ##
            #################

            # calculate net international immigration
            self.immigration()  # creates self.immigrants
            self.current_pop = (self.current_pop.join(self.immigrants,
                                                      on=['GEOID', 'AGE_GROUP', 'RACE', 'SEX'],
                                                      how='left',
                                                      coalesce=True)
                                .with_columns(pl.when(pl.col('NET_IMMIGRATION').is_not_null()).then(pl.col('POPULATION') + pl.col('NET_IMMIGRATION'))
                                .otherwise(pl.col('POPULATION'))
                                .alias('POPULATION'))
                                .drop('NET_IMMIGRATION'))
            # correct for any cohorts that have negative population
            self.current_pop = self.current_pop.with_columns(pl.col('POPULATION').clip(lower_bound=0))
            # assert self.current_pop.shape == (675648, 5)
            assert sum(self.current_pop.null_count()).item() == 0
            assert self.current_pop.filter(pl.col('POPULATION') < 0).shape[0] == 0
            self.immigrants = None

            ###############
            ## MIGRATION ##
            ###############

            # calculate domestic migration
            self.migration()  # creates self.net_migration
            self.current_pop = (self.current_pop.join(other=self.net_migration,
                                                      on=['GEOID', 'AGE_GROUP', 'RACE', 'SEX'],
                                                      how='left',
                                                      coalesce=True)
                                .fill_null(0)
                                .with_columns((pl.col('POPULATION') + pl.col('NET_MIGRATION'))
                                .alias('POPULATION')))
            self.current_pop = self.current_pop.drop('NET_MIGRATION')

            # correct for any cohorts that have negative population
            self.current_pop = self.current_pop.with_columns(pl.col('POPULATION').clip(lower_bound=0))

            assert self.current_pop.shape == (671760, 5)
            assert sum(self.current_pop.null_count()).item() == 0
            # assert self.current_pop.filter(pl.col('POPULATION') < 0).shape[0] == 0
            self.net_migration = None

            ############
            ## BIRTHS ##
            ############

            # calculate births
            self.fertility()  # create self.births

            # age everyone by one year
            self.advance_age_groups()
            assert self.current_pop.shape == (671760, 5)

            # add births
            self.current_pop = (self.current_pop.join(other=self.births,
                                                     on=['GEOID', 'RACE', 'AGE_GROUP', 'SEX'],
                                                     how='left',
                                                     coalesce=True)
                                .with_columns(pl.when(pl.col('BIRTHS').is_not_null())
                                              .then(pl.col('POPULATION') + pl.col('BIRTHS'))
                                              .otherwise(pl.col('POPULATION'))
                                .alias('POPULATION'))
                                .drop('BIRTHS'))

            assert self.current_pop.shape == (671760, 5)
            self.births = None

            self.current_pop = self.current_pop.sort(['GEOID', 'RACE', 'SEX', 'AGE_GROUP'])

            if self.population_time_series is None:
                self.population_time_series = self.current_pop.clone()
            else:
                self.population_time_series = pl.concat(items=[self.population_time_series, self.current_pop], how='align')
            self.population_time_series = self.population_time_series.rename({'POPULATION': str(self.current_projection_year)})
            self.current_projection_year += 1

            print(f"Total population (end): {self.current_pop.select('POPULATION').sum().item():,}\n")

            # save results to sqlite3 database
            uri = f'sqlite:{OUTPUT_DATABASE}'
            temp = self.population_time_series.clone()
            temp = temp.sort(by=['GEOID', 'RACE', 'SEX', 'AGE_GROUP'])
            temp.write_database(table_name=f'population_by_race_sex_age_{self.scenario}',
                                connection=uri,
                                if_table_exists='replace',
                                engine='adbc')
            del temp

    def advance_age_groups(self):
        '''
        Since cohorts are aggregated into 5-year age groups, advance 20 percent
        of the population in each cohorts to the next AGE_GROUP
        '''
        print("Advancing the age of the population by one year...", end='')
        starting_pop = self.current_pop.select('POPULATION').sum().item()

        # VERY IMPORTANT that the dataframe is sorted exactly like this
        self.current_pop = self.current_pop.sort(['GEOID', 'RACE', 'SEX', 'AGE_GROUP'])

        # shift 20 percent of the population in each cohort

        self.current_pop = self.current_pop.with_columns((pl.col('POPULATION') * 0.2)
                                           .shift(fill_value=0)
                                           .over('GEOID', 'RACE', 'SEX')
                                           .alias('AGE_ADVANCING'))

        # reduce the population in each age cohort by 20%, except for 85+
        self.current_pop = self.current_pop.with_columns(pl.when(pl.col('AGE_GROUP') != pl.lit('85+'))
                                                         .then(pl.col('POPULATION') * 0.8)
                                                         .otherwise(pl.col('POPULATION'))
                                                         .alias('POPULATION'))

        self.current_pop = self.current_pop.with_columns((pl.col('POPULATION') + pl.col('AGE_ADVANCING')).alias('POPULATION'))
        self.current_pop = self.current_pop.drop('AGE_ADVANCING')

        # a rounding difference of << 1 is possible
        assert starting_pop - self.current_pop.select('POPULATION').sum().item() < 1
        # self.current_pop = self.current_pop.round().astype(int)

        print("finished!")

    def mortality(self):
        '''
        Placeholder
        '''

        print("Calculating mortality...", end='')

        # get CDC mortality rates by AGE_GROUP, RACE, SEX, and COUNTY
        uri = f'sqlite:{CDC_DB}'
        query = 'SELECT RACE, AGE_GROUP, SEX, COFIPS AS GEOID, MORTALITY AS MORTALITY_RATE_100K \
                 FROM mortality_2018_2022_county'
        county_mort_rates = pl.read_database_uri(query=query, uri=uri).with_columns(pl.col('AGE_GROUP').cast(pl.Enum(AGE_GROUPS)))

        df = self.current_pop.clone()
        df = df.join(other=county_mort_rates,
                     on=['RACE', 'AGE_GROUP', 'SEX', 'GEOID'],
                     how='left',
                     coalesce=True)

        # get Wittgenstein mortality rate adjustments
        uri = f'sqlite:{WITT_DB}'
        query = f'SELECT AGE_GROUP, SEX, MORT_CHANGE_MULT AS MORT_MULTIPLY \
                  FROM age_specific_mortality_v3 \
                  WHERE SCENARIO = "{self.scenario}" \
                  AND YEAR = "{self.current_projection_year - 1}"'
        mort_multiply = pl.read_database_uri(query=query, uri=uri).with_columns(pl.col('AGE_GROUP').cast(pl.Enum(AGE_GROUPS)))

        df = df.join(other=mort_multiply,
                     on=['AGE_GROUP', 'SEX'],
                     how='left',
                     coalesce=True)
        # assert df.shape[0] == 675648
        df = df.with_columns(((pl.col('MORTALITY_RATE_100K') * pl.col('MORT_MULTIPLY')) / 100000.0).alias('MORT_PROJ'))

        # calculate deaths
        df = df.with_columns((pl.col('MORT_PROJ') * pl.col('POPULATION')).alias('DEATHS'))
        df = df.select(['GEOID', 'AGE_GROUP', 'RACE', 'SEX', 'DEATHS'])
        assert sum(df.null_count()).item() == 0

        # store deaths
        self.deaths = df.clone()
        total_deaths_this_year = round(self.deaths.select(pl.col('DEATHS').sum()).item())

        # store time series of mortality in sqlite3
        uri = f'sqlite:{OUTPUT_DATABASE}'
        if self.current_projection_year == self.launch_year + 1:
            deaths = self.deaths.rename({'DEATHS': str(self.current_projection_year)})
        else:
            query = f'SELECT * FROM deaths_by_race_sex_age_{self.scenario}'
            deaths = pl.read_database_uri(query=query, uri=uri).with_columns(pl.col('AGE_GROUP').cast(pl.Enum(AGE_GROUPS)))
            current_deaths = self.deaths.clone()
            current_deaths = current_deaths.rename({'DEATHS': str(self.current_projection_year)})
            deaths = pl.concat(items=[deaths, current_deaths], how='align')
        deaths.sort(by=['GEOID', 'RACE', 'SEX', 'AGE_GROUP'])
        # assert deaths.shape[0] == 675648
        assert sum(deaths.null_count()).item() == 0

        deaths.write_database(table_name=f'deaths_by_race_sex_age_{self.scenario}',
                              connection=uri,
                              if_table_exists='replace',
                              engine='adbc')

        print(f"finished! ({total_deaths_this_year:,} deaths this year)")

    def immigration(self):
        '''
        Calculate net immigration
        '''
        IMMIGRATION_RACES = ['AIAN', 'ASIAN', 'BLACK', 'HISP_WHITE', 'NHPI', 'NH_WHITE', 'TWO_OR_MORE', 'NET']

        print("Calculating net immigration...", end='')
        # get the County level age-race-ethnicity-sex proportions
        uri = f'sqlite:{ACS_DB}'
        query = 'SELECT *  FROM acs_immigration_cohort_fractions_by_age_group_2006_2015'
        county_weights = pl.read_database_uri(query=query, uri=uri)

        # this is the net migrants for each age-sex combination
        uri = f'sqlite:{WITT_DB}'
        query = f'SELECT AGE_GROUP, SEX, NETMIG_INTERP AS NET \
                  FROM age_specific_net_migration_v3 \
                  WHERE SCENARIO = "{self.scenario}" \
                  AND YEAR = "{self.current_projection_year}"'
        witt = pl.read_database_uri(query=query, uri=uri)

        # get the projected (2017-2060) annual Census annual age-race-sex
        # proportions; this DataFrame is used to allocate one year of total net
        # immigration across all age-race-sex combinations; after 2060 and
        # before 2017 the rates are held constant (at 2060 and 2017 rates,
        # respectively.
        if self.scenario == 'SSP2':
            ratio = 'high'
        elif self.scenario == 'SSP1':
            ratio = 'mid'
        elif self.scenario == 'SSP3':
            ratio = 'low'
        else:
            raise Exception
        uri = f'sqlite:{CENSUS_DB}'
        query = f'SELECT * \
                  FROM annual_immigration_fraction_{ratio} \
                  WHERE year = "{self.current_projection_year}"'
        df_census = pl.read_database_uri(query=query, uri=uri).drop('YEAR')

        # multiply annual immigration by the agegroup/race/sex proportions
        all_immig_cohorts = df_census.join(other=witt,
                                           on=['SEX', 'AGE_GROUP'],
                                           how='left',
                                           coalesce=True)
        for race in IMMIGRATION_RACES:
            all_immig_cohorts = all_immig_cohorts.with_columns((pl.col(race) * pl.col('NET')).alias(race))

        all_immig_cohorts = all_immig_cohorts.drop('NET')
        all_immig_cohorts = all_immig_cohorts.unpivot(index=['AGE_GROUP', 'SEX'],
                                                      variable_name='RACE',
                                                      value_name='NET_IMMIGRATION')

        df = (county_weights.join(other=all_immig_cohorts,
                                  on=['RACE', 'AGE_GROUP', 'SEX'],
                                  how='left',
                                  coalesce=True)
                            .with_columns((pl.col('NET_IMMIGRATION') * pl.col('COUNTY_FRACTION'))
                            .alias('NET_IMMIGRATION'))
                            .drop('COUNTY_FRACTION'))

        value1 = all_immig_cohorts.select('NET_IMMIGRATION').sum().item()
        value2 = df.select('NET_IMMIGRATION').sum().item()
        assert abs(value1 - value2) < 1
        assert sum(df.null_count()).item() == 0

        # clean things up
        df = df.with_columns(pl.when(pl.col('RACE') == pl.lit('NH_WHITE')).then(pl.lit('WHITE'))
                               .when(pl.col('RACE') == pl.lit('HISP_WHITE')).then(pl.lit('WHITE'))
                               .otherwise(pl.col('RACE')).alias('RACE'))
        df = df.group_by(['GEOID', 'RACE', 'AGE_GROUP', 'SEX']).agg(pl.col('NET_IMMIGRATION').sum())
        df = df.with_columns(pl.col('AGE_GROUP').cast(pl.Enum(AGE_GROUPS)))

        self.immigrants = df.clone()

        # store time series of immigration in sqlite3
        uri = f'sqlite:{OUTPUT_DATABASE}'
        if self.current_projection_year == self.launch_year + 1:
            immigration = self.immigrants.rename({'NET_IMMIGRATION': str(self.current_projection_year)}).clone()
        else:
            query = f'SELECT * FROM immigration_by_race_sex_age_{self.scenario}'
            immigration = pl.read_database_uri(query=query, uri=uri).with_columns(pl.col('AGE_GROUP').cast(pl.Enum(AGE_GROUPS)))
            current_immigration = self.immigrants.clone()
            current_immigration = current_immigration.rename({'NET_IMMIGRATION': str(self.current_projection_year)}).clone()
            immigration = pl.concat(items=[immigration, current_immigration], how='align')

        assert sum(immigration.null_count()).item() == 0

        immigration.write_database(table_name=f'immigration_by_race_sex_age_{self.scenario}',
                                   connection=uri,
                                   if_table_exists='replace',
                                   engine='adbc')

        total_immigrants_this_year = round(immigration.select(f'{self.current_projection_year}').sum().item())
        print(f"finished! ({total_immigrants_this_year:,} net immigrants this year)")

    def migration(self):
        '''
        Calculate domestic migration
        '''
        print("Calculating domestic migration...")

        migration_model = MigrationModel()
        migration_model.current_pop = self.current_pop.clone()

        for race, age_group in product(RACES, AGE_GROUPS):
            print(f"\t{race}...{age_group}")

            # compute all county to county migration flows
            gross_flows = migration_model.compute_migrants(race, age_group)
            gross_flows = gross_flows.with_columns(pl.lit(race).alias('RACE'))

            # calculate a sex fraction for each county/race/age cohort
            ratios = self.current_pop.filter(pl.col('RACE') == race)
            ratios = ratios.with_columns(pl.col('POPULATION')
                                         .sum()
                                         .over(['GEOID', 'AGE_GROUP'])
                                         .alias('GEOID_AGE_POP')).lazy()

            ratios = ratios.with_columns((pl.col('POPULATION') / pl.col('GEOID_AGE_POP'))
                                         .fill_null(value=0)
                                         .alias('SEX_FRACTION'))
            ratios = ratios.drop(['POPULATION', 'GEOID_AGE_POP', 'RACE'])

            lf = (ratios.join(other=gross_flows,
                              how='left',
                              left_on=['GEOID', 'AGE_GROUP'],
                              right_on=['DESTINATION_FIPS', 'AGE_GROUP'],
                              coalesce=True)
                  .fill_null(value=0)
                  .fill_nan(value=0)
                  .with_columns((pl.col('SEX_FRACTION') * pl.col('MIGRATION'))
                  .alias('GROSS_INFLOW')))
            lf = lf.with_columns(pl.col('GROSS_INFLOW')
                                 .sum()
                                 .over(['GEOID', 'AGE_GROUP', 'SEX'])
                                 .alias('SUM_INFLOWS'))
            lf = lf.select(['GEOID', 'AGE_GROUP', 'SEX', 'SEX_FRACTION', 'SUM_INFLOWS']).unique()

            lf = (lf.join(other=gross_flows,
                          how='left',
                          left_on=['GEOID', 'AGE_GROUP'],
                          right_on=['ORIGIN_FIPS', 'AGE_GROUP'],
                          coalesce=True)
                  .fill_null(value=0)
                  .fill_nan(value=0)
                  .with_columns((pl.col('SEX_FRACTION') * pl.col('MIGRATION'))
                  .alias('GROSS_OUTFLOW')))

            lf = lf.with_columns(pl.col('GROSS_OUTFLOW')
                        .sum()
                        .over(['GEOID', 'AGE_GROUP', 'SEX'])
                        .alias('SUM_OUTFLOWS'))
            lf = lf.with_columns((pl.col('SUM_INFLOWS') - pl.col('SUM_OUTFLOWS'))
                                 .alias('NET_MIGRATION'))
            lf = lf.select(['GEOID', 'AGE_GROUP', 'SEX', 'NET_MIGRATION']).unique()
            lf = lf.with_columns(pl.lit(race).alias('RACE'))

            # assert lf.shape == (111960, 5)

            if self.net_migration is None:
                self.net_migration = lf.clone()
            else:
                self.net_migration = pl.concat(items=[self.net_migration, lf], how='vertical_relaxed')

        self.net_migration = self.net_migration.sort(['GEOID', 'RACE', 'SEX', 'AGE_GROUP']).collect()
        # assert self.net_migration.shape[0] == 675648
        assert self.net_migration.null_count().sum_horizontal().item() == 0
        assert self.net_migration.filter(pl.col('NET_MIGRATION').is_nan()).shape[0] == 0

        # store time series of migration in sqlite3
        uri = f'sqlite:{OUTPUT_DATABASE}'
        if self.current_projection_year == self.launch_year + 1:
            migration = self.net_migration.rename({'NET_MIGRATION': str(self.current_projection_year)}).clone()
        else:
            query = f'SELECT * FROM migration_by_race_sex_age_{self.scenario}'
            migration = pl.read_database_uri(query=query, uri=uri).with_columns(pl.col('AGE_GROUP').cast(pl.Enum(AGE_GROUPS)))
            current_migration = self.net_migration.clone().rename({'NET_MIGRATION': str(self.current_projection_year)})
            migration = migration.join(current_migration,
                                       on=['GEOID', 'RACE', 'AGE_GROUP', 'SEX'],
                                       how='left',
                                       coalesce=True)
        # migration = migration.sort(by=['GEOID', 'RACE', 'SEX', 'AGE_GROUP'])
        # assert migration.shape[0] == 671760
        assert sum(migration.null_count()).item() == 0
        assert self.net_migration.filter(pl.col('NET_MIGRATION') == np.nan).shape[0] == 0

        migration.write_database(table_name=f'migration_by_race_sex_age_{self.scenario}',
                                 connection=uri,
                                 if_table_exists='replace',
                                 engine='adbc')

        total_migrants_this_year = round((self.net_migration.filter(pl.col('NET_MIGRATION') > 0)
                                          .select('NET_MIGRATION')
                                          .sum())
                                          .item())
        pct_migration = round(((total_migrants_this_year / self.current_pop.select('POPULATION').sum().item())) * 100.0, 1)
        print(f"...finished! ({total_migrants_this_year:,} total migrants this year; {pct_migration}% of the current population)")

    def fertility(self):
        '''
        Calculate births
        '''
        print("Calculating fertility...", end='')

        fertility_age_groups = ('15-19',
                                '20-24',
                                '25-29',
                                '30-34',
                                '35-39',
                                '40-44')

        # get CDC fertility rates by AGE_GROUP (15-44), RACE, and COUNTY
        uri = f'sqlite:{CDC_DB}'
        query = 'SELECT COFIPS AS GEOID, RACE, AGE_GROUP, FERTILITY \
                 FROM fertility_2018_2022_county'
        county_fert_rates = pl.read_database_uri(query=query, uri=uri)
        county_fert_rates = county_fert_rates.with_columns(pl.when(pl.col('RACE') == 'MULTI')
                                             .then(pl.lit('TWO_OR_MORE'))
                                             .otherwise(pl.col('RACE'))
                                             .alias('RACE'))
        county_fert_rates = county_fert_rates.with_columns(pl.col('AGE_GROUP').cast(pl.Enum(AGE_GROUPS)))

        df = self.current_pop.filter(pl.col('SEX').is_in(('FEMALE',)) & pl.col('AGE_GROUP').is_in(fertility_age_groups))

        # get Wittgenstein fertility rate adjustments
        uri = f'sqlite:{WITT_DB}'
        query = f'SELECT AGE_GROUP, FERT_CHANGE_MULT AS FERT_MULT \
                  FROM age_specific_fertility \
                  WHERE SCENARIO = "{self.scenario}" \
                  AND YEAR = "{self.current_projection_year - 1}"'
        fert_multiply = pl.read_database_uri(query=query, uri=uri).with_columns(pl.col('AGE_GROUP').cast(pl.Enum(AGE_GROUPS)))

        # adjust the county fertility rates using change factors from
        # Wittgenstein and then calculate births
        df = df.join(other=county_fert_rates,
                     on=['GEOID', 'AGE_GROUP', 'RACE'],
                     how='left',
                     coalesce=True)

        df = df.join(other=fert_multiply,
                     on='AGE_GROUP',
                     how='left',
                     coalesce=True)

        df = df.with_columns(((pl.col('FERTILITY') * pl.col('FERT_MULT') / 1000) * pl.col('POPULATION')).alias('TOTAL_BIRTHS'))
        df = df.with_columns((pl.col('TOTAL_BIRTHS') * 0.512195122).alias('MALE'))  # from Mathews, et al. (2005)
        df = df.with_columns((pl.col('TOTAL_BIRTHS') - pl.col('MALE')).alias('FEMALE'))
        df = (df.select(['GEOID', 'RACE', 'MALE', 'FEMALE'])
                .unpivot(index=['GEOID', 'RACE'], variable_name='SEX', value_name='BIRTHS')
                .group_by(['GEOID', 'RACE', 'SEX']).agg(pl.col('BIRTHS').sum()))
        df = df.with_columns(pl.lit('0-4').cast(pl.Enum(AGE_GROUPS)).alias('AGE_GROUP'))
        assert sum(df.null_count()).item() == 0

        # store births
        self.births = df.clone()
        total_births_this_year = round(self.births.select('BIRTHS').sum().item())

        # store time series of fertility in sqlite3
        uri = f'sqlite:{OUTPUT_DATABASE}'
        if self.current_projection_year == self.launch_year + 1:
            births = self.births.rename({'BIRTHS': str(self.current_projection_year)})
        else:
            query = f'SELECT * FROM births_by_race_sex_age_{self.scenario}'
            births = pl.read_database_uri(query=query, uri=uri).with_columns(pl.col('AGE_GROUP').cast(pl.Enum(AGE_GROUPS)))
            current_births = self.births.clone()
            current_births = current_births.rename({'BIRTHS': str(self.current_projection_year)}).clone()
            births = pl.concat(items=[births, current_births], how='align')
        births.sort(by=['GEOID', 'RACE', 'SEX', 'AGE_GROUP'])
        assert births.shape[0] == 37320
        assert sum(births.null_count()).item() == 0
        births.write_database(table_name=f'births_by_race_sex_age_{self.scenario}',
                      connection=uri,
                      if_table_exists='replace',
                      engine='adbc')

        print(f"finished! ({total_births_this_year:,} births this year)")


if __name__ == '__main__':
    print(time.ctime())
    main('SSP3')
    print(time.ctime())
