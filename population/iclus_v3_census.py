"""
Author:  Phil Morefield
Purpose: Create county-level population projections using the 2023 vintage
         Census
Created: April 26th, 2025


20250504 - ICLUSv3 births using the Census main series are roughly 4.5% higher
           than the Census national totals for 2023. I attribute this to
           innacurate or missing fertility rates from the CDC. I'm applying a
           4.5% reduction to the CDC fertility rates so that ICLUSv3 national
           totals more closely match the Census.

           Also reducing CDC mortality rates by 15% to match Census projections
           for 2023.
"""
import os
import time

from datetime import datetime

import numpy as np
import polars as pl

from iclus_v3_migration import migration_plum_v3 as MigrationModel


BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'
if os.path.isdir('D:\\projects\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\projects\\ICLUS_v3\\population'

d = datetime.now()
TIME_STAMP = f'{d.year}{d.month}{d.day}{d.hour}{d.minute}{d.second}'

INPUT_FOLDER = os.path.join(BASE_FOLDER, 'inputs')
OUTPUT_FOLDER = os.path.join(BASE_FOLDER, 'outputs')
OUTPUT_DATABASE = os.path.join(OUTPUT_FOLDER, f'iclus_v3_census_{TIME_STAMP}.sqlite')
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


def set_launch_population():
    '''
    2020 launch population is taken from Census 2020-2023 Intercensal Population
    Estimates.
    '''
    uri = f'sqlite:{POP_DB}'
    query = 'SELECT * FROM county_population_ageracesex_2020'
    df = pl.read_database_uri(query=query, uri=uri)

    df = df.with_columns(pl.col('AGE_GROUP').cast(pl.Enum(AGE_GROUPS)))
    df = df.sort(['GEOID', 'RACE', 'AGE_GROUP', 'SEX'])

    #assert df.shape[0] == 675648
    return df

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

    def run(self, final_projection_year=2099):
        '''
        TODO:
        '''
        self.current_pop = set_launch_population()

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

            # assert self.current_pop.shape == (675648, 5)
            # self.current_pop = self.current_pop.with_columns(pl.col('POPULATION').clip(lower_bound=0))
            assert sum(self.current_pop.null_count()).item() == 0
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

            # assert self.current_pop.shape == (675648, 5)
            self.current_pop = self.current_pop.with_columns(pl.col('POPULATION').clip(lower_bound=0))
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
                                .alias('POPULATION')
                                .round(0)
                                .cast(pl.UInt64)))
            self.current_pop = self.current_pop.drop('NET_MIGRATION')

            # assert self.current_pop.shape == (675648, 5)
            self.current_pop = self.current_pop.with_columns(pl.col('POPULATION').clip(lower_bound=0))
            assert sum(self.current_pop.null_count()).item() == 0
            assert self.current_pop.filter(pl.col('POPULATION') < 0).shape[0] == 0
            self.net_migration = None

            ############
            ## BIRTHS ##
            ############

            # calculate births
            self.fertility()  # create self.births

            # age everyone by one year
            self.advance_age_groups()
            assert self.current_pop.shape == (675648, 5)

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

            assert self.current_pop.shape == (675648, 5)
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

        # get Census mortality rate adjustments
        uri = f'sqlite:{CENSUS_DB}'
        query = f'SELECT AGE_GROUP, SEX, MORT_MULTIPLIER AS MORT_MULTIPLY \
                  FROM census_np2023_asmr \
                  WHERE YEAR = "{self.current_projection_year - 1}"'
        mort_multiply = pl.read_database_uri(query=query, uri=uri).with_columns(pl.col('AGE_GROUP').cast(pl.Enum(AGE_GROUPS)))

        df = df.join(other=mort_multiply,
                     on=['AGE_GROUP', 'SEX'],
                     how='left',
                     coalesce=True)

        # 20250504 - reducing CDC mortality rates by 15% to match Census
        # projections for 2023
        df = df.with_columns(((pl.col('MORTALITY_RATE_100K') * 0.85 * pl.col('MORT_MULTIPLY')) / 100000.0).alias('MORT_PROJ'))

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
        print("Calculating net immigration...", end='')
        # get the County level age-race-ethnicity-sex proportions
        uri = f'sqlite:{ACS_DB}'
        query = 'SELECT *  FROM acs_immigration_cohort_fractions_by_age_group_2006_2015'
        county_weights = pl.read_database_uri(query=query, uri=uri)

        # this is the net migrants for each age-sex combination
        uri = f'sqlite:{CENSUS_DB}'
        query = f'SELECT *  \
                  FROM census_np2023_asmig_{self.scenario} \
                  WHERE YEAR = "{self.current_projection_year}"'
        df_census = pl.read_database_uri(query=query, uri=uri).drop('YEAR')
        df_census = df_census.unpivot(index=['AGE_GROUP', 'SEX'], variable_name='RACE', value_name='NET_IMMIGRATION')

        df = (county_weights.join(other=df_census,
                                  on=['RACE', 'AGE_GROUP', 'SEX'],
                                  how='left',
                                  coalesce=True)
                            .with_columns((pl.col('NET_IMMIGRATION') * pl.col('COUNTY_FRACTION'))
                            .alias('NET_IMMIGRATION'))
                            .drop('COUNTY_FRACTION'))

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

        # for race in ('WHITE',):
        for race in RACES:
            print(f"\t{race}...")

            # compute all county to county migration flows
            # 'compute migrants' iterates over all age groups
            gross_flows = migration_model.compute_migrants(race)
            gross_flows = gross_flows.with_columns(pl.lit(race).alias('RACE'))

            # calculate a sex fraction for each county/race/age cohort
            ratios = self.current_pop.filter(pl.col('RACE') == race)
            ratios = ratios.with_columns(pl.col('POPULATION')
                                         .sum()
                                         .over(['GEOID', 'AGE_GROUP'])
                                         .alias('GEOID_AGE_POP'))

            ratios = ratios.with_columns((pl.col('POPULATION') / pl.col('GEOID_AGE_POP'))
                                         .fill_null(value=0)
                                         .alias('SEX_FRACTION'))
            ratios = ratios.drop(['POPULATION', 'GEOID_AGE_POP', 'RACE'])

            inflows = gross_flows.group_by(pl.col(['DESTINATION_FIPS', 'AGE_GROUP'])).agg(pl.col('MIGRATION').sum())
            lf = (ratios.join(other=inflows,
                              how='left',
                              left_on=['GEOID', 'AGE_GROUP'],
                              right_on=['DESTINATION_FIPS', 'AGE_GROUP'],
                              coalesce=True)
                  .fill_null(value=0)
                  .fill_nan(value=0)
                  .with_columns((pl.col('SEX_FRACTION') * pl.col('MIGRATION'))
                  .alias('INFLOWS')))
            lf = lf.select(['GEOID', 'AGE_GROUP', 'SEX', 'SEX_FRACTION', 'INFLOWS'])

            outflows = gross_flows.group_by(pl.col(['ORIGIN_FIPS', 'AGE_GROUP'])).agg(pl.col('MIGRATION').sum())
            lf = (lf.join(other=outflows,
                          how='left',
                          left_on=['GEOID', 'AGE_GROUP'],
                          right_on=['ORIGIN_FIPS', 'AGE_GROUP'],
                          coalesce=True)
                  .fill_null(value=0)
                  .fill_nan(value=0)
                  .with_columns((pl.col('SEX_FRACTION') * pl.col('MIGRATION'))
                  .alias('OUTFLOWS')))
            lf = lf.with_columns((pl.col('INFLOWS') - pl.col('OUTFLOWS'))
                                 .alias('NET_MIGRATION'))
            lf = lf.select(['GEOID', 'AGE_GROUP', 'SEX', 'INFLOWS', 'OUTFLOWS', 'NET_MIGRATION'])
            lf = lf.with_columns(pl.lit(race).alias('RACE'))

            # assert lf.shape == (111960, 5)

            if self.net_migration is None:
                self.net_migration = lf.clone()
            else:
                self.net_migration = pl.concat(items=[self.net_migration, lf], how='vertical')

        total_migrants_this_year = round(self.net_migration.select('INFLOWS').sum().item())
        self.net_migration = self.net_migration.select(['GEOID', 'RACE', 'SEX', 'AGE_GROUP', 'NET_MIGRATION'])
        self.net_migration = self.net_migration.sort(['GEOID', 'RACE', 'SEX', 'AGE_GROUP'])

        assert self.net_migration.shape[0] == 675648
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
        migration = migration.sort(by=['GEOID', 'RACE', 'SEX', 'AGE_GROUP'])
        assert self.net_migration.shape[0] == 675648
        assert sum(migration.null_count()).item() == 0
        assert self.net_migration.filter(pl.col('NET_MIGRATION') == np.nan).shape[0] == 0

        migration.write_database(table_name=f'migration_by_race_sex_age_{self.scenario}',
                                 connection=uri,
                                 if_table_exists='replace',
                                 engine='adbc')

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

        # get Census fertility rate adjustments
        uri = f'sqlite:{CENSUS_DB}'
        query = f'SELECT AGE_GROUP, TFR_MULTIPLIER AS FERT_MULT \
                  FROM census_np2023_asfr \
                  WHERE YEAR = "{self.current_projection_year - 1}"'
        fert_multiply = pl.read_database_uri(query=query, uri=uri).with_columns(pl.col('AGE_GROUP').cast(pl.Enum(AGE_GROUPS)))

        # adjust the county fertility rates using change factors from
        # Census and then calculate births
        df = df.join(other=county_fert_rates,
                     on=['GEOID', 'AGE_GROUP', 'RACE'],
                     how='left',
                     coalesce=True)

        df = df.join(other=fert_multiply,
                     on='AGE_GROUP',
                     how='left',
                     coalesce=True)

        # 20250504 - reducing CDC fertility rates by 5.5% to match Census
        df = df.with_columns(((pl.col('FERTILITY') * 0.945 * pl.col('FERT_MULT') / 1000) * pl.col('POPULATION')).alias('TOTAL_BIRTHS'))
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
        assert births.shape[0] == 37536
        assert sum(births.null_count()).item() == 0
        births.write_database(table_name=f'births_by_race_sex_age_{self.scenario}',
                      connection=uri,
                      if_table_exists='replace',
                      engine='adbc')

        print(f"finished! ({total_births_this_year:,} births this year)")


if __name__ == '__main__':
    print(time.ctime())
    main('mid')  # immigration scenario from Census 2023
    print(time.ctime())
