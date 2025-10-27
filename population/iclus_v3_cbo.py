"""
Author:  Phil Morefield
Purpose: Create county-level population projections using the 2025 vintage
         Congressional Budget Office (CBO) projections
Created: October 20th, 2025
"""
import os
import time

from datetime import datetime

import numpy as np
import polars as pl


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
CBO_DB = os.path.join(INPUT_FOLDER, 'databases', 'cbo.sqlite')
CENSUS_DB = os.path.join(INPUT_FOLDER, 'databases', 'census.sqlite')
ACS_DB = os.path.join(INPUT_FOLDER, 'databases', 'acs.sqlite')

SEXES = ('MALE', 'FEMALE')
AGE_GROUPS = ('0-4', '5-9', '10-14', '15-19', '20-24', '25-29', '30-34',
              '35-39', '40-44', '45-49', '50-54', '55-59', '60-64', '65-69',
              '70-74', '75-79', '80-84', '85+')


def make_fips_changes(df):
    csv_name = 'fips_or_name_changes.csv'
    df_fips = pl.read_csv(source=os.path.join(INPUT_FOLDER, csv_name))
    df_fips = df_fips.with_columns(pl.col('OLD_FIPS').cast(pl.Utf8).str.zfill(5))
    df_fips = df_fips.with_columns(pl.col('NEW_FIPS').cast(pl.Utf8).str.zfill(5))

    if {'GEOID', 'AGE_GROUP', 'SEX'}.issubset(df.columns):
        df = df.join(other=df_fips,
                     how='left',
                     left_on='GEOID',
                     right_on='OLD_FIPS')
        df = df.with_columns(pl.when(pl.col('NEW_FIPS').is_not_null())
                             .then(pl.col('NEW_FIPS'))
                             .otherwise(pl.col('GEOID'))
                             .alias('GEOID'))

        df = df.drop(['NEW_FIPS', 'NEW_NAME', 'NEW_STUSPS'])
        df = df.group_by(['GEOID', 'AGE_GROUP', 'SEX']).agg(pl.col('POPULATION').sum())
    else:
        Exception("DataFrame doesn't have required columns for FIPS changes")

    assert df.null_count().sum_horizontal()[0] == 0, "NaN values present after FIPS changes"

    return df

def set_launch_population():
    '''
    2024 launch population is taken from Census Intercensal Population
    Estimates.
    '''
    census_input_folder = os.path.join(INPUT_FOLDER, 'raw_files', 'census', '2024')
    csv_name = 'cc-est2024-agesex-all.csv'
    df = pl.read_csv(source=os.path.join(census_input_folder, csv_name),
                     encoding='latin1').filter(pl.col('YEAR') == 6)
    df = df.with_columns((pl.col('STATE').cast(pl.Utf8).str.zfill(2) +
                          pl.col('COUNTY').cast(pl.Utf8).str.zfill(3))
                          .alias('GEOID'))

    df = df[['GEOID', 'AGE04_FEM', 'AGE04_MALE', 'AGE59_FEM', 'AGE59_MALE',
             'AGE1014_FEM', 'AGE1014_MALE', 'AGE1519_FEM', 'AGE1519_MALE',
             'AGE2024_FEM', 'AGE2024_MALE', 'AGE2529_FEM', 'AGE2529_MALE',
             'AGE3034_FEM', 'AGE3034_MALE', 'AGE3539_FEM', 'AGE3539_MALE',
             'AGE4044_FEM', 'AGE4044_MALE', 'AGE4549_FEM', 'AGE4549_MALE',
             'AGE5054_FEM', 'AGE5054_MALE', 'AGE5559_FEM', 'AGE5559_MALE',
             'AGE6064_FEM', 'AGE6064_MALE', 'AGE6569_FEM', 'AGE6569_MALE',
             'AGE7074_FEM', 'AGE7074_MALE', 'AGE7579_FEM', 'AGE7579_MALE',
             'AGE8084_FEM', 'AGE8084_MALE', 'AGE85PLUS_FEM', 'AGE85PLUS_MALE']]

    df.columns = ['GEOID', '0-4_FEMALE', '0-4_MALE', '5-9_FEMALE', '5-9_MALE',
                  '10-14_FEMALE', '10-14_MALE', '15-19_FEMALE', '15-19_MALE',
                  '20-24_FEMALE', '20-24_MALE', '25-29_FEMALE', '25-29_MALE',
                  '30-34_FEMALE', '30-34_MALE', '35-39_FEMALE', '35-39_MALE',
                  '40-44_FEMALE', '40-44_MALE', '45-49_FEMALE', '45-49_MALE',
                  '50-54_FEMALE', '50-54_MALE', '55-59_FEMALE', '55-59_MALE',
                  '60-64_FEMALE', '60-64_MALE', '65-69_FEMALE', '65-69_MALE',
                  '70-74_FEMALE', '70-74_MALE', '75-79_FEMALE', '75-79_MALE',
                  '80-84_FEMALE', '80-84_MALE', '85+_FEMALE', '85+_MALE']

    df = df.unpivot(index='GEOID',
                    variable_name='AGE_GROUP',
                    value_name='POPULATION')

    df = df.with_columns(pl.col('AGE_GROUP').str.split('_').list.get(1).alias('SEX'))
    df = df.with_columns(pl.col('AGE_GROUP').str.split('_').list.get(0).alias('AGE_GROUP'))
    df = df.with_columns(pl.col('AGE_GROUP').cast(pl.Enum(AGE_GROUPS)))
    df = df.sort(['GEOID', 'AGE_GROUP', 'SEX'])
    df = make_fips_changes(df)
    assert df.shape == (112608, 4)

    return df

def main(scenario, cdc_fert_adj, cdc_mort_adj, census_imm_hist2324):
    '''
    TODO: Add docstring
    '''
    model = Projector(scenario=scenario,
                      cdc_fert_adj=cdc_fert_adj,
                      cdc_mort_adj=cdc_mort_adj,
                      census_imm_hist2324=census_imm_hist2324)
    model.run()


class Projector():
    '''
    TODO: Add docstring
    '''
    def __init__(self, scenario, cdc_fert_adj, cdc_mort_adj, census_imm_hist2324):

        # time-related attributes
        self.launch_year = 2024
        self.current_projection_year = self.launch_year + 1

        # scenario-related attributes
        self.scenario = scenario

        # population-related attributes
        self.current_pop = None
        self.population_time_series = None

        # immigration-related attributes
        self.immigrants = None
        self.census_imm_hist2324 = census_imm_hist2324

        # mortality-related attributes
        self.deaths = None
        self.cdc_mort_adj = cdc_mort_adj

        # migration-related attributes
        self.net_migration = None

        # fertility-related attributes
        self.births = None
        self.cdc_fert_adj = cdc_fert_adj

    def run(self, final_projection_year=2099):
        '''
        TODO:
        '''
        self.current_pop = set_launch_population()

        print("\n")
        print("***************** PARAMETERS ******************")
        print("Scenario: ", self.scenario)
        print("CDC fertility adjustment:", f'{self.cdc_fert_adj * 100}%')
        print("CDC mortality adjustment:", f'{self.cdc_mort_adj * 100}%')
        print("Census immigration historical 2023-2024:", self.census_imm_hist2324)
        print("***********************************************")

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
                                                      on=['GEOID', 'AGE_GROUP', 'SEX'],
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
                                                      on=['GEOID', 'AGE_GROUP', 'SEX'],
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
                                                      on=['GEOID', 'AGE_GROUP', 'SEX'],
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
                                                     on=['GEOID', 'AGE_GROUP', 'SEX'],
                                                     how='left',
                                                     coalesce=True)
                                .with_columns(pl.when(pl.col('BIRTHS').is_not_null())
                                              .then(pl.col('POPULATION') + pl.col('BIRTHS'))
                                              .otherwise(pl.col('POPULATION'))
                                .alias('POPULATION'))
                                .drop('BIRTHS'))

            assert self.current_pop.shape == (675648, 5)
            self.births = None

            self.current_pop = self.current_pop.sort(['GEOID', 'SEX', 'AGE_GROUP'])
            self.current_pop = self.current_pop.with_columns(pl.col('POPULATION').round().alias('POPULATION').cast(pl.UInt64))

            if self.population_time_series is None:
                self.population_time_series = self.current_pop.clone()
            else:
                self.population_time_series = pl.concat(items=[self.population_time_series, self.current_pop], how='align')
            self.population_time_series = self.population_time_series.rename({'POPULATION': str(self.current_projection_year)})
            self.current_projection_year += 1

            print(f"Total population (end): {self.current_pop.select('POPULATION').sum().item():,}\n")

            print("\n")
            print("***************** PARAMETERS ******************")
            print("Scenario: ", self.scenario)
            print("CDC fertility adjustment:", f'{self.cdc_fert_adj * 100}%')
            print("CDC mortality adjustment:", f'{self.cdc_mort_adj * 100}%')
            print("Census immigration historical 2023-2024:", self.census_imm_hist2324)
            print("Output database:", os.path.basename(OUTPUT_DATABASE))
            print("***********************************************")

            # save results to sqlite3 database
            uri = f'sqlite:{OUTPUT_DATABASE}'
            temp = self.population_time_series.clone()
            temp = temp.sort(by=['GEOID', 'SEX', 'AGE_GROUP'])
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
        self.current_pop = self.current_pop.sort(['GEOID', 'SEX', 'AGE_GROUP'])

        # shift 20 percent of the population in each cohort

        self.current_pop = self.current_pop.with_columns((pl.col('POPULATION') * 0.2)
                                           .shift(fill_value=0)
                                           .over('GEOID', 'SEX')
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

        print("finished!")

    def mortality(self):
        '''
        Placeholder
        '''

        print("Calculating mortality...", end='')

        # get CDC mortality rates by AGE_GROUP, SEX, and COUNTY
        uri = f'sqlite:{CDC_DB}'
        query = 'SELECT AGE_GROUP, SEX, COFIPS AS GEOID, MORTALITY AS MORTALITY_RATE_100K \
                 FROM mortality_2019_2023_county'
        county_mort_rates = pl.read_database_uri(query=query, uri=uri).with_columns(pl.col('AGE_GROUP').cast(pl.Enum(AGE_GROUPS)))

        df = self.current_pop.clone()
        df = df.join(other=county_mort_rates,
                     on=['AGE_GROUP', 'SEX', 'GEOID'],
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

        df = df.with_columns(((pl.col('MORTALITY_RATE_100K') * (1 + self.cdc_mort_adj) * pl.col('MORT_MULTIPLY')) / 100000.0).alias('MORT_PROJ'))

        # calculate deaths
        df = df.with_columns((pl.col('MORT_PROJ') * pl.col('POPULATION')).alias('DEATHS'))
        df = df.select(['GEOID', 'AGE_GROUP', 'SEX', 'DEATHS'])
        assert sum(df.null_count()).item() == 0

        # store deaths
        self.deaths = df.clone()
        total_deaths_this_year = round(self.deaths.select(pl.col('DEATHS').sum()).item())

        # store time series of mortality in sqlite3
        uri = f'sqlite:{OUTPUT_DATABASE}'
        if self.current_projection_year == self.launch_year + 1:
            deaths = self.deaths.rename({'DEATHS': str(self.current_projection_year)})
        else:
            query = f'SELECT * FROM deaths_by_age_and_sex_{self.scenario}'
            deaths = pl.read_database_uri(query=query, uri=uri).with_columns(pl.col('AGE_GROUP').cast(pl.Enum(AGE_GROUPS)))
            current_deaths = self.deaths.clone()
            current_deaths = current_deaths.rename({'DEATHS': str(self.current_projection_year)})
            deaths = pl.concat(items=[deaths, current_deaths], how='align')
        deaths.sort(by=['GEOID', 'SEX', 'AGE_GROUP'])
        # assert deaths.shape[0] == 675648
        assert sum(deaths.null_count()).item() == 0

        deaths.write_database(table_name=f'deaths_by_age_and_sex_{self.scenario}',
                              connection=uri,
                              if_table_exists='replace',
                              engine='adbc')

        print(f"finished! ({total_deaths_this_year:,} deaths this year)")

    def immigration(self):
        '''
        Calculate net immigration
        '''
        print("Calculating net immigration...", end='')
        # get the County level age-sex proportions
        uri = f'sqlite:{ACS_DB}'
        query = 'SELECT *  FROM acs_immigration_age_sex_fractions_2011_2015'
        county_weights = pl.read_database_uri(query=query, uri=uri)

        # this is the net migrants for each age-sex combination
        uri = f'sqlite:{CBO_DB}'
        query = f'SELECT AGE_GROUP, SEX, NET_IMMIGRATION  \
                  FROM cbo_2025_9_migration \
                  WHERE YEAR = "{self.current_projection_year}"'
        df_cbo = pl.read_database_uri(query=query, uri=uri)

        df = (county_weights.join(other=df_cbo,
                                  on=['AGE_GROUP', 'SEX'],
                                  how='left',
                                  coalesce=True)
                            .with_columns((pl.col('NET_IMMIGRATION') * pl.col('PERCENT_OF_AGE_SEX_COHORT'))
                            .alias('NET_IMMIGRATION'))
                            .drop('PERCENT_OF_AGE_SEX_COHORT'))

        assert sum(df.null_count()).item() == 0
        df = df.with_columns(pl.col('AGE_GROUP').cast(pl.Enum(AGE_GROUPS)))

        self.immigrants = df.clone()

        # store time series of immigration in sqlite3
        uri = f'sqlite:{OUTPUT_DATABASE}'
        if self.current_projection_year == self.launch_year + 1:
            immigration = self.immigrants.rename({'NET_IMMIGRATION': str(self.current_projection_year)}).clone()
        else:
            query = f'SELECT * FROM immigration_by_age_sex_{self.scenario}'
            immigration = pl.read_database_uri(query=query, uri=uri).with_columns(pl.col('AGE_GROUP').cast(pl.Enum(AGE_GROUPS)))
            current_immigration = self.immigrants.clone()
            current_immigration = current_immigration.rename({'NET_IMMIGRATION': str(self.current_projection_year)}).clone()
            immigration = pl.concat(items=[immigration, current_immigration], how='align')

        assert sum(immigration.null_count()).item() == 0

        immigration.write_database(table_name=f'immigration_by_age_sex_{self.scenario}',
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

        # get the age-sex migration rates specific to each ORIGIN-DESTINATION
        uri = f'sqlite:{ACS_DB}'
        query = 'SELECT *  FROM acs_gross_migration_age_sex_fractions_2011_2015'
        rates = pl.read_database_uri(query=query, uri=uri).with_columns(pl.col('AGE_GROUP').cast(pl.Enum(AGE_GROUPS)))

        # we have migration rates to/from Puerto Rico, but not currently
        # modeling migration involving PR
        rates = rates.filter(~pl.col('ORIGIN_FIPS').str.starts_with('7'))
        rates = rates.filter(~pl.col('DESTINATION_FIPS').str.starts_with('7'))

        # compute all county to county migration flows
        # join current population with migration rates ORIGIN_FIPS
        migr = rates.join(other=self.current_pop.clone(),
                                         left_on=['ORIGIN_FIPS', 'AGE_GROUP', 'SEX'],
                                         right_on=['GEOID', 'AGE_GROUP', 'SEX'],
                                         how='left',
                                         coalesce=True).rename({'POPULATION': 'ORIGIN_POPULATION'})

        assert sum(migr.null_count()).item() == 0

        # calculate net migration flows
        migr = migr.with_columns((pl.col('MIGRATION_RATE') * pl.col('ORIGIN_POPULATION')).alias('FLOW'))
        inflows = (migr.with_columns(pl.col('FLOW').sum().over(['DESTINATION_FIPS', 'AGE_GROUP', 'SEX'])
                                           .alias('INFLOWS'))
                                           .select(['DESTINATION_FIPS', 'AGE_GROUP', 'SEX', 'INFLOWS'])
                                           .unique()
                                           .rename({'DESTINATION_FIPS': 'GEOID'}))
        outflows = (migr.with_columns(pl.col('FLOW').sum().over(['ORIGIN_FIPS', 'AGE_GROUP', 'SEX'])
                                            .alias('OUTFLOWS'))
                                            .select(['ORIGIN_FIPS', 'AGE_GROUP', 'SEX', 'OUTFLOWS'])
                                            .unique()
                                            .rename({'ORIGIN_FIPS': 'GEOID'}))

        net_migr = inflows.join(other=outflows,
                                on=['GEOID', 'AGE_GROUP', 'SEX'],
                                how='full',
                                coalesce=True).fill_null(0)
        assert round(inflows.select(pl.col.INFLOWS).sum().item()) == round(outflows.select(pl.col.OUTFLOWS).sum().item())
        total_migrants_this_year = round(net_migr.select(pl.col('INFLOWS').sum()).item())

        self.net_migration = net_migr.with_columns((pl.col('INFLOWS') - pl.col('OUTFLOWS')).alias('NET_MIGRATION'))

        assert self.net_migration.shape[0] == 101309
        assert self.net_migration.null_count().sum_horizontal().item() == 0
        assert self.net_migration.filter(pl.col('NET_MIGRATION').is_nan()).shape[0] == 0

        # store time series of migration in sqlite3
        uri = f'sqlite:{OUTPUT_DATABASE}'
        if self.current_projection_year == self.launch_year + 1:
            migration = self.net_migration.rename({'NET_MIGRATION': f'NETMIG{self.current_projection_year}',
                                                    'INFLOWS': f'INMIG{self.current_projection_year}',
                                                    'OUTFLOWS': f'OUTMIG{self.current_projection_year}'}).clone()
        else:
            query = f'SELECT * FROM migration_by_age_sex_{self.scenario}'
            migration = pl.read_database_uri(query=query, uri=uri).with_columns(pl.col('AGE_GROUP').cast(pl.Enum(AGE_GROUPS)))
            current_migration = self.net_migration.clone().rename({'NET_MIGRATION': f'NETMIG{self.current_projection_year}',
                                                                   'INFLOWS': f'INMIG{self.current_projection_year}',
                                                                   'OUTFLOWS': f'OUTMIG{self.current_projection_year}'})
            migration = migration.join(current_migration,
                                       on=['GEOID', 'AGE_GROUP', 'SEX'],
                                       how='left',
                                       coalesce=True).sort(by=['GEOID', 'AGE_GROUP', 'SEX'])

        migration.write_database(table_name=f'migration_by_age_sex_{self.scenario}',
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

        # get CDC fertility rates by AGE_GROUP (15-44) and COUNTY
        uri = f'sqlite:{CDC_DB}'
        query = 'SELECT COFIPS AS GEOID, AGE_GROUP, FERTILITY \
                 FROM fertility_2018_2022_county'
        county_fert_rates = pl.read_database_uri(query=query, uri=uri)
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
                     on=['GEOID', 'AGE_GROUP'],
                     how='left',
                     coalesce=True)

        df = df.join(other=fert_multiply,
                     on='AGE_GROUP',
                     how='left',
                     coalesce=True)

        df = df.with_columns(((pl.col('FERTILITY') * (1 + self.cdc_fert_adj) * pl.col('FERT_MULT') / 1000) * pl.col('POPULATION')).alias('TOTAL_BIRTHS'))
        df = df.with_columns((pl.col('TOTAL_BIRTHS') * 0.512195122).alias('MALE'))  # from Mathews, et al. (2005)
        df = df.with_columns((pl.col('TOTAL_BIRTHS') - pl.col('MALE')).alias('FEMALE'))
        df = (df.select(['GEOID', 'MALE', 'FEMALE'])
                .unpivot(index='GEOID', variable_name='SEX', value_name='BIRTHS')
                .group_by(['GEOID', 'SEX']).agg(pl.col('BIRTHS').sum()))
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
        births.sort(by=['GEOID', 'SEX', 'AGE_GROUP'])
        assert births.shape[0] == 37536
        assert sum(births.null_count()).item() == 0
        births.write_database(table_name=f'births_by_race_sex_age_{self.scenario}',
                      connection=uri,
                      if_table_exists='replace',
                      engine='adbc')

        print(f"finished! ({total_births_this_year:,} births this year)")


if __name__ == '__main__':
    print(time.ctime())
    main(scenario='CBO', # immigration scenario from Census 2023
         cdc_fert_adj=-0.055, # example: -0.045 for a 4.5% reduction
         cdc_mort_adj=-0.15, # example: -0.15 for a 15% reduction
         census_imm_hist2324=False) # boolean; use historical values in place
                                    # of projected Census immigration for years
                                    # 2023-2024. Historical values are always
                                    # used for 2021 and 2022.
    print(time.ctime())
