"""
Author: Phil Morefield (pmorefie@gmu.edu)
Purpose: Create county-level population projections using Wittgenstein
         projections
Created: May 13th, 2021

Note: v3 of this script uses immigration age-race ratios from the 2017 Census
      projections to disaggregate Wittgenstein immigration projections
"""
import os
import sqlite3
import time

from datetime import datetime

import polars as pl

from migration_POLARS import migration_2_c_ii_3_a as MigrationModel


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
OUTPUT_DATABASE = os.path.join(OUTPUT_FOLDER, f'wittgenstein_v2_{TIME_STAMP}.sqlite')
POP_DB = os.path.join(INPUT_FOLDER, 'databases', 'population.sqlite')
MIG_DB = os.path.join(INPUT_FOLDER, 'databases', 'migration.sqlite')
CDC_DB = os.path.join(INPUT_FOLDER, 'databases', 'cdc.sqlite')
WITT_DB = os.path.join(INPUT_FOLDER, 'databases', 'wittgenstein.sqlite')
CENSUS_DB = os.path.join(INPUT_FOLDER, 'databases', 'census.sqlite')
ACS_DB = os.path.join(INPUT_FOLDER, 'databases', 'acs.sqlite')

ETHNICITIES = ('HISPANIC', 'NONHISPANIC')
GENDERS = ('MALE', 'FEMALE')
RACES = ('WHITE', 'BLACK', 'ASIAN', 'AIAN', 'NHPI', 'TWO_OR_MORE')
AGE_GROUPS = ('0-4', '5-9', '10-14', '15-19', '20-24', '25-29', '30-34',
              '35-39', '40-44', '45-49', '50-54', '55-59', '60-64', '65-69',
              '70-74', '75-79', '80-84', '85+')


def make_fips_changes(df=None):
    '''
    TODO: Add docstring
    '''

    uri = f'sqlite:{MIG_DB}'
    query = 'SELECT OLD_FIPS, NEW_FIPS \
             FROM fips_or_name_changes'
    df_fips = pl.read_database_uri(query=query, uri=uri)

    df = df.join(other=df_fips,
                 how='left',
                 left_on='GEOID',
                 right_on='OLD_FIPS')

    df = df.with_columns(pl.when(pl.col('NEW_FIPS').is_not_null())
                         .then(pl.col('NEW_FIPS'))
                         .otherwise(pl.col('GEOID')).alias('GEOID'))
    df = df.drop('NEW_FIPS')
    df = df.group_by(['GEOID', 'AGE_GROUP', 'RACE', 'GENDER']).agg(pl.col('POPULATION').sum())

    return df


def set_launch_population(launch_year):
    '''
    2020 launch population is taken from Census 2020-2023 Intercensal Population
    Estimates.
    '''

    if launch_year != 2020:
        raise Exception

    uri = f'sqlite:{POP_DB}'
    query = 'SELECT * FROM county_population_ageracegender_2020'
    df = pl.read_database_uri(query=query, uri=uri)

    df = make_fips_changes(df=df)
    df = df.sort(['GEOID', 'RACE', 'AGE_GROUP'])

    assert df.shape[0] == 671760
    return df


def retrieve_baseline_migration_estimate():
    p = os.path.join(INPUT_FOLDER, 'part_4')
    db = os.path.join(p, 'baseline_migration_2015_2_c_ii_3_a.sqlite')
    con = sqlite3.connect(db, timeout=60)
    query = 'SELECT ORIGIN_FIPS, DESTINATION_FIPS, MIGRATION as BASELINE\
             FROM gross_migration_by_race_2015'
    df = pl.read_sql(sql=query, con=con, index_col=['ORIGIN_FIPS', 'DESTINATION_FIPS'])
    con.close()

    TOTAL_IN = df.group_by(by='DESTINATION_FIPS')['BASELINE'].sum()
    TOTAL_OUT = df.group_by(by='ORIGIN_FIPS')['BASELINE'].sum()
    df = pl.DataFrame(data=TOTAL_IN.sub(other=TOTAL_OUT, axis='index'))
    # df = df.round().astype(int)
    df.index.rename(name='GEOID', inplace=True)
    df.columns = ['MIGRATION']

    return df


def retrieve_intercensal_migration():
    p = os.path.join(INPUT_FOLDER, 'part_5')
    db = os.path.join(p, 'part_5_inputs.sqlite')
    con = sqlite3.connect(db, timeout=60)
    query = 'SELECT COFIPS AS GEOID, DOMESTICMIG2015 AS MIGRATION FROM baseline_net_migration_2015'
    df = pl.read_sql(sql=query, con=con, index_col='GEOID')
    con.close()

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
        if self.scenario not in ('SSP1', 'SSP2', 'SSP3'):
            raise Exception

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

    def run(self, launch_year=2020, final_projection_year=2050):
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

            # calculate deaths
            self.mortality()  # creates self.death
            self.current_pop = (self.current_pop.join(self.deaths, on=['GEOID', 'AGE_GROUP', 'RACE', 'GENDER'], how='left')
                                .with_columns(pl.col('POPULATION') - pl.col('DEATHS')
                                .alias('POPULATION'))
                                .drop('DEATHS'))
            assert self.current_pop.shape == (671760, 5)
            assert sum(self.current_pop.null_count()).item() == 0
            # self.current_pop.clip(lower=0, inplace=True)
            # assert self.current_pop.select('POPULATION') >= 0).all().all()
            self.deaths = None

            # calculate net international immigration
            self.immigration()  # creates self.immigrants
            self.current_pop = (self.current_pop.join(self.immigrants, on=['GEOID', 'AGE_GROUP', 'RACE', 'GENDER'], how='left')
                                .with_columns(pl.when(pl.col('NET_IMMIGRATION').is_not_null()).then(pl.col('POPULATION') + pl.col('NET_IMMIGRATION'))
                                .otherwise(pl.col('POPULATION'))
                                .alias('POPULATION'))
                                .drop('NET_IMMIGRATION'))

            # correct for any cohorts that have negative population
            # self.current_pop = self.current_pop.clip(lower=0).astype(int)

            assert self.current_pop.shape == (671760, 5)
            assert sum(self.current_pop.null_count()).item() == 0
            # assert (self.current_pop >= 0).all().all()
            self.immigrants = None

            # calculate domestic migration
            self.migration()  # creates self.net_migration
            self.current_pop = self.current_pop.add(other=self.net_migration, axis='index', fill_value=0)
            # correct for any cohorts that have negative population
            self.current_pop = self.current_pop.clip(lower=0).astype(int)

            assert self.current_pop.shape == (671760, 1)
            assert not self.current_pop.isnull().any().any()
            assert (self.current_pop >= 0).all().all()
            self.net_migration = None

            # calculate births
            self.fertility()  # create self.births

            # age everyone by one year
            self.advance_age_groups()
            assert self.current_pop.shape == (671760, 1)

            # add births
            self.current_pop = self.current_pop.add(other=self.births, axis='index', fill_value=0.0).astype(int)
            assert self.current_pop.shape == (671760, 1)
            self.births = None

            self.current_pop.sort_index(level=['GEOID', 'GENDER', 'RACE', 'AGE_GROUP'],
                                        inplace=True,
                                        sort_remaining=True)

            if self.population_time_series is None:
                self.population_time_series = self.current_pop.copy()
                self.population_time_series.columns = [self.current_projection_year]
            else:
                self.population_time_series = pl.concat(objs=[self.population_time_series, self.current_pop], axis=1)
                self.population_time_series.rename(columns={'VALUE': self.current_projection_year}, inplace=True)
            self.current_projection_year += 1

            print(f"Total population (end): {self.current_pop.sum().sum():,}\n")

            # save results to sqlite3 database
            db = OUTPUT_DATABASE
            temp = self.population_time_series.reset_index()
            temp['AGE_GROUP'] = temp['AGE_GROUP'].astype(age_group_dtype)
            temp.sort_index(level=['GEOID', 'RACE', 'GENDER', 'AGE_GROUP'], inplace=True)
            con = sqlite3.connect(database=db, timeout=60)
            temp.to_sql(name=f'population_by_race_gender_age_{self.scenario}',
                        con=con,
                        if_table_exists='replace',
                        index=False)
            con.close()
            del temp

    def advance_age_groups(self):
        '''
        Since cohorts are aggregated into 5-year age groups, advance 20 percent
        of the population in each cohorts to the next AGE_GROUP
        '''
        print("Advancing the age of the population by one year...", end='')
        starting_pop = self.current_pop.sum().values[0]

        self.current_pop.reset_index(level='AGE_GROUP', inplace=True)
        self.current_pop['AGE_GROUP'] = self.current_pop['AGE_GROUP'].astype(age_group_dtype)
        self.current_pop.set_index(keys='AGE_GROUP', append=True, inplace=True)
        self.current_pop.sort_index(level=['GEOID', 'RACE', 'GENDER', 'AGE_GROUP'], inplace=True)

        # shift 20 percent of the population in each cohort
        advancers = pl.DataFrame(self.current_pop
                                 .group_by(by=['GEOID', 'RACE', 'GENDER'])
                                 .VALUE
                                 .transform(lambda x: x.shift() * 0.2))

        # reduce the population in each age cohort by 20%, except for 85+
        self.current_pop.loc[self.current_pop.index.get_level_values('AGE_GROUP') != "85+", 'VALUE'] *= 0.8
        self.current_pop = self.current_pop.add(other=pl.DataFrame(advancers),
                                                axis='index',
                                                fill_value=0.0)

        # a rounding difference of << 1 is possible
        assert starting_pop - self.current_pop.sum().values[0] < 1
        self.current_pop = self.current_pop.round().astype(int)

        self.current_pop.reset_index(level='AGE_GROUP', inplace=True)
        self.current_pop['AGE_GROUP'] = self.current_pop['AGE_GROUP'].astype(object)
        self.current_pop.set_index(keys='AGE_GROUP', append=True, inplace=True)

        print("finished!")

    def mortality(self):
        '''
        Placeholder
        '''

        print("Calculating mortality...", end='')

        # get CDC mortality rates by AGE_GROUP, RACE, GENDER, and COUNTY
        uri = f'sqlite:{CDC_DB}'
        query = 'SELECT RACE, AGE_GROUP, GENDER, COFIPS AS GEOID, MORTALITY AS MORTALITY_RATE_100K \
                 FROM mortality_2018_2022_county'
        county_mort_rates = pl.read_database_uri(query=query, uri=uri)

        df = self.current_pop.clone()
        df = df.join(other=county_mort_rates, on=['RACE', 'AGE_GROUP', 'GENDER', 'GEOID'], how='left')

        # get Wittgenstein mortality rate adjustments
        uri = f'sqlite:{WITT_DB}'
        query = f'SELECT AGE_GROUP, GENDER, MORT_CHANGE_MULT AS MORT_MULTIPLY \
                  FROM age_specific_mortality \
                  WHERE SCENARIO = "{self.scenario}" \
                  AND YEAR = "{self.current_projection_year - 1}"'
        mort_multiply = pl.read_database_uri(query=query, uri=uri)

        df = df.join(other=mort_multiply, on=['AGE_GROUP', 'GENDER'], how='left')
        assert df.shape[0] == 671760
        df = df.with_columns(((pl.col('MORTALITY_RATE_100K') * pl.col('MORT_MULTIPLY')) / 100000.0).alias('MORT_PROJ'))

        # calculate deaths
        df = df.with_columns((pl.col('MORT_PROJ') * pl.col('POPULATION')).alias('DEATHS'))
        df = df.select(['GEOID', 'AGE_GROUP', 'RACE', 'GENDER', 'DEATHS'])
        assert sum(df.null_count()).item() == 0

        # store deaths
        self.deaths = df.clone()
        total_deaths_this_year = self.deaths.select(pl.col('DEATHS').sum()).item()

        # store time series of mortality in sqlite3
        uri = f'sqlite:{OUTPUT_DATABASE}'
        if self.current_projection_year == self.launch_year + 1:
            deaths = self.deaths.rename({'DEATHS': str(self.current_projection_year)})
        else:
            query = f'SELECT * FROM deaths_by_race_gender_age_{self.scenario}'
            deaths = pl.read_database_uri(query=query, uri=uri)
            current_deaths = self.deaths.clone()
            current_deaths = current_deaths.rename({'DEATHS': self.current_projection_year}).clone()
            deaths = pl.concat(items=[deaths, current_deaths], how='align')
        deaths.sort(by=['GEOID', 'RACE', 'GENDER', 'AGE_GROUP'])
        assert deaths.shape[0] == 671760
        assert sum(deaths.null_count()).item() == 0

        deaths.write_database(table_name=f'deaths_by_race_gender_age_{self.scenario}',
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

        # this is the net migrants for each age-gender combination
        uri = f'sqlite:{WITT_DB}'
        query = f'SELECT AGE_GROUP, GENDER, NETMIG_INTERP AS NET \
                  FROM age_specific_net_migration \
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
        all_immig_cohorts = df_census.join(other=witt, on=['GENDER', 'AGE_GROUP'], how='left')
        for race in IMMIGRATION_RACES:
            all_immig_cohorts = all_immig_cohorts.with_columns((pl.col(race) * pl.col('NET')).alias(race))

        all_immig_cohorts = all_immig_cohorts.drop('NET')
        all_immig_cohorts = all_immig_cohorts.melt(id_vars=['AGE_GROUP', 'GENDER'],
                                                   variable_name='RACE',
                                                   value_name='NET_IMMIGRATION')

        df = (county_weights.join(other=all_immig_cohorts,
                                  on=['RACE', 'AGE_GROUP', 'GENDER'],
                                  how='left')
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
        df = df.group_by(['GEOID', 'RACE', 'AGE_GROUP', 'GENDER']).agg(pl.col('NET_IMMIGRATION').sum())

        self.immigrants = df.clone()

        # store time series of immigration in sqlite3
        if self.current_projection_year == self.launch_year + 1:
            immigration = self.immigrants.rename({'NET_IMMIGRATION': str(self.current_projection_year)}).clone()
        else:
            query = f'SELECT * FROM immigration_by_race_gender_age_{self.scenario}'
            immigration = pl.read_database_uri(query=query,
                                               uri=uri)
            current_immigration = self.immigrants.clone()
            current_immigration = current_immigration.rename(columns={'NET_IMMIGRATION': self.current_projection_year}).clone()
            immigration = pl.concat(objs=[immigration, current_immigration])

        assert sum(immigration.null_count()).item() == 0

        uri = f'sqlite:{OUTPUT_DATABASE}'
        immigration.write_database(table_name=f'immigration_by_race_gender_age_{self.scenario}',
                                   connection=uri,
                                   if_table_exists='replace',
                                   engine='adbc')

        total_immigrants_this_year = immigration.select(f'{self.current_projection_year}').sum().item()
        print(f"finished! ({total_immigrants_this_year:,} net immigrants this year)")

    def migration(self):
        '''
        Calculate domestic migration
        '''
        print("Calculating domestic migration...")

        migration_model = MigrationModel()
        migration_model.current_pop = self.current_pop.clone()

        for race in RACES:
            print(f"\t{race}...")

            # compute all county to county migration flows
            gross_migration_flows = migration_model.compute_migrants(race)
            gross_migration_flows = gross_migration_flows.with_columns(pl.lit(race).alias('RACE'))

            # the MIGRATION column is actually a gender ratio, but naming it
            # MIGRATION to match gross_migration_flows makes the multiplication
            # step cleaner
            ratio = self.current_pop.filter(pl.col('RACE') == race)
            ratio = (ratio.with_columns(pl.col('POPULATION')
                                        .sum()
                                        .over(['GEOID', 'AGE_GROUP'])
                                        .alias('GEOID_AGE_POP')))

            ratio = ratio.with_columns((pl.col('POPULATION') / pl.col('GEOID_AGE_POP'))
                                       .alias('MIGRATION'))

            ratio = ratio[['MIGRATION']].copy()
            ratio.index.rename(names='ORIGIN_FIPS', level='GEOID', inplace=True)

            ratio_male = ratio.query('GENDER == "MALE"').reset_index(level=['GENDER', 'RACE'], drop=True)
            gross_male = gross_migration_flows.mul(other=ratio_male, axis='index', fill_value=0)
            migin_male = pl.DataFrame(gross_male.group_by(by=['DESTINATION_FIPS', 'AGE_GROUP'])['MIGRATION'].sum())
            migin_male.index.rename(names='GEOID', level='DESTINATION_FIPS', inplace=True)
            migout_male = pl.DataFrame(gross_male.group_by(by=['ORIGIN_FIPS', 'AGE_GROUP'])['MIGRATION'].sum())
            migout_male.index.rename(names='GEOID', level='ORIGIN_FIPS', inplace=True)
            mignet_male = pl.DataFrame(migin_male.sub(other=migout_male, axis='index', fill_value=0))
            mignet_male['GENDER'] = 'MALE'
            mignet_male.set_index(keys='GENDER', append=True, inplace=True)

            ratio_female = ratio.query('GENDER == "FEMALE"').reset_index(level=['GENDER', 'RACE'], drop=True)
            gross_female = gross_migration_flows.mul(other=ratio_female, axis='index', fill_value=0)
            migin_female = gross_female.group_by(by=['DESTINATION_FIPS', 'AGE_GROUP'])['MIGRATION'].sum()
            migin_female.index.rename(names='GEOID', level='DESTINATION_FIPS', inplace=True)
            migout_female = gross_female.group_by(by=['ORIGIN_FIPS', 'AGE_GROUP'])['MIGRATION'].sum()
            migout_female.index.rename(names='GEOID', level='ORIGIN_FIPS', inplace=True)
            mignet_female = pl.DataFrame(migin_female.sub(other=migout_female, axis='index', fill_value=0))
            mignet_female['GENDER'] = 'FEMALE'
            mignet_female.set_index(keys='GENDER', append=True, inplace=True)

            mignet = pl.concat(objs=[mignet_male, mignet_female])
            mignet['RACE'] = race
            mignet.set_index(keys='RACE', append=True, inplace=True)
            assert mignet.shape == (111960, 1)

            if self.net_migration is None:
                self.net_migration = mignet.copy()
            else:
                self.net_migration = pl.concat(objs=[self.net_migration, mignet])

        assert self.net_migration.shape == (671760, 1)
        # assert self.net_migration_r.shape == (671760, 1)

        # using the % change in net migration by county from my model for the
        # years 2015 and the projected year, calculate new net migration by
        # county using observed net migration for 2015 (i.e., delta method)
        baseline_migration_estimate = retrieve_baseline_migration_estimate()
        net_migration_total = self.net_migration.group_by(by='GEOID').sum()
        county_change_factor = net_migration_total.div(other=baseline_migration_estimate, axis='index')
        baseline_migration = retrieve_intercensal_migration()
        new_net_migration = baseline_migration.mul(other=county_change_factor, axis='index')

        # the numbers get messy when the baseline migration estimate and
        # my model's projected migration have a different sign, e.g., negative
        # net migration in 2015 (the baseline) and positive net migration in
        # 2016 (projected). In those cases - for now - use the average of the
        # projected net migration and the 2015 value
        opposite_signs = net_migration_total.join(other=baseline_migration_estimate, lsuffix='proj', rsuffix='base')
        opposite_signs.query('MIGRATIONproj * MIGRATIONbase < 0', inplace=True)
        avg_for_opp_signs = baseline_migration.join(other=net_migration_total, lsuffix='obs', rsuffix='proj')
        # avg_for_opp_signs = avg_for_opp_signs.loc[avg_for_opp_signs.index.isin(opposite_signs.index)]
        avg_for_opp_signs = avg_for_opp_signs.loc[opposite_signs.index]
        avg_for_opp_signs.eval('MIGRATION = (MIGRATIONobs + MIGRATIONproj) / 2', inplace=True)
        new_net_migration.update(other=avg_for_opp_signs)

        net_change = new_net_migration.sub(other=net_migration_total, axis='index').div(other=216)
        self.net_migration = self.net_migration.add(other=net_change, axis='index')

        self.net_migration = self.net_migration.round().astype(int)

        # store time series of migration in sqlite3
        db = OUTPUT_DATABASE
        con = sqlite3.connect(database=db, timeout=60)
        if self.current_projection_year == self.launch_year + 1:
            migration = self.net_migration.rename(columns={'MIGRATION': self.current_projection_year}).copy()
        else:
            query = f'SELECT * FROM migration_by_race_gender_age_{self.scenario}'
            migration = pl.read_sql(sql=query,
                                    con=con,
                                    index_col=['GEOID', 'AGE_GROUP', 'GENDER', 'RACE'])
            current_migration = self.net_migration.copy()
            current_migration.rename(columns={'MIGRATION': self.current_projection_year}, inplace=True)
            migration = pl.concat(objs=[migration, current_migration], axis=1)
        migration.reset_index(inplace=True)
        migration['AGE_GROUP'] = migration['AGE_GROUP'].astype(age_group_dtype)
        migration.sort_values(by=['GEOID', 'RACE', 'GENDER', 'AGE_GROUP'], inplace=True)
        assert migration.shape[0] == 671760
        assert not migration.isnull().any().any()
        migration.to_sql(name=f'migration_by_race_gender_age_{self.scenario}',
                         con=con,
                         if_table_exists='replace',
                         index=False)
        con.close()

        self.net_migration.rename(columns={'MIGRATION': 'VALUE'}, inplace=True)
        total_migrants_this_year = self.net_migration.query('VALUE > 0').sum().squeeze()
        pct_migration = (((total_migrants_this_year / self.current_pop.sum().values[0]) * 100.0).round(1))
        print(f"finished! ({total_migrants_this_year:,} total migrants this year; {pct_migration}% of the current population)")

    def fertility(self):
        '''
        Calculate births
        '''
        print("Calculating fertility...", end='')

        # get CDC fertility rates by AGE_GROUP (15-44), RACE, and COUNTY
        con = sqlite3.connect(database=CDC_DB, timeout=60)
        query = 'SELECT COFIPS AS GEOID, RACE, AGE_GROUP, FERTILITY AS VALUE \
                 FROM fertility_2013_2017_county_age_groups'
        county_fert_rates = pl.read_sql(sql=query, con=con)
        con.close()

        age_groups = list(county_fert_rates.AGE_GROUP.unique())
        # county_fert_rates = pl.DataFrame(data=county_fert_rates.eval('RACE = RACE.str.replace("MULTI", "TWO_OR_MORE")', engine='python'))
        county_fert_rates['RACE'] = county_fert_rates['RACE'].str.replace('MULTI', 'TWO_OR_MORE')
        county_fert_rates.set_index(keys=['GEOID', 'RACE', 'AGE_GROUP'], inplace=True)

        # df = self.current_pop.query('GENDER == "FEMALE" & AGE_GROUP.isin(@age_groups)', engine='python').copy()
        df = self.current_pop.loc[(self.current_pop.index.get_level_values(level='GENDER') == "FEMALE") & (self.current_pop.index.get_level_values(level='AGE_GROUP').isin(age_groups))].copy()
        df.reset_index(level='GENDER', drop=True, inplace=True)

        # get Wittgenstein fertility rate adjustments
        con = sqlite3.connect(database=WITT_DB, timeout=60)
        query = f'SELECT AGE_GROUP, FERT_CHANGE_MULT AS VALUE \
                  FROM age_specific_fertility_v2 \
                  WHERE SCENARIO = "{self.scenario}" \
                  AND YEAR = "{self.current_projection_year - 1}"'
        fert_multiply = pl.read_sql(sql=query, con=con, index_col='AGE_GROUP')
        con.close()

        # adjust the county fertility rates using change factors from
        # Wittgenstein and then calculate births
        county_fert_rates = county_fert_rates.mul(other=fert_multiply, axis='index')
        county_fert_rates /= 1000.0  # fertility rates are births per 1,000
        births = df.mul(other=county_fert_rates, axis='index')

        births['MALE'] = births['VALUE'] * 0.512195122  # from Mathews, et al. (2005)
        births['FEMALE'] = births['VALUE'] - births['MALE']
        births = births.reset_index().drop(columns=['VALUE', 'AGE_GROUP'])
        births = births.melt(id_vars=['GEOID', 'RACE'], var_name='GENDER', value_name='VALUE')
        births = births.group_by(by=['GEOID', 'RACE', 'GENDER']).sum()

        assert not df.isnull().any().any()

        self.births = births.round().astype(int).copy()
        self.births['AGE_GROUP'] = '0-4'
        self.births.set_index(keys='AGE_GROUP', append=True, inplace=True)

        total_births_this_year = self.births.sum().values[0]

        # store time series of fertility in sqlite3
        db = OUTPUT_DATABASE
        con = sqlite3.connect(database=db, timeout=60)
        if self.current_projection_year == self.launch_year + 1:
            births = self.births.rename(columns={'VALUE': self.current_projection_year})
        else:
            query = f'SELECT * FROM births_by_race_age_{self.scenario}'
            births = pl.read_sql(sql=query,
                                 con=con,
                                 index_col=['GEOID', 'RACE', 'GENDER', 'AGE_GROUP'])
            current_births = self.births.copy()
            current_births = current_births.rename(columns={'VALUE': self.current_projection_year}).copy()
            births = pl.concat(objs=[births, current_births], axis=1)
        births.reset_index(inplace=True)
        births['AGE_GROUP'] = births['AGE_GROUP'].astype(age_group_dtype)
        births.sort_values(by=['GEOID', 'RACE', 'GENDER', 'AGE_GROUP'], inplace=True)
        assert births.shape[0] == 37320
        assert not births.isnull().any().any()
        births.to_sql(name=f'births_by_race_age_{self.scenario}',
                      con=con,
                      if_table_exists='replace',
                      index=False)
        con.close()

        print(f"finished! ({total_births_this_year:,} births this year)")


if __name__ == '__main__':
    print(time.ctime())
    main('SSP3')
    print(time.ctime())
