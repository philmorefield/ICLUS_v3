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
from multiprocessing import Pool

import pandas as pd

from pandas.api.types import CategoricalDtype

from migration import migration_2_c_ii_3_a as MigrationModel

pd.options.mode.chained_assignment = 'raise'

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
CDC_DB = os.path.join(INPUT_FOLDER, 'part_3', 'cdc.sqlite')
WITT_DB = os.path.join(INPUT_FOLDER, 'part_3', 'wittgenstein.sqlite')
CENSUS_DB = os.path.join(INPUT_FOLDER, 'part_3', 'census.sqlite')
ACS_DB = os.path.join(INPUT_FOLDER, 'part_3', 'acs.sqlite')

ETHNICITIES = ('HISPANIC', 'NONHISPANIC')
GENDERS = ('MALE', 'FEMALE')
RACES = ('WHITE', 'BLACK', 'ASIAN', 'AIAN', 'NHPI', 'TWO_OR_MORE')
AGE_GROUPS = ('0-4', '5-9', '10-14', '15-19', '20-24', '25-29', '30-34',
              '35-39', '40-44', '45-49', '50-54', '55-59', '60-64', '65-69',
              '70-74', '75-79', '80-84', '85+')

age_group_dtype = CategoricalDtype(categories=AGE_GROUPS, ordered=True)


def make_fips_changes(df=None):
    '''
    TODO: Add docstring
    '''

    con = sqlite3.connect(database=MIG_DB)
    query = 'SELECT OLD_FIPS, NEW_FIPS \
             FROM fips_or_name_changes'
    df_fips = pd.read_sql(sql=query, con=con)
    con.close()

    df = df.merge(right=df_fips,
                  how='left',
                  left_on='GEOID',
                  right_on='OLD_FIPS')
    df.loc[~df.NEW_FIPS.isnull(), 'GEOID'] = df.NEW_FIPS
    df.drop(columns=['OLD_FIPS', 'NEW_FIPS'], inplace=True)

    df = df.groupby(by=['GEOID', 'RACE', 'AGE_GROUP', 'GENDER'], as_index=False).sum()

    return df


def set_launch_population(launch_year):
    '''
    2015 launch population is taken from Census 2010-2019 Intercensal Population
    Estimates. Retrieved March 2, 2021.
    '''

    if launch_year != 2015:
        raise Exception

    con = sqlite3.connect(database=POP_DB, timeout=60)
    query = 'SELECT * FROM county_population_ageracegender_2015'
    df = pd.read_sql(sql=query, con=con)
    con.close()
    df = make_fips_changes(df=df)
    df.set_index(keys=['GEOID', 'RACE', 'GENDER', 'AGE_GROUP'],
                 inplace=True,
                 verify_integrity=True)
    df.columns = ['VALUE']
    df.sort_values(by=['GEOID', 'RACE', 'GENDER', 'AGE_GROUP'], inplace=True)

    assert df.shape[0] == 671760
    return df


def retrieve_baseline_migration_estimate():
    p = os.path.join(INPUT_FOLDER, 'part_4')
    db = os.path.join(p, 'baseline_migration_2015_2_c_ii_3_a.sqlite')
    con = sqlite3.connect(db, timeout=60)
    query = 'SELECT ORIGIN_FIPS, DESTINATION_FIPS, MIGRATION as BASELINE\
             FROM gross_migration_by_race_2015'
    df = pd.read_sql(sql=query, con=con, index_col=['ORIGIN_FIPS', 'DESTINATION_FIPS'])
    con.close()

    TOTAL_IN = df.groupby(by='DESTINATION_FIPS')['BASELINE'].sum()
    TOTAL_OUT = df.groupby(by='ORIGIN_FIPS')['BASELINE'].sum()
    df = pd.DataFrame(data=TOTAL_IN.sub(other=TOTAL_OUT, axis='index'))
    # df = df.round().astype(int)
    df.index.rename(name='GEOID', inplace=True)
    df.columns = ['MIGRATION']

    return df


def retrieve_intercensal_migration():
    p = os.path.join(INPUT_FOLDER, 'part_5')
    db = os.path.join(p, 'part_5_inputs.sqlite')
    con = sqlite3.connect(db, timeout=60)
    query = 'SELECT COFIPS AS GEOID, DOMESTICMIG2015 AS MIGRATION FROM baseline_net_migration_2015'
    df = pd.read_sql(sql=query, con=con, index_col='GEOID')
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
        self.launch_year = 2015
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

    def run(self, launch_year=2015, final_projection_year=2050):
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
            print(f"Total population (start): {self.current_pop.sum().sum():,}\n")

            # calculate deaths
            self.mortality()  # creates self.death
            self.current_pop = self.current_pop.sub(other=self.deaths, axis='index')
            assert self.current_pop.shape == (671760, 1)
            assert not self.current_pop.isnull().any().any()
            # self.current_pop.clip(lower=0, inplace=True)
            assert (self.current_pop >= 0).all().all()
            self.deaths = None

            # calculate net international immigration
            self.immigration()  # creates self.immigrants
            self.current_pop = self.current_pop.add(other=self.immigrants, axis='index', fill_value=0.0).astype(int)
            # correct for any cohorts that have negative population
            self.current_pop = self.current_pop.clip(lower=0).astype(int)

            assert self.current_pop.shape == (671760, 1)
            assert not self.current_pop.isnull().any().any()
            assert (self.current_pop >= 0).all().all()
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
                self.population_time_series = pd.concat(objs=[self.population_time_series, self.current_pop], axis=1)
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
                        if_exists='replace',
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
        advancers = pd.DataFrame(self.current_pop
                                 .groupby(by=['GEOID', 'RACE', 'GENDER'])
                                 .VALUE
                                 .transform(lambda x: x.shift() * 0.2))

        # reduce the population in each age cohort by 20%, except for 85+
        self.current_pop.loc[self.current_pop.index.get_level_values('AGE_GROUP') != "85+", 'VALUE'] *= 0.8
        self.current_pop = self.current_pop.add(other=pd.DataFrame(advancers),
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

        mort_race_map = {'WHITE': 'WHITE',
                         'BLACK': 'BLACK',
                         'AIAN': 'AIAN',
                         'ASIAN': 'ASIAN',
                         'NHPI': 'ASIAN',
                         'OTHER': 'MULTI',
                         'TWO_OR_MORE': 'MULTI'}

        # get CDC mortality rates by AGE_GROUP, RACE, GENDER, and COUNTY
        con = sqlite3.connect(database=CDC_DB, timeout=60)
        query = 'SELECT RACE AS MORT_RACE, AGE_GROUP, GENDER, COFIPS AS GEOID, MORTALITY AS MORTALITY_RATE_100K \
                 FROM mortality_2013_2017_county_age_groups'
        county_mort_rates = pd.read_sql(sql=query, con=con)
        con.close()

        df = self.current_pop.copy()
        df.reset_index(inplace=True)
        df['MORT_RACE'] = df['RACE'].map(mort_race_map)
        df = df.merge(right=county_mort_rates,
                      how='left',
                      on=['MORT_RACE', 'AGE_GROUP', 'GENDER', 'GEOID'],
                      copy=False)
        assert df.shape[0] == 671760
        assert not df.isnull().any().any()
        df.drop(columns='MORT_RACE', inplace=True)

        # get Wittgenstein mortality rate adjustments
        con = sqlite3.connect(database=WITT_DB, timeout=60)
        query = f'SELECT AGE_GROUP, GENDER, MORT_CHANGE_MULT AS MORT_MULTIPLY \
                  FROM age_specific_mortality_v2 \
                  WHERE SCENARIO = "{self.scenario}" \
                  AND YEAR = "{self.current_projection_year - 1}"'
        mort_multiply = pd.read_sql(sql=query, con=con)
        con.close()

        df = df.merge(right=mort_multiply, how='left', on=['AGE_GROUP', 'GENDER'])
        assert df.shape[0] == 671760
        df.eval('MORT_PROJ = (MORTALITY_RATE_100K * MORT_MULTIPLY) / 100000.0', inplace=True)

        # calculate deaths
        df.eval('DEATHS = MORT_PROJ * VALUE', inplace=True)
        df.set_index(keys=['GEOID', 'RACE', 'GENDER', 'AGE_GROUP'], inplace=True)
        df = df[['DEATHS']]
        df.columns = ['VALUE']
        assert not df.isnull().any().any()

        # store deaths
        df = df.round().astype(int)
        self.deaths = df.copy()
        total_deaths_this_year = self.deaths.sum().values[0]

        # store time series of mortality in sqlite3
        db = OUTPUT_DATABASE
        con = sqlite3.connect(database=db, timeout=60)
        if self.current_projection_year == self.launch_year + 1:
            deaths = self.deaths.rename(columns={'VALUE': self.current_projection_year})
        else:
            query = f'SELECT * FROM deaths_by_race_gender_age_{self.scenario}'
            deaths = pd.read_sql(sql=query,
                                 con=con,
                                 index_col=['GEOID', 'RACE', 'GENDER', 'AGE_GROUP'])
            current_deaths = self.deaths.copy()
            current_deaths = current_deaths.rename(columns={'VALUE': self.current_projection_year}).copy()
            deaths = pd.concat(objs=[deaths, current_deaths], axis=1)
        deaths.reset_index(inplace=True)
        deaths['AGE_GROUP'] = deaths['AGE_GROUP'].astype(age_group_dtype)
        deaths.sort_values(by=['GEOID', 'RACE', 'GENDER', 'AGE_GROUP'], inplace=True)
        assert deaths.shape[0] == 671760
        assert not deaths.isnull().any().any()

        deaths.to_sql(name=f'deaths_by_race_gender_age_{self.scenario}',
                      con=con,
                      if_exists='replace',
                      index=False)
        con.close()

        print(f"finished! ({total_deaths_this_year:,} deaths this year)")

    def immigration(self):
        '''
        Calculate net immigration
        '''
        print("Calculating net immigration...", end='')
        # get the County level age-race-ethnicity-sex proportions
        con = sqlite3.connect(database=ACS_DB, timeout=60)
        query = 'SELECT *  FROM acs_immigration_cohort_fractions_age_groups'
        county_weights = pd.read_sql(sql=query, con=con, index_col=['DESTINATION_FIPS', 'ETHNICITY_RACE', 'GENDER', 'AGE_GROUP'])
        con.close()

        # this is the net migrants for each age-gender combination
        con = sqlite3.connect(database=WITT_DB, timeout=60)
        query = f'SELECT AGE_GROUP, GENDER, NETMIG_INTERP AS NET \
                  FROM age_specific_net_migration_v2 \
                  WHERE SCENARIO = "{self.scenario}" \
                  AND YEAR = "{self.current_projection_year}"'
        df_witt = pd.read_sql(sql=query, con=con)
        con.close()

        # result of this is a Series
        witt = df_witt.set_index(keys=['AGE_GROUP', 'GENDER']).loc[:, 'NET']

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
        con = sqlite3.connect(database=CENSUS_DB, timeout=60)
        query = f'SELECT * \
                  FROM annual_immigration_fraction_{ratio} \
                  WHERE year = "{self.current_projection_year}"'
        df_census = pd.read_sql(sql=query, con=con)
        con.close()
        df_census.drop(columns='YEAR', inplace=True)
        df_census.set_index(keys=['AGE_GROUP', 'GENDER'], inplace=True)

        # multiply annual immigration by the agegroup/race/sex proportions
        all_immig_cohorts = df_census.mul(other=witt, axis='index')

        all_immig_cohorts.columns.name = 'ETHNICITY_RACE'
        all_immig_cohorts = all_immig_cohorts.stack()
        all_immig_cohorts = all_immig_cohorts.reset_index().set_index(keys=['ETHNICITY_RACE', 'GENDER', 'AGE_GROUP'])
        all_immig_cohorts.columns = ['VALUE']

        df = pd.DataFrame(data=county_weights.mul(other=all_immig_cohorts, axis='index'), columns=['VALUE'])
        assert abs(all_immig_cohorts.sum().values[0] - df.sum().values[0]) < 1
        assert not df.isnull().any().any()

        # clean things up
        df.reset_index(inplace=True)
        df.rename(columns={'DESTINATION_FIPS': 'GEOID', 'ETHNICITY_RACE': 'RACE'}, inplace=True)
        df['RACE'] = df['RACE'].str.replace('NH_WHITE', 'WHITE')
        df['RACE'] = df['RACE'].str.replace('HISP_WHITE', 'WHITE')
        df = df.groupby(by=['GEOID', 'RACE', 'GENDER', 'AGE_GROUP']).sum()

        # store immigrants
        self.immigrants = df.round().astype(int)

        # store time series of immigration in sqlite3
        db = OUTPUT_DATABASE
        con = sqlite3.connect(database=db, timeout=60)
        if self.current_projection_year == self.launch_year + 1:
            immigration = self.immigrants.rename(columns={'VALUE': self.current_projection_year}).copy()
        else:
            query = f'SELECT * FROM immigration_by_race_gender_age_{self.scenario}'
            immigration = pd.read_sql(sql=query,
                                      con=con,
                                      index_col=['GEOID', 'RACE', 'GENDER', 'AGE_GROUP'])
            current_immigration = self.immigrants.copy()
            current_immigration = current_immigration.rename(columns={'VALUE': self.current_projection_year}).copy()
            immigration = pd.concat(objs=[immigration, current_immigration], axis=1)
        immigration.reset_index(inplace=True)
        immigration['AGE_GROUP'] = immigration['AGE_GROUP'].astype(age_group_dtype)
        immigration.sort_values(by=['GEOID', 'RACE', 'GENDER', 'AGE_GROUP'], inplace=True)
        assert not immigration.isnull().any().any()
        immigration.to_sql(name=f'immigration_by_race_gender_age_{self.scenario}',
                           con=con,
                           if_exists='replace',
                           index=False)
        con.close()

        total_immigrants_this_year = self.immigrants.sum().values[0]
        print(f"finished! ({total_immigrants_this_year:,} net immigrants this year)")

    def migration(self):
        '''
        Calculate domestic migration
        '''
        print("Calculating domestic migration...")

        migration_model = MigrationModel()
        migration_model.current_pop = self.current_pop.copy()

        for race in RACES:
            print(f"\t{race}...")

            # compute all county to county migration flows
            gross_migration_flows = migration_model.compute_migrants(race)
            gross_migration_flows['RACE'] = race
            gross_migration_flows.set_index(keys='RACE',
                                            append=True,
                                            inplace=True,
                                            verify_integrity=True)

            # the MIGRATION column is actually a gender ratio, but naming it
            # MIGRATION to match gross_migration_flows makes the multiplication
            # step cleaner
            ratio = self.current_pop.query('RACE == @race').copy()
            ratio['GEOID_AGE_POP'] = ratio.groupby(by=['GEOID', 'AGE_GROUP']).transform('sum')
            ratio['MIGRATION'] = (ratio['VALUE'] / ratio['GEOID_AGE_POP']).fillna(0)
            ratio = ratio[['MIGRATION']].copy()
            ratio.index.rename(names='ORIGIN_FIPS', level='GEOID', inplace=True)

            ratio_male = ratio.query('GENDER == "MALE"').reset_index(level=['GENDER', 'RACE'], drop=True)
            gross_male = gross_migration_flows.mul(other=ratio_male, axis='index', fill_value=0)
            migin_male = pd.DataFrame(gross_male.groupby(by=['DESTINATION_FIPS', 'AGE_GROUP'])['MIGRATION'].sum())
            migin_male.index.rename(names='GEOID', level='DESTINATION_FIPS', inplace=True)
            migout_male = pd.DataFrame(gross_male.groupby(by=['ORIGIN_FIPS', 'AGE_GROUP'])['MIGRATION'].sum())
            migout_male.index.rename(names='GEOID', level='ORIGIN_FIPS', inplace=True)
            mignet_male = pd.DataFrame(migin_male.sub(other=migout_male, axis='index', fill_value=0))
            mignet_male['GENDER'] = 'MALE'
            mignet_male.set_index(keys='GENDER', append=True, inplace=True)

            ratio_female = ratio.query('GENDER == "FEMALE"').reset_index(level=['GENDER', 'RACE'], drop=True)
            gross_female = gross_migration_flows.mul(other=ratio_female, axis='index', fill_value=0)
            migin_female = gross_female.groupby(by=['DESTINATION_FIPS', 'AGE_GROUP'])['MIGRATION'].sum()
            migin_female.index.rename(names='GEOID', level='DESTINATION_FIPS', inplace=True)
            migout_female = gross_female.groupby(by=['ORIGIN_FIPS', 'AGE_GROUP'])['MIGRATION'].sum()
            migout_female.index.rename(names='GEOID', level='ORIGIN_FIPS', inplace=True)
            mignet_female = pd.DataFrame(migin_female.sub(other=migout_female, axis='index', fill_value=0))
            mignet_female['GENDER'] = 'FEMALE'
            mignet_female.set_index(keys='GENDER', append=True, inplace=True)

            mignet = pd.concat(objs=[mignet_male, mignet_female])
            mignet['RACE'] = race
            mignet.set_index(keys='RACE', append=True, inplace=True)
            assert mignet.shape == (111960, 1)

            if self.net_migration is None:
                self.net_migration = mignet.copy()
            else:
                self.net_migration = pd.concat(objs=[self.net_migration, mignet])

        assert self.net_migration.shape == (671760, 1)
        # assert self.net_migration_r.shape == (671760, 1)

        # using the % change in net migration by county from my model for the
        # years 2015 and the projected year, calculate new net migration by
        # county using observed net migration for 2015 (i.e., delta method)
        baseline_migration_estimate = retrieve_baseline_migration_estimate()
        net_migration_total = self.net_migration.groupby(by='GEOID').sum()
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
            migration = pd.read_sql(sql=query,
                                    con=con,
                                    index_col=['GEOID', 'AGE_GROUP', 'GENDER', 'RACE'])
            current_migration = self.net_migration.copy()
            current_migration.rename(columns={'MIGRATION': self.current_projection_year}, inplace=True)
            migration = pd.concat(objs=[migration, current_migration], axis=1)
        migration.reset_index(inplace=True)
        migration['AGE_GROUP'] = migration['AGE_GROUP'].astype(age_group_dtype)
        migration.sort_values(by=['GEOID', 'RACE', 'GENDER', 'AGE_GROUP'], inplace=True)
        assert migration.shape[0] == 671760
        assert not migration.isnull().any().any()
        migration.to_sql(name=f'migration_by_race_gender_age_{self.scenario}',
                         con=con,
                         if_exists='replace',
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
        county_fert_rates = pd.read_sql(sql=query, con=con)
        con.close()

        age_groups = list(county_fert_rates.AGE_GROUP.unique())
        # county_fert_rates = pd.DataFrame(data=county_fert_rates.eval('RACE = RACE.str.replace("MULTI", "TWO_OR_MORE")', engine='python'))
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
        fert_multiply = pd.read_sql(sql=query, con=con, index_col='AGE_GROUP')
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
        births = births.groupby(by=['GEOID', 'RACE', 'GENDER']).sum()

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
            births = pd.read_sql(sql=query,
                                 con=con,
                                 index_col=['GEOID', 'RACE', 'GENDER', 'AGE_GROUP'])
            current_births = self.births.copy()
            current_births = current_births.rename(columns={'VALUE': self.current_projection_year}).copy()
            births = pd.concat(objs=[births, current_births], axis=1)
        births.reset_index(inplace=True)
        births['AGE_GROUP'] = births['AGE_GROUP'].astype(age_group_dtype)
        births.sort_values(by=['GEOID', 'RACE', 'GENDER', 'AGE_GROUP'], inplace=True)
        assert births.shape[0] == 37320
        assert not births.isnull().any().any()
        births.to_sql(name=f'births_by_race_age_{self.scenario}',
                      con=con,
                      if_exists='replace',
                      index=False)
        con.close()

        print(f"finished! ({total_births_this_year:,} births this year)")


if __name__ == '__main__':
    main('SSP3')
    print(time.ctime())

    # all_scenarios = ('SSP1', 'SSP2', 'SSP3')
# #
    # with Pool(processes=3) as pool:
        # pool.map(main, all_scenarios)
        # pool.terminate()
        # pool.join()
    print(time.ctime())
