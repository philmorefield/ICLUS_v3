import os
import sqlite3

import pandas as pd


INPUT_FOLDER = 'D:\\OneDrive\\Dissertation\\analysis\\part_3\\inputs'
OUTPUT_DB = 'D:\\OneDrive\\ICLUS_v3\\ICLUS_v3_tables_for_RTI_20230426.sqlite'


def gather_witt_v2_tables():
    db = os.path.join(INPUT_FOLDER, 'wittgenstein\\wittgenstein.sqlite')
    con = sqlite3.connect(database=db)

    # read fertility
    query = 'SELECT YEAR, SCENARIO, AGE_GROUP, FERT_CHANGE_MULT \
             FROM age_specific_fertility_v2'
    fertility_v2 = pd.read_sql(sql=query, con=con)

    # read mortality
    query = 'SELECT * FROM age_specific_mortality_v2'
    mortality_v2 = pd.read_sql(sql=query, con=con)

    # read net (im)migration
    query = 'SELECT * FROM age_specific_net_migration_v2'
    net_migration_v2 = pd.read_sql(sql=query, con=con)

    con.close()
    con = sqlite3.connect(database=OUTPUT_DB)

    fertility_v2.to_sql(name='age_specific_fertility_v2',
                        con=con,
                        if_exists='replace',
                        index=False)

    mortality_v2.to_sql(name='age_specific_mortality_v2',
                        con=con,
                        if_exists='replace',
                        index=False)

    net_migration_v2.to_sql(name='age_specific_net_migration_v2',
                        con=con,
                        if_exists='replace',
                        index=False)

    con.close()


def gather_cdc_tables():
    db = os.path.join(INPUT_FOLDER, 'cdc\\cdc.sqlite')
    con = sqlite3.connect(database=db)

    # read fertility
    query = 'SELECT * FROM fertility_2013_2017_county_age_groups'
    fertility = pd.read_sql(sql=query, con=con)

    # read mortality
    query = 'SELECT * FROM mortality_2013_2017_county_age_groups'
    mortality = pd.read_sql(sql=query, con=con)

    con.close()
    con = sqlite3.connect(database=OUTPUT_DB)

    fertility.to_sql(name='fertility_2013_2017_county_age_groups',
                        con=con,
                        if_exists='replace',
                        index=False)

    mortality.to_sql(name='mortality_2013_2017_county_age_groups',
                        con=con,
                        if_exists='replace',
                        index=False)

    con.close()


def gather_census_tables():
    db = os.path.join(INPUT_FOLDER, 'census\\census.sqlite')
    con = sqlite3.connect(database=db)

    # read immigration fractions high
    query = 'SELECT * FROM annual_immigration_fraction_high'
    immig_high = pd.read_sql(sql=query, con=con)

    # read immigration fractions mid
    query = 'SELECT * FROM annual_immigration_fraction_mid'
    immig_mid = pd.read_sql(sql=query, con=con)

    # read immigration fractions high
    query = 'SELECT * FROM annual_immigration_fraction_low'
    immig_low = pd.read_sql(sql=query, con=con)

    con.close()
    con = sqlite3.connect(database=OUTPUT_DB)

    immig_low.to_sql(name='annual_immigration_fractions_mid',
                        con=con,
                        if_exists='replace',
                        index=False)

    immig_high.to_sql(name='annual_immigration_fractions_high',
                        con=con,
                        if_exists='replace',
                        index=False)

    immig_low.to_sql(name='annual_immigration_fractions_low',
                        con=con,
                        if_exists='replace',
                        index=False)

    con.close()


def gather_acs_tables():

    db = os.path.join(INPUT_FOLDER, 'acs\\acs.sqlite')
    con = sqlite3.connect(database=db)

    # read immigration cohort fractions
    query = 'SELECT * FROM acs_immigration_cohort_fractions_age_groups'
    immig_fractions = pd.read_sql(sql=query, con=con)

    con.close()
    con = sqlite3.connect(database=OUTPUT_DB)

    immig_fractions.to_sql(name='acs_immigration_cohort_fractions_age_groups',
                           con=con,
                           if_exists='replace',
                           index=False)

    con.close()



def main():
    gather_census_tables()
    gather_witt_v2_tables()
    gather_cdc_tables()
    gather_acs_tables()


if __name__ == '__main__':
    main()
