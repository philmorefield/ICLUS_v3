import os
import sqlite3

import pandas as pd


INPUT_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases'
OUTPUT_DB = 'D:\\OneDrive\\ICLUS_v3\\for_rti\\ICLUS_v3_tables_for_RTI_20250314.sqlite'


def gather_witt_v3_tables():
    db = os.path.join(INPUT_FOLDER, 'wittgenstein.sqlite')
    con = sqlite3.connect(database=db)

    # read fertility
    query = 'SELECT * FROM age_specific_fertility_v3'
    fertility_v3 = pd.read_sql(sql=query, con=con)

    # read mortality
    query = 'SELECT * FROM age_specific_mortality_v3'
    mortality_v3 = pd.read_sql(sql=query, con=con)

    # read net (im)migration
    query = 'SELECT * FROM age_specific_net_migration_v3'
    net_migration_v3 = pd.read_sql(sql=query, con=con)

    con.close()
    con = sqlite3.connect(database=OUTPUT_DB)

    fertility_v3.to_sql(name='age_specific_fertility_v3',
                        con=con,
                        if_exists='replace',
                        index=False)

    mortality_v3.to_sql(name='age_specific_mortality_v3',
                        con=con,
                        if_exists='replace',
                        index=False)

    net_migration_v3.to_sql(name='age_specific_net_migration_v3',
                        con=con,
                        if_exists='replace',
                        index=False)

    con.close()


def gather_2020_cdc_tables():
    db = os.path.join(INPUT_FOLDER, 'cdc.sqlite')
    con = sqlite3.connect(database=db)

    # read fertility
    query = 'SELECT * FROM fertility_2018_2022_county'
    fertility = pd.read_sql(sql=query, con=con)

    # read mortality
    query = 'SELECT * FROM mortality_2018_2022_county'
    mortality = pd.read_sql(sql=query, con=con)

    con.close()
    con = sqlite3.connect(database=OUTPUT_DB)

    fertility.to_sql(name='fertility_2018_2022_county',
                     con=con,
                     if_exists='replace',
                     index=False)

    mortality.to_sql(name='mortality_2018_2022_county',
                     con=con,
                     if_exists='replace',
                     index=False)

    con.close()


def gather_2010_cdc_tables():
    INPUT_FOLDER_2010 = 'D:\\OneDrive\\Dissertation\\analysis\\part_3\\inputs\\cdc'
    db = os.path.join(INPUT_FOLDER_2010, 'cdc.sqlite')
    con = sqlite3.connect(database=db)

    # read fertility
    query = 'SELECT * FROM fertility_2008_2012_county'
    fertility = pd.read_sql(sql=query, con=con)

    # read mortality
    query = 'SELECT * FROM mortality_2008_2012_county'
    mortality = pd.read_sql(sql=query, con=con)

    con.close()
    con = sqlite3.connect(database=OUTPUT_DB)

    fertility.to_sql(name='fertility_2008_2012_county',
                     con=con,
                     if_exists='replace',
                     index=False)

    mortality.to_sql(name='mortality_2008_2012_county',
                     con=con,
                     if_exists='replace',
                     index=False)

    con.close()


def gather_census_tables():
    db = os.path.join(INPUT_FOLDER, 'census.sqlite')
    con = sqlite3.connect(database=db)

    # # read immigration fractions high
    # query = 'SELECT * FROM annual_immigration_fraction_high'
    # immig_high = pd.read_sql(sql=query, con=con)

    # read immigration fractions mid
    query = 'SELECT * FROM annual_immigration_fraction_mid'
    immig_mid = pd.read_sql(sql=query, con=con)

    # # read immigration fractions high
    # query = 'SELECT * FROM annual_immigration_fraction_low'
    # immig_low = pd.read_sql(sql=query, con=con)

    con.close()
    con = sqlite3.connect(database=OUTPUT_DB)

    for year in range(2011, 2015):
        temp = immig_mid.query('YEAR == 2015')
        temp.loc[:, 'YEAR'] = year
        immig_mid = pd.concat([immig_mid, temp])

    immig_mid = immig_mid.sort_values(by=['YEAR', 'AGE_GROUP']).reset_index(drop=True)

    immig_mid.to_sql(name='annual_immigration_fractions_mid',
                        con=con,
                        if_exists='replace',
                        index=False)

    # immig_high.to_sql(name='annual_immigration_fractions_high',
    #                   con=con,
    #                   if_exists='replace',
    #                   index=False)

    # immig_low.to_sql(name='annual_immigration_fractions_low',
    #                  con=con,
    #                  if_exists='replace',
    #                  index=False)

    con.close()


def gather_acs_tables():

    db = os.path.join(INPUT_FOLDER, 'acs.sqlite')
    con = sqlite3.connect(database=db)

    # read immigration cohort fractions
    query = 'SELECT * FROM acs_immigration_cohort_fractions_by_age_group_2006_2015'
    immig_fractions = pd.read_sql(sql=query, con=con)

    con.close()
    con = sqlite3.connect(database=OUTPUT_DB)
    immig_fractions.to_sql(name='acs_immigration_cohort_fractions_by_age_group_2006_2015',
                           con=con,
                           if_exists='replace',
                           index=False)
    con.close()


def main():
    gather_census_tables()
    gather_witt_v3_tables()
    gather_2020_cdc_tables()
    # gather_2010_cdc_tables()
    gather_acs_tables()


if __name__ == '__main__':
    main()
