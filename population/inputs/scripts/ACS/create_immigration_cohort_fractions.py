import os
import sqlite3

from itertools import product

import pandas as pd

DATABASES = 'D:\\projects\\ICLUS_v3\\population\\inputs\\databases'
ACS_DB = os.path.join(DATABASES, 'acs.sqlite')
MIGRATION_DB = os.path.join(DATABASES, 'migration.sqlite')

ETHNICITIES = ('HISPANIC', 'NONHISPANIC')
RACES = ('WHITE', 'BLACK', 'ASIAN', 'NHPI', 'AIAN', 'OTHER', 'TWO_OR_MORE')


def retrieve_race_weights():
    '''
    For each of four races (WHITE, BLACK, ASIAN, OTHER), this calculates the
    average annual (2011-2015) percentage of immigrants for each county.
    '''
    print("Processing race weights...")

    con = sqlite3.connect(ACS_DB)
    query = 'SELECT * FROM acs_immigration_weights_race_2006_2015'
    df = pd.read_sql(sql=query, index_col='DESTINATION_FIPS', con=con)
    con.close()

    return df


def retrieve_sex_weights():
    print("Processing sex weights...")

    con = sqlite3.connect(ACS_DB)
    query = 'SELECT * FROM acs_immigration_weights_sex_2006_2015'
    # df = pd.read_sql(sql=query, index_col=['DESTINATION_FIPS', 'SEX'], con=con)
    df = pd.read_sql(sql=query, con=con)
    df = df.melt(id_vars=['DESTINATION_FIPS'], var_name='SEX', value_name='SEX_FRACTION')
    df = df.set_index(keys=['DESTINATION_FIPS', 'SEX'])
    con.close()

    return df


def retrieve_age_weights():
    print("Processing age weights...")

    con = sqlite3.connect(ACS_DB)
    query = 'SELECT * FROM acs_immigration_weights_age_2006_2015'
    df = pd.read_sql(sql=query, index_col='DESTINATION_FIPS', con=con)
    con.close()

    return df


def retrieve_hispanic_weights():
    print("Processing hispanic weights...")

    con = sqlite3.connect(ACS_DB)
    query = 'SELECT * FROM acs_immigration_weights_hispanic_2006_2015'
    df = pd.read_sql(sql=query, index_col='DESTINATION_FIPS', con=con)
    con.close()

    return df


def main():
    cy_race = retrieve_race_weights()
    cy_sex = retrieve_sex_weights()
    cy_hispanic = retrieve_hispanic_weights()
    cy_age = retrieve_age_weights()

    df = None
    for ethnicity, race in product(ETHNICITIES, RACES):
        # TODO: Re-evaluate how to handle OTHER immigration
        if race == 'OTHER':
            continue
        label = f'{ethnicity}_{race}'

        # multiply weights for RACE, HISPANIC, AGE, and SEX
        if label == 'NONHISPANIC_WHITE':
            weight3 = cy_hispanic['WHITE_NONHISPANIC']
        else:
            weight1 = cy_hispanic[ethnicity]
            if race in ('WHITE', 'BLACK', 'ASIAN'):
                weight2 = cy_race[race]
            else:
                weight2 = cy_race['OTHER']
            weight3 = weight1.add(other=weight2, axis='index', fill_value=0)
        temp = cy_age.add(other=weight3, axis='index').fillna(0)

        # convert the product of all weights into fractions
        if label == 'NONHISPANIC_WHITE':
            temp.update(other=temp.div(other=2000000))
        else:
            temp.update(other=temp.div(other=3000000))
        if temp.sum().min() < 0.9:
            print("Wait!")

        temp = pd.concat(objs=[temp, temp], keys=['MALE', 'FEMALE'], names=['SEX'])
        temp = temp.reorder_levels(order=['DESTINATION_FIPS', 'SEX']).sort_index()
        temp.update(other=temp.mul(other=cy_sex.squeeze(), axis='index'))

        temp.reset_index(inplace=True)
        temp['ETHNICITY_RACE'] = label

        if df is None:
            df = temp.copy()
        else:
            df = pd.concat(objs=[df, temp], ignore_index=True)

        del temp

    df.set_index(keys=['DESTINATION_FIPS', 'ETHNICITY_RACE', 'SEX'], inplace=True)
    df.reset_index(inplace=True)

    con = sqlite3.connect(ACS_DB)
    df.to_sql(name='acs_immigration_cohort_fractions_2006_2015',
              con=con,
              if_exists='replace',
              index=False)
    con.close()

    print("Finished!")


if __name__ == '__main__':
    main()
