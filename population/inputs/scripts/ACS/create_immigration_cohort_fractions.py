import os
import sqlite3

import pandas as pd

DATABASES = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases'
ACS_DB = os.path.join(DATABASES, 'acs.sqlite')
MIGRATION_DB = os.path.join(DATABASES, 'migration.sqlite')


def retrieve_sex_weights():
    print("Processing sex weights...")

    con = sqlite3.connect(ACS_DB)
    query = 'SELECT * FROM acs_immigration_weights_sex_2011_2015'
    df = pd.read_sql(sql=query, con=con)
    df = df.melt(id_vars=['DESTINATION_FIPS'], var_name='SEX', value_name='VALUE')
    df = df.set_index(keys=['DESTINATION_FIPS', 'SEX'])
    con.close()

    return df


def retrieve_age_weights():
    print("Processing age weights...")

    con = sqlite3.connect(ACS_DB)
    query = 'SELECT * FROM acs_immigration_weights_age_2011_2015'
    df = pd.read_sql(sql=query, con=con)
    con.close()

    df = (df.melt(id_vars=['DESTINATION_FIPS'],
                 var_name='AGE',
                 value_name='VALUE')
            .set_index(keys=['DESTINATION_FIPS', 'AGE']))

    return df


def main():
    sex = retrieve_sex_weights()
    age = retrieve_age_weights()

    df = sex.mul(other=age, axis='index').reset_index()
    df['AGE'] = df.AGE.astype(int)

    # bin rows by age group
    df['AGE_GROUP'] = '0-4'
    df.loc[df.AGE >= 85, 'AGE_GROUP'] = '85+'
    df.loc[df.AGE.between(80, 84), 'AGE_GROUP'] = '80-84'
    df.loc[df.AGE.between(75, 79), 'AGE_GROUP'] = '75-79'
    df.loc[df.AGE.between(70, 74), 'AGE_GROUP'] = '70-74'
    df.loc[df.AGE.between(65, 69), 'AGE_GROUP'] = '65-69'
    df.loc[df.AGE.between(60, 64), 'AGE_GROUP'] = '60-64'
    df.loc[df.AGE.between(55, 59), 'AGE_GROUP'] = '55-59'
    df.loc[df.AGE.between(50, 54), 'AGE_GROUP'] = '50-54'
    df.loc[df.AGE.between(45, 49), 'AGE_GROUP'] = '45-49'
    df.loc[df.AGE.between(40, 44), 'AGE_GROUP'] = '40-44'
    df.loc[df.AGE.between(35, 39), 'AGE_GROUP'] = '35-39'
    df.loc[df.AGE.between(30, 34), 'AGE_GROUP'] = '30-34'
    df.loc[df.AGE.between(25, 29), 'AGE_GROUP'] = '25-29'
    df.loc[df.AGE.between(20, 24), 'AGE_GROUP'] = '20-24'
    df.loc[df.AGE.between(15, 19), 'AGE_GROUP'] = '15-19'
    df.loc[df.AGE.between(10, 14), 'AGE_GROUP'] = '10-14'
    df.loc[df.AGE.between(5, 9), 'AGE_GROUP'] = '5-9'

    df = df.drop(columns='AGE')
    df = df.groupby(by=['DESTINATION_FIPS', 'AGE_GROUP', 'SEX'], as_index=False).sum()
    df['AGE_SEX_SUM'] = df.groupby(by=['AGE_GROUP', 'SEX'], as_index=False)['VALUE'].transform('sum')
    df['PERCENT_OF_AGE_SEX_COHORT'] = df['VALUE'] / df['AGE_SEX_SUM']

    df = df.rename(columns={'DESTINATION_FIPS': 'GEOID'})
    df = df[['GEOID', 'AGE_GROUP', 'SEX', 'PERCENT_OF_AGE_SEX_COHORT']]

    con = sqlite3.connect(ACS_DB)
    df.to_sql(name='acs_immigration_age_sex_fractions_2011_2015',
              con=con,
              if_exists='replace',
              index=False)
    con.close()

    print("Finished!")


if __name__ == '__main__':
    main()
