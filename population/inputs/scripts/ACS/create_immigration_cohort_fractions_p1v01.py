import os
import sqlite3

import pandas as pd


BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'
DATABASES = os.path.join(BASE_FOLDER, 'inputs\\databases')
ACS_DB = os.path.join(DATABASES, 'acs.sqlite')


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

    df = df.drop(columns='AGE')
    df['AGE_SEX_SUM'] = df.groupby(by=['AGE', 'SEX'], as_index=False)['VALUE'].transform('sum')
    df['PERCENT_OF_AGE_SEX_COHORT'] = df['VALUE'] / df['AGE_SEX_SUM']

    df = df.rename(columns={'DESTINATION_FIPS': 'GEOID'})
    df = df[['GEOID', 'AGE', 'SEX', 'PERCENT_OF_AGE_SEX_COHORT']]

    con = sqlite3.connect(ACS_DB)
    df.to_sql(name='acs_immigration_age_sex_fractions_2011_2015',
              con=con,
              if_exists='replace',
              index=False)
    con.close()

    df.to_csv(path_or_buf=os.path.join(DATABASES, 'acs_immigration_age_sex_fractions_2011_2015.csv'),
              index=False)

    print("Finished!")


if __name__ == '__main__':
    main()
