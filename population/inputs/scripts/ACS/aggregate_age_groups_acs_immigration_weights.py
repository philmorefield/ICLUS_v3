import os
import sqlite3

import pandas as pd

'''
20210514 - aggregating some groups so that only WHITE has HISPANIC/NONHISPANIC
           subgroups. This is to align directly with the race and ethnicity
           breakouts from the 2017 Census immigration projections
'''

RENAME_DICT = {'HISPANIC_WHITE': 'HISP_WHITE',
               'HISPANIC_BLACK': 'BLACK',
               'HISPANIC_ASIAN': 'ASIAN',
               'HISPANIC_NHPI': 'NHPI',
               'HISPANIC_AIAN': 'AIAN',
               'HISPANIC_TWO_OR_MORE': 'TWO_OR_MORE',
               'NONHISPANIC_WHITE': 'NH_WHITE',
               'NONHISPANIC_BLACK': 'BLACK',
               'NONHISPANIC_ASIAN': 'ASIAN',
               'NONHISPANIC_NHPI': 'NHPI',
               'NONHISPANIC_AIAN': 'AIAN',
               'NONHISPANIC_TWO_OR_MORE': 'TWO_OR_MORE'}


def main():
    p = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases'
    f = 'acs.sqlite'
    con = sqlite3.connect(os.path.join(p, f))
    query = 'SELECT * FROM \
             acs_immigration_cohort_fractions_2006_2015'
    df = pd.read_sql(sql=query, con=con)

    for col in df.columns:
        if col in ('DESTINATION_FIPS', 'ETHNICITY_RACE', 'SEX'):
            continue

        if int(col) in list(range(0, 5)):
            df.rename(columns={col: '0-4'}, inplace=True)

        if int(col) in list(range(5, 10)):
            df.rename(columns={col: '5-9'}, inplace=True)

        if int(col) in list(range(10, 15)):
            df.rename(columns={col: '10-14'}, inplace=True)

        if int(col) in list(range(15, 20)):
            df.rename(columns={col: '15-19'}, inplace=True)

        if int(col) in list(range(20, 25)):
            df.rename(columns={col: '20-24'}, inplace=True)

        if int(col) in list(range(25, 30)):
            df.rename(columns={col: '25-29'}, inplace=True)

        if int(col) in list(range(30, 35)):
            df.rename(columns={col: '30-34'}, inplace=True)

        if int(col) in list(range(35, 40)):
            df.rename(columns={col: '35-39'}, inplace=True)

        if int(col) in list(range(40, 45)):
            df.rename(columns={col: '40-44'}, inplace=True)

        if int(col) in list(range(45, 50)):
            df.rename(columns={col: '45-49'}, inplace=True)

        if int(col) in list(range(50, 55)):
            df.rename(columns={col: '50-54'}, inplace=True)

        if int(col) in list(range(55, 60)):
            df.rename(columns={col: '55-59'}, inplace=True)

        if int(col) in list(range(60, 65)):
            df.rename(columns={col: '60-64'}, inplace=True)

        if int(col) in list(range(65, 70)):
            df.rename(columns={col: '65-69'}, inplace=True)

        if int(col) in list(range(70, 75)):
            df.rename(columns={col: '70-74'}, inplace=True)

        if int(col) in list(range(75, 80)):
            df.rename(columns={col: '75-79'}, inplace=True)

        if int(col) in list(range(80, 85)):
            df.rename(columns={col: '80-84'}, inplace=True)

        if int(col) >= 85:
            df.rename(columns={col: '85+'}, inplace=True)

    df = df.groupby(axis=1, level=0).sum()
    df['ETHNICITY_RACE'] = df['ETHNICITY_RACE'].map(RENAME_DICT)
    df = df.groupby(by=['DESTINATION_FIPS', 'ETHNICITY_RACE', 'SEX']).sum()

    # df.set_index(keys=['DESTINATION_FIPS', 'ETHNICITY_RACE', 'SEX'], inplace=True)
    df.columns.name = 'AGE_GROUP'
    df = pd.DataFrame(data=df.stack())
    df.columns = ['VALUE']

    df['DENOMENATOR'] = df.groupby(by=['ETHNICITY_RACE', 'SEX', 'AGE_GROUP'])['VALUE'].transform(sum)
    df.VALUE /= df.DENOMENATOR
    df = df[['VALUE']]
    df.reset_index(inplace=True)
    df['DESTINATION_FIPS'] = df['DESTINATION_FIPS'].astype(int).astype(str).str.zfill(5)

    df.rename(columns={'DESTINATION_FIPS': 'GEOID',
                       'ETHNICITY_RACE': 'RACE',
                       'VALUE': 'COUNTY_FRACTION'},
              inplace=True)
    df.to_sql(name='acs_immigration_cohort_fractions_by_age_group_2006_2015',
              if_exists='replace',
              con=con,
              index=False)
    con.close()

    print("Finished!")


if __name__ == '__main__':
    main()
