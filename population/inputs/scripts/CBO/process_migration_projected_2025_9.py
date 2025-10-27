import os
import sqlite3

import pandas as pd

CBO_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\raw_files\\CBO'
CSV_FOLDER = 'demographic_projections_2025_9//CSV files'
CSV_FILE = 'grossMigration_byYearAgeSexStatusFlow.csv'
OUTPUT_DB = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases\\cbo.sqlite'


def main():
    df = pd.read_csv(filepath_or_buffer=os.path.join(CBO_FOLDER, CSV_FOLDER, CSV_FILE))
    df.columns = ['YEAR', 'AGE', 'SEX', 'STATUS', 'TYPE', 'FLOW']
    df.drop(columns=['STATUS'], inplace=True)
    df.AGE = df.AGE.str.replace('+', '').astype(int)
    df.SEX = df.SEX.str.upper()
    df.TYPE = df.TYPE.str.upper()
    df.FLOW = df.FLOW.astype(int)

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

    # aggregate by age group and pivot
    df.drop(columns='AGE', inplace=True)
    df = df.groupby(by=['YEAR', 'AGE_GROUP', 'SEX', 'TYPE'], as_index=False).sum()
    df = df.pivot(index=['YEAR', 'AGE_GROUP', 'SEX'],
                  columns='TYPE',
                  values='FLOW').fillna(0).reset_index()
    df = df.eval('NET_IMMIGRATION = IMMIGRATION - EMIGRATION')

    con = sqlite3.connect(database=OUTPUT_DB)
    df.to_sql(name='cbo_2025_9_migration',
              con=con,
              if_exists='replace',
              index=False)
    con.close()


if __name__ == '__main__':
    main()
