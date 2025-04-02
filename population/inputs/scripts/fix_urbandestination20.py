''''
Current values: 1 = Metro, 2 = Micro, 3 = Rural

In order to have "Rural" as the reference case for the regression:

New values: 1 = Rural, 2 = Micro, 3 = Metro

'''

import sqlite3

import pandas as pd



def main():
    db = 'D:\\OneDrive\\ICLUS_V3\\population\\inputs\\databases\\migration.sqlite'
    con = sqlite3.connect(database=db)

    query = 'SELECT * FROM fips_to_urb20_bea10_hhs'
    df = pd.read_sql_query(sql=query, con=con)

    df['URBANDESTINATION20'] = df['URBANDESTINATION20'].astype(int)

    df.loc[df.URBANDESTINATION20 == 1, 'URBANDESTINATION20'] = 99
    df.loc[df.URBANDESTINATION20 == 3, 'URBANDESTINATION20'] = 1
    df.loc[df.URBANDESTINATION20 == 99, 'URBANDESTINATION20'] = 3

    df.to_sql(name='fips_to_urb20_bea10_hhs',
              con=con,
              if_exists='replace',
              index=False)
    con.close()


if __name__ == '__main__':
    main()