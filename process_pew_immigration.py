"""
Author:  Phil Morefield (pmorefie@gmu.edu)
Purpose: Calculate ethnic/racial proportions of annual immigration proportions by race
Created: 20190504
"""
import os
import sqlite3

import numpy as np
import pandas as pd

HISPANIC_RACE_WEIGHTS = {'WHITE': 0.5297,
                         'BLACK': 0.0246,
                         'ASIAN': 0.0041,
                         'NHPI': 0.0012,
                         'AIAN': 0.0136,
                         'TWO_OR_MORE': 0.0603,
                         'OTHER': 0.3666}

NONHISPANIC_RACE_WEIGHTS = {'WHITE': 0.7621,
                            'BLACK': 0.1459,
                            'ASIAN': 0.0560,
                            'NHPI': 0.0019,
                            'AIAN': 0.0087,
                            'TWO_OR_MORE': 0.0231,
                            'OTHER': 0.0023}

OUTPUT_DB = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases\\pew.sqlite'


def main():
    p = 'D:\\OneDrive\\Dissertation\\data\\Pew'
    f = 'Average Annual Immigration, 1960-65 to 2060-65.xlsx'
    xl = os.path.join(p, f)

    names = ['YEAR', 'TOTAL', 'WHITE', 'BLACK', 'HISPANIC', 'ASIAN', 'OTHER']
    df = pd.read_excel(io=xl, header=None, skiprows=16, skipfooter=15).drop(index=1)
    df = pd.DataFrame(data=np.repeat(df.values, 5, axis=0), columns=names)
    df.drop(columns='TOTAL', inplace=True)
    df['YEAR'] = range(2011, 2066)

    con = sqlite3.connect(OUTPUT_DB)
    df.to_sql(name='pew_immigration_total_by_race',
              con=con,
              if_exists='replace',
              index=False)
    con.close()

    df.set_index(keys='YEAR', inplace=True)
    df.columns = pd.MultiIndex.from_product([('NONHISPANIC',), df.columns])

    for key, value in HISPANIC_RACE_WEIGHTS.items():
        df[('HISPANIC', key)] = df[('NONHISPANIC', 'HISPANIC')] * value

    df.drop(columns=[('NONHISPANIC', 'HISPANIC'), ], inplace=True)

    df = df.divide(other=df.sum(axis=1), axis='index')
    df.columns = ['_'.join(col) for col in df.columns.values]
    df.reset_index(inplace=True)

    con = sqlite3.connect(OUTPUT_DB)
    df.to_sql(name='pew_immigration_proportions',
              con=con,
              if_exists='replace',
              index=False)
    con.close()

    print("Finished!")


if __name__ == '__main__':
    main()
