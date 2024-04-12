import os
import sqlite3

import pandas as pd

p1 = 'D:\\OneDrive\\Dissertation\\data\\ACS\\2006_2010'
f1 = 'county-to-county-by-hispanic-origin-2006-2010-current-residence-sort.xls'
xl1 = os.path.join(p1, f1)

p2 = 'D:\\OneDrive\\Dissertation\\data\\ACS\\2011_2015'
f2 = 'county-to-county-by-hispanic-origin-2011-2015-current-residence-sort.xlsx'
xl2 = os.path.join(p2, f2)

xl_files = [xl1, xl2]

years = ('2006-2010', '2011-2015')

RACE_MAP = {1: 'WHITE_NONHISPANIC', 2: 'NONHISPANIC', 3: 'HISPANIC'}


def main():
    dfs = []
    for xl_file in xl_files:
        columns = ('D_STFIPS', 'D_COFIPS', 'O_STFIPS', 'O_COFIPS', 'RACE', 'D_STATE', 'D_COUNTY', 'D_POP',
                   'D_POP_MOE', 'D_NONMOVERS', 'D_NONMOVERS_MOE', 'D_MOVERS',
                   'D_MOVERS_MOE', 'D_MOVERS_SAME_CY', 'D_MOVERS_SAME_CY_MOE',
                   'D_MOVERS_FROM_DIFF_CY_SAME_ST',
                   'D_MOVERS_FROM_DIFF_CY_SAME_ST_MOE', 'D_MOVERS_FROM_DIFF_ST',
                   'D_MOVERS_DIFF_ST_MOE', 'D_MOVERS_FROM_ABROAD',
                   'D_MOVERS_FROM_ABROAD_MOE', 'O_STATE', 'O_COUNTY', 'O_POP',
                   'O_POP_MOE', 'O_NONMOVERS', 'O_NOMMOVERS_MOE', 'O_MOVERS',
                   'O_MOVERS_MOE', 'O_MOVERS_SAME_CY', 'O_MOVERS_SAME_CY_MOE',
                   'O_MOVERS_FROM_DIFF_CY_SAME_ST',
                   'O_MOVERS_FROM_DIFF_CY_SAME_ST_MOE', 'O_MOVERS_FROM_DIFF_ST',
                   'O_MOVERS_DIFF_ST_MOE', 'O_MOVERS_PUERTO_RICO',
                   'O_MOVERS_PUERTO_RICO_MOE', 'TOTAL_FLOW', 'TOTAL_FLOW_MOE')

        xls = pd.ExcelFile(xl_file)
        df = pd.concat([xls.parse(sheet_name=name, header=None, names=columns, skiprows=4, skipfooter=8) for name in xls.sheet_names if name != 'Puerto Rico'])

        df = df[~df.O_STFIPS.str.contains('XXX')]

        foreign = ('EUR', 'ASI', 'SAM', 'ISL', 'NAM', 'CAM', 'CAR', 'AFR', 'OCE')
        df = df.loc[df.O_STFIPS.isin(foreign), ('D_STFIPS', 'D_COFIPS', 'RACE', 'TOTAL_FLOW')]

        df['D_STFIPS'] = df.D_STFIPS.astype('int').astype('str').str.zfill(2)
        df['D_COFIPS'] = df.D_COFIPS.astype('int').astype('str').str.zfill(3)
        df['DESTINATION_FIPS'] = df.D_STFIPS + df.D_COFIPS

        df.RACE.replace(to_replace=RACE_MAP, inplace=True)
        df = df[['DESTINATION_FIPS', 'RACE', 'TOTAL_FLOW']]

        assert not df.isnull().any().any()

        df = df.groupby(['DESTINATION_FIPS', 'RACE'], as_index=False).sum()
        df['RACE_SUM'] = df.groupby('RACE')['TOTAL_FLOW'].transform(sum)
        df['WEIGHT_x_10^6'] = (df['TOTAL_FLOW'] / df['RACE_SUM']) * 1000000
        df = df.pivot(index='DESTINATION_FIPS', columns='RACE', values='WEIGHT_x_10^6')
        df.reset_index(inplace=True)
        df.columns.name = None
        df.fillna(value=0, inplace=True)
        df = df[['DESTINATION_FIPS'] + list(RACE_MAP.values())]
        dfs.append(df)

    df1, df2 = dfs
    new1 = df2[~df2.DESTINATION_FIPS.isin(df1.DESTINATION_FIPS)]
    new1.loc[:, list(RACE_MAP.values())] = 0
    df1 = df1.append(other=new1, ignore_index=True, verify_integrity=True, sort=True)
    df1 = df1[['DESTINATION_FIPS'] + list(RACE_MAP.values())]
    df1.sort_values(by='DESTINATION_FIPS', inplace=True)

    new2 = df1[~df1.DESTINATION_FIPS.isin(df2.DESTINATION_FIPS)]
    new2.loc[:, list(RACE_MAP.values())] = 0
    df2 = df2.append(other=new2, ignore_index=True, verify_integrity=True, sort=True)
    df2 = df2[['DESTINATION_FIPS'] + list(RACE_MAP.values())]
    df2.sort_values(by='DESTINATION_FIPS', inplace=True)

    df1.set_index(keys='DESTINATION_FIPS', inplace=True)
    df2.set_index(keys='DESTINATION_FIPS', inplace=True)

    df = (df1 + df2) / 2

    df.reset_index(inplace=True)

    p = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases'
    f = 'acs.sqlite'
    con = sqlite3.connect(os.path.join(p, f))
    df.to_sql(name='acs_immigration_weights_hispanic_2006_2015',
              con=con,
              if_exists='replace',
              index=False)
    con.close()


if __name__ == '__main__':
    main()
