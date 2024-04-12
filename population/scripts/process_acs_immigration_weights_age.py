import os
import sqlite3

import pandas as pd

p1 = 'D:\\OneDrive\\Dissertation\\data\\ACS\\2006_2010'
f1 = 'county-to-county-by-age-2006-2010-current-residence-sort.xls'
xl1 = os.path.join(p1, f1)

p2 = 'D:\\OneDrive\\Dissertation\\data\\ACS\\2011_2015'
f2 = 'county-to-county-by-age-2011-2015-current-residence-sort.xlsx'
xl2 = os.path.join(p2, f2)

xl_files = [xl1, xl2]

years = ('2006-2010', '2011-2015')

AGE_MAP = {1: '1_to_4',
           2: '5_to_17',
           3: '18_to_19',
           4: '20_to_24',
           5: '25_to_29',
           6: '30_to_34',
           7: '35_to_39',
           8: '40_to_44',
           9: '45_to_49',
           10: '50_to_54',
           11: '55_to_59',
           12: '60_to_64',
           13: '65_to_69',
           14: '70_to_74',
           15: '75+'}


def parse_age_groups(s):

    if s == '1_to_4':
        return range(0, 5)
    elif '_to_' in s:
        s = s.split('_to_')
        return list(range(int(s[0]), int(s[1]) + 1))
    elif s == '75+':
        return range(75, 100)
    else:
        raise Exception


def main():
    dfs = []
    for xl_file in xl_files:
        columns = ('D_STFIPS', 'D_COFIPS', 'O_STFIPS', 'O_COFIPS', 'AGE', 'D_STATE', 'D_COUNTY', 'D_POP',
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
        df = df.loc[df.O_STFIPS.isin(foreign), ('D_STFIPS', 'D_COFIPS', 'AGE', 'TOTAL_FLOW')]

        df['D_STFIPS'] = df.D_STFIPS.astype('int').astype('str').str.zfill(2)
        df['D_COFIPS'] = df.D_COFIPS.astype('int').astype('str').str.zfill(3)
        df['DESTINATION_FIPS'] = df.D_STFIPS + df.D_COFIPS

        df.AGE.replace(to_replace=AGE_MAP, inplace=True)
        df = df[['DESTINATION_FIPS', 'AGE', 'TOTAL_FLOW']]

        assert not df.isnull().any().any()

        df = df.groupby(['DESTINATION_FIPS', 'AGE'], as_index=False).sum()
        df['AGE_SUM'] = df.groupby('AGE')['TOTAL_FLOW'].transform(sum)
        df['WEIGHT_x_10^6'] = (df['TOTAL_FLOW'] / df['AGE_SUM']) * 1000000
        df = df.pivot(index='DESTINATION_FIPS', columns='AGE', values='WEIGHT_x_10^6')
        df.reset_index(inplace=True)
        df.columns.name = None
        df.fillna(value=0, inplace=True)
        df = df[['DESTINATION_FIPS'] + list(AGE_MAP.values())]
        dfs.append(df)

    df1, df2 = dfs
    new1 = df2[~df2.DESTINATION_FIPS.isin(df1.DESTINATION_FIPS)]
    new1.loc[:, list(AGE_MAP.values())] = 0
    df1 = df1.append(other=new1, ignore_index=True, verify_integrity=True, sort=True)
    df1 = df1[['DESTINATION_FIPS'] + list(AGE_MAP.values())]
    df1.sort_values(by='DESTINATION_FIPS', inplace=True)

    new2 = df1[~df1.DESTINATION_FIPS.isin(df2.DESTINATION_FIPS)]
    new2.loc[:, list(AGE_MAP.values())] = 0
    df2 = df2.append(other=new2, ignore_index=True, verify_integrity=True, sort=True)
    df2 = df2[['DESTINATION_FIPS'] + list(AGE_MAP.values())]
    df2.sort_values(by='DESTINATION_FIPS', inplace=True)

    df1.set_index(keys='DESTINATION_FIPS', inplace=True)
    df2.set_index(keys='DESTINATION_FIPS', inplace=True)

    df = (df1 + df2) / 2

    df.reset_index(inplace=True)

    # expand age groups
    df = df.set_index(keys='DESTINATION_FIPS').T.reset_index()
    df.rename(columns={'index': 'AGE_GROUP'}, inplace=True)
    df['AGE_GROUP'] = df['AGE_GROUP'].apply(lambda x: parse_age_groups(x))
    exploded = df.apply(lambda x: pd.Series(x['AGE_GROUP']), axis=1).stack().reset_index(level=1, drop=True)
    exploded.name = 'AGE'
    df = df.drop(columns='AGE_GROUP').join(exploded)
    df['AGE'] = df.AGE.astype(int)

    df.set_index(keys='AGE', inplace=True)
    df = df.T
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'DESTINATION_FIPS'}, inplace=True)
    df.columns.name = None

    p = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases'
    f = 'acs.sqlite'
    con = sqlite3.connect(os.path.join(p, f))
    df.to_sql(name='acs_immigration_weights_age_2006_2015',
              con=con,
              if_exists='replace',
              index=False)
    con.close()


if __name__ == '__main__':
    main()
