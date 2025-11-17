import os
import sqlite3

import pandas as pd

pd.set_option("display.max_columns", None) # show all cols

BASE_FOLDER = 'D:\\projects\\ICLUS_v3\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'
if os.path.isdir('D:\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'

DATABASE_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\databases')
MIGRATION_DB = os.path.join(DATABASE_FOLDER, 'migration.sqlite')
ACS_DB = os.path.join(DATABASE_FOLDER, 'acs.sqlite')
ACS_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\ACS')
ACS_AGE_GROUP_MAP = {1: '1_TO_4',
                     2: '5_TO_17',
                     3: '18_TO_19',
                     4: '20_TO_24',
                     5: '25_TO_29',
                     6: '30_TO_34',
                     7: '35_TO_39',
                     8: '40_TO_44',
                     9: '45_TO_49',
                     10: '50_TO_54',
                     11: '55_TO_59',
                     12: '60_TO_64',
                     13: '65_TO_69',
                     14: '70_TO_74',
                     15: '75_AND_OVER'}


def make_fips_changes(df):
    con =sqlite3.connect(MIGRATION_DB)
    query = 'SELECT OLD_FIPS AS COFIPS, NEW_FIPS \
             FROM fips_or_name_changes'
    df_fips = pd.read_sql_query(sql=query, con=con)
    con.close()

    if 'ORIGIN_FIPS' in df.columns and 'DESTINATION_FIPS' in df.columns:
        df = df.merge(right=df_fips,
                    how='left',
                    left_on='ORIGIN_FIPS',
                    right_on='COFIPS')
        df.loc[~df.NEW_FIPS.isnull(), 'ORIGIN_FIPS'] = df['NEW_FIPS']
        df = df.drop(columns=['NEW_FIPS', 'COFIPS'])

        df = df.merge(right=df_fips,
                    how='left',
                    left_on='DESTINATION_FIPS',
                    right_on='COFIPS')
        df.loc[~df.NEW_FIPS.isnull(), 'DESTINATION_FIPS'] = df['NEW_FIPS']
        df = df.drop(columns=['NEW_FIPS', 'COFIPS'])

        if 'RACE' in df.columns:
            df = df.groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'RACE'], as_index=False).sum()
        elif 'MIGRATION_AGE_GROUP' in df.columns:
            df = df.groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'MIGRATION_AGE_GROUP'], as_index=False).sum()
        else:
            df = df.groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS'], as_index=False).sum()
    elif 'COFIPS' in df.columns:
        df = df.merge(right=df_fips,
                    how='left',
                    on='COFIPS')
        df.loc[~df.NEW_FIPS.isnull(), 'COFIPS'] = df['NEW_FIPS']
        df = df.drop(columns='NEW_FIPS')

        if 'RACE' in df.columns:
            df = df.groupby(by=['COFIPS', 'RACE'], as_index=False).sum()
        elif 'POPULATION_AGE_GROUP' in df.columns:
            df = df.groupby(by=['COFIPS', 'POPULATION_AGE_GROUP'], as_index=False).sum()
        elif 'AGE_GROUP' in df.columns:
            df = df.groupby(by=['COFIPS', 'AGE_GROUP'], as_index=False).sum()
        else:
            df = df.groupby(by='COFIPS', as_index=False).sum()

    assert not df.isna().any().any(), "NaN values present after FIPS changes"

    return df


# def get_acs_2011_2015_population_by_age():
#     csv_name = 'ACSDP5Y2015.DP05-Data.csv'
#     csv = os.path.join(ACS_FOLDER, '2011_2015//population', csv_name)
#     usecols = ['GEOID'] + [f'DP05_00{str(i).zfill(2)}E' for i in range(4, 17)]
#     df = pd.read_csv(filepath_or_buffer=csv,
#                      usecols=usecols,
#                      skiprows=[1],
#                      encoding='latin-1')

#     cols = ['COFIPS', '0_TO_4', '5_TO_9', '10_TO_14', '15_TO_19', '20_TO_24',
#             '25_TO_34', '35_TO_44', '45_TO_54', '55_TO_59', '60_TO_64',
#             '65_TO_74', '75_TO_84', '85_AND_OVER']
#     df.columns = cols
#     df['COFIPS'] = df['COFIPS'].str[-5:]
#     df = df.melt(id_vars='COFIPS', var_name='AGE_GROUP', value_name='POPULATION')

#     df = make_fips_changes(df)

#     assert not df.isna().any().any(), "NaN values present after cleaning"

#     return df


def get_acs_2011_2015_migration():
    xl_filename = 'county-to-county-by-age-2011-2015-current-residence-sort.xlsx'

    columns = ('D_STFIPS', 'D_COFIPS', 'O_STFIPS', 'O_COFIPS', 'MIGRATION_AGE_GROUP',
               'D_STATE', 'D_COUNTY', 'D_POP', 'D_POP_MOE', 'D_NONMOVERS',
               'D_NONMOVERS_MOE', 'D_MOVERS', 'D_MOVERS_MOE',
               'D_MOVERS_SAME_CY', 'D_MOVERS_SAME_CY_MOE',
               'D_MOVERS_FROM_DIFF_CY_SAME_ST',
               'D_MOVERS_FROM_DIFF_CY_SAME_ST_MOE', 'D_MOVERS_FROM_DIFF_ST',
               'D_MOVERS_DIFF_ST_MOE', 'D_MOVERS_FROM_ABROAD',
               'D_MOVERS_FROM_ABROAD_MOE', 'O_STATE', 'O_COUNTY',
               'ORIGIN_POPULATION_M', 'O_POP_MOE', 'O_NONMOVERS',
               'O_NOMMOVERS_MOE', 'O_MOVERS', 'O_MOVERS_MOE',
               'O_MOVERS_SAME_CY', 'O_MOVERS_SAME_CY_MOE',
               'O_MOVERS_FROM_DIFF_CY_SAME_ST',
               'O_MOVERS_FROM_DIFF_CY_SAME_ST_MOE', 'O_MOVERS_FROM_DIFF_ST',
               'O_MOVERS_DIFF_ST_MOE', 'O_MOVERS_PUERTO_RICO',
               'O_MOVERS_PUERTO_RICO_MOE', 'FLOW', 'TOTAL_FLOW_MOE')

    xls = pd.ExcelFile(os.path.join(ACS_FOLDER, '2011_2015', 'migration', xl_filename))
    df = pd.concat([xls.parse(sheet_name=name, header=None, names=columns, skiprows=4, skipfooter=8) for name in xls.sheet_names if name != 'Puerto Rico'])

    df = df[~df.O_STFIPS.str.contains('XXX')]
    foreign = ('EUR', 'ASI', 'SAM', 'ISL', 'NAM', 'CAM', 'CAR', 'AFR', 'OCE')
    df = df.loc[~df.O_STFIPS.isin(foreign), ['D_STFIPS', 'D_COFIPS', 'O_STFIPS', 'O_COFIPS', 'MIGRATION_AGE_GROUP', 'ORIGIN_POPULATION_M', 'FLOW']]

    df['D_STFIPS'] = df.D_STFIPS.astype(int).astype(str).str.zfill(2)
    df['D_COFIPS'] = df.D_COFIPS.astype(int).astype(str).str.zfill(3)
    df['DESTINATION_FIPS'] = df.D_STFIPS + df.D_COFIPS

    df['O_STFIPS'] = df.O_STFIPS.astype(int).astype(str).str.zfill(2)
    df['O_COFIPS'] = df.O_COFIPS.astype(int).astype(str).str.zfill(3)
    df['ORIGIN_FIPS'] = df.O_STFIPS + df.O_COFIPS

    df['MIGRATION_AGE_GROUP'] = df['MIGRATION_AGE_GROUP'].replace(to_replace=ACS_AGE_GROUP_MAP)
    df = df[['ORIGIN_FIPS', 'DESTINATION_FIPS', 'MIGRATION_AGE_GROUP', 'FLOW', 'ORIGIN_POPULATION_M']]

    # make FIPS changes and consolidate migration flows
    df_m = make_fips_changes(df[['ORIGIN_FIPS', 'DESTINATION_FIPS', 'MIGRATION_AGE_GROUP', 'FLOW']])

    # make FIPS changes and cosolidate origin population
    df_p = df[['ORIGIN_FIPS', 'MIGRATION_AGE_GROUP', 'ORIGIN_POPULATION_M']].drop_duplicates()
    df_p = df_p.rename(columns={'ORIGIN_FIPS': 'COFIPS',
                                'MIGRATION_AGE_GROUP': 'POPULATION_AGE_GROUP'})
    df_p = make_fips_changes(df_p)

    # join the origin population from the migration file back to the migration
    # dataframe
    df = df_m.merge(right=df_p,
                    how='left',
                    left_on=['ORIGIN_FIPS', 'MIGRATION_AGE_GROUP'],
                    right_on=['COFIPS', 'POPULATION_AGE_GROUP'])

    df = df.rename(columns={'ORIGIN_POPULATION_M': 'ORIGIN_POPULATION',
                            'MIGRATION_AGE_GROUP': 'AGE_GROUP'})
    df = df.drop(columns=['COFIPS', 'POPULATION_AGE_GROUP'])
    df = df.sort_values(by=['ORIGIN_FIPS', 'AGE_GROUP', 'DESTINATION_FIPS'])

    assert not df.isna().any().any(), "NaN values present after cleaning"

    return df


# def get_acs_2018_2022_migration():
#     xlsx_filename = 'state-to-county-migration-flows-acs-2018-2022.xlsx'

#     columns = ('D_STFIPS', 'D_COFIPS', 'O_STFIPS', 'D_STATE', 'D_COUNTY',
#                'O_STATE', 'FLOW', 'FLOW_MOE', 'D_POP', 'D_POP_MOE',
#                'D_NONMOVERS', 'D_NONMOVERS_MOE', 'D_MOVERS', 'D_MOVERS_MOE',
#                'D_MOVERS_SAME_CY', 'D_MOVERS_SAME_CY_MOE',
#                'D_MOVERS_FROM_DIFF_CY_SAME_ST',
#                'D_MOVERS_FROM_DIFF_CY_SAME_ST_MOE', 'D_MOVERS_FROM_DIFF_ST',
#                'D_MOVERS_DIFF_ST_MOE', 'D_MOVERS_FROM_ABROAD',
#                'D_MOVERS_FROM_ABROAD_MOE', 'ORIGIN_POPULATION', 'O_POP_MOE',
#                'O_NONMOVERS', 'O_NOMMOVERS_MOE', 'O_TOTAL_MOVERS',
#                'O_TOTAL_MOVERS_MOE', 'O_MOVERS_PUERTO_RICO',
#                'O_MOVERS_PUERTO_RICO_MOE')

#     xlsx = pd.ExcelFile(os.path.join(ACS_FOLDER, '2018_2022', 'migration', xlsx_filename))
#     df = pd.concat([xlsx.parse(sheet_name=name, header=None, names=columns, skiprows=4, skipfooter=10) for name in xlsx.sheet_names])
#     df = df.dropna(how='any')
#     df['DESTINATION_FIPS'] = df.D_STFIPS.astype(int).astype(str).str.zfill(2) + df.D_COFIPS.astype(int).astype(str).str.zfill(3)
#     df['ORIGIN_FIPS'] = df.O_STFIPS.astype(int).astype(str).str.zfill(2)
#     df['MIGRATION_RATE_2022'] = df.FLOW.div(df.ORIGIN_POPULATION)
#     df = df[['ORIGIN_FIPS', 'DESTINATION_FIPS', 'MIGRATION_RATE_2022']]
#     df = df.groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS'], as_index=False).mean()

#     return df


# def get_acs_2011_2015_state_migration(migration):
#     # Merge the 5-17 and 18-19 migration flows into a wide format and calculate
#     # the 15-19 migration rate as a weighted average of the two age groups
#     df1 = migration.loc[migration.AGE_GROUP.isin(['5_TO_17', '18_TO_19'])]
#     df1['MIGRATION_RATE'] = df1.loc[:, 'FLOW'].div(df1.loc[:, 'ORIGIN_POPULATION'])
#     df1 = df1[['ORIGIN_FIPS', 'DESTINATION_FIPS', 'AGE_GROUP', 'MIGRATION_RATE']]
#     df1 = df1.pivot_table(index=['ORIGIN_FIPS', 'DESTINATION_FIPS'],
#                             columns='AGE_GROUP',
#                             values='MIGRATION_RATE',
#                             fill_value=0).reset_index()
#     df1['15_TO_19'] = ((df1['18_TO_19'] * 2) + (df1['5_TO_17'] * 3)) / 5
#     df1 = (df1[['ORIGIN_FIPS', 'DESTINATION_FIPS', '15_TO_19']]
#            .melt(id_vars=['ORIGIN_FIPS', 'DESTINATION_FIPS'],
#                  var_name='AGE_GROUP',
#                  value_name='MIGRATION_RATE_2015'))

#     # Concatenate the 15-19 migration rates back to the migration dataframe
#     df = migration.loc[migration.AGE_GROUP != '18_TO_19']
#     df['MIGRATION_RATE_2015'] = df['FLOW'].div(df['ORIGIN_POPULATION'])
#     df = df[['ORIGIN_FIPS', 'DESTINATION_FIPS', 'AGE_GROUP', 'MIGRATION_RATE_2015']]
#     df['AGE_GROUP'] = df['AGE_GROUP'].replace({'5_TO_17': '5_TO_9'})
#     temp = df.loc[df.AGE_GROUP == '5_TO_9'].copy()
#     temp['AGE_GROUP'] = temp['AGE_GROUP'].replace({'5_TO_9': '10_TO_14'})
#     df = pd.concat([df, temp, df1], ignore_index=True)

#     df = df.drop(columns='AGE_GROUP')
#     df['ORIGIN_FIPS'] = df['ORIGIN_FIPS'].str[:2]
#     df = df.groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS'], as_index=False).mean()

#     return df


def calculate_flow_percentages(migration):
    '''
    Purpose: use the ACS 2011-2015 population-by-age data to disaggregate
    the migration-by-age data. Specifically: dissaggregate the 5_TO_17
    migration age group into 5_TO_14 and 15_TO_17.
    '''

    # Merge the 5-17 and 18-19 migration flows into a wide format and calculate
    # the 15-19 migration rate as a weighted average of the two age groups
    df1 = migration.loc[migration.AGE_GROUP.isin(['5_TO_17', '18_TO_19'])]
    df1['MIGRATION_RATE'] = df1.loc[:, 'FLOW'].div(df1.loc[:, 'ORIGIN_POPULATION'])
    df1 = df1[['ORIGIN_FIPS', 'DESTINATION_FIPS', 'AGE_GROUP', 'MIGRATION_RATE']]
    df1 = df1.pivot_table(index=['ORIGIN_FIPS', 'DESTINATION_FIPS'],
                            columns='AGE_GROUP',
                            values='MIGRATION_RATE',
                            fill_value=0).reset_index()
    df1['15_TO_19'] = ((df1['18_TO_19'] * 2) + (df1['5_TO_17'] * 3)) / 5
    df1 = (df1[['ORIGIN_FIPS', 'DESTINATION_FIPS', '15_TO_19']]
           .melt(id_vars=['ORIGIN_FIPS', 'DESTINATION_FIPS'],
                 var_name='AGE_GROUP',
                 value_name='MIGRATION_RATE'))

    # Concatenate the 15-19 migration rates back to the migration dataframe
    df = migration.loc[migration.AGE_GROUP != '18_TO_19']
    df['MIGRATION_RATE'] = df['FLOW'].div(df['ORIGIN_POPULATION'])
    df = df[['ORIGIN_FIPS', 'DESTINATION_FIPS', 'AGE_GROUP', 'MIGRATION_RATE']]
    df['AGE_GROUP'] = df['AGE_GROUP'].replace({'5_TO_17': '5_TO_9'})
    temp = df.loc[df.AGE_GROUP == '5_TO_9'].copy()
    temp['AGE_GROUP'] = temp['AGE_GROUP'].replace({'5_TO_9': '10_TO_14'})
    df = pd.concat([df, temp, df1], ignore_index=True)

    # disaggregate the 75+ migration rate to 75-79, 80-84, and 85+
    df_75_over = df.loc[df.AGE_GROUP == '75_AND_OVER'].copy()
    df_75_79 = df_75_over.copy()
    df_75_79['AGE_GROUP'] = '75_TO_79'

    df_80_84 = df_75_over.copy()
    df_80_84['AGE_GROUP'] = '80_TO_84'

    df_85_over = df_75_over.copy()
    df_85_over['AGE_GROUP'] = '85_AND_OVER'

    df = df.loc[df.AGE_GROUP != '75_AND_OVER']
    df = pd.concat([df, df_75_79, df_80_84, df_85_over], ignore_index=True)

    # Rename the AGE_GROUP "1_TO_4" to "0_TO_4"
    df['AGE_GROUP'] = df.AGE_GROUP.replace({'1_TO_4': '0_TO_4'})

    # state_migration = get_acs_2011_2015_state_migration(migration)
    # new_migration = get_acs_2018_2022_migration()

    # migration_adustment = state_migration.merge(right=new_migration,
    #                                             how='inner',
    #                                             on=['ORIGIN_FIPS', 'DESTINATION_FIPS'])
    # migration_adustment['ADJUSTMENT_FACTOR'] = migration_adustment['MIGRATION_RATE_2022'] / migration_adustment['MIGRATION_RATE_2015']

    assert not df.isna().any().any(), "NaN values present after calculating migration rates"

    return df


def get_gross_migration_ratios_by_age():
    migration = get_acs_2011_2015_migration()

    df = calculate_flow_percentages(migration)

    con = sqlite3.connect(ACS_DB)
    df.to_sql(name='acs_gross_migration_ratios_2011_2015_age',
              con=con,
              if_exists='replace',
              index=False)
    con.close()

    df.to_csv(os.path.join(DATABASE_FOLDER, 'acs_gross_migration_ratios_2011_2015_age.csv'),
              index=False)

def main():
    get_gross_migration_ratios_by_age()

if __name__ == '__main__':
    main()
