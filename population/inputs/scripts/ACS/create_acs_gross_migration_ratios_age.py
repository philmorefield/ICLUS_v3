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
POPULATION_DB = os.path.join(DATABASE_FOLDER, 'population.sqlite')
ANALYSIS_DB = os.path.join(DATABASE_FOLDER, 'analysis.sqlite')
ACS_DB = os.path.join(DATABASE_FOLDER, 'acs.sqlite')

ACS_FOLDER = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\ACS\\2011_2015')
ACS_CSV_PATH = os.path.join(ACS_FOLDER, 'population')

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

POP_TO_MIGRATION_MAP = {'0_TO_4': '1_TO_4',
                        '5_TO_9': '5_TO_17',
                        '10_TO_14': '5_TO_17',
                        '15_TO_17': '5_TO_17',
                        '18_TO_19': '18_TO_19',
                        '20': '20_TO_24',
                        '21': '20_TO_24',
                        '22_TO_24': '20_TO_24',
                        '25_TO_29': '25_TO_29',
                        '30_TO_34': '30_TO_34',
                        '35_TO_39': '35_TO_39',
                        '40_TO_44': '40_TO_44',
                        '45_TO_49': '45_TO_49',
                        '50_TO_54': '50_TO_54',
                        '55_TO_59': '55_TO_59',
                        '60_TO_61': '60_TO_64',
                        '62_TO_64': '60_TO_64',
                        '65_TO_66': '65_TO_69',
                        '67_TO_69': '65_TO_69',
                        '70_TO_74': '70_TO_74',
                        '75_TO_79': '75_AND_OVER',
                        '80_TO_84': '75_AND_OVER',
                        '85_AND_OVER': '75_AND_OVER'}


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
        else:
            df = df.groupby(by='COFIPS', as_index=False).sum()

    assert not df.isna().any().any(), "NaN values present after FIPS changes"

    return df


def get_acs_2011_2015_population_by_age():
    csv_name = 'ACSDP5Y2015.DP05-Data.csv'
    csv = os.path.join(ACS_CSV_PATH, csv_name)
    usecols = ['GEOID'] + [f'DP05_00{str(i).zfill(2)}E' for i in range(4, 17)]
    df = pd.read_csv(filepath_or_buffer=csv,
                     usecols=usecols,
                     skiprows=[1],
                     encoding='latin-1')

    cols = ['ORIGIN_FIPS', '0_TO_4', '5_TO_9', '10_TO_14', '15_TO_19', '20_TO_24',
            '25_TO_34', '35_TO_44', '45_TO_54', '55_TO_59', '60_TO_64',
            '65_TO_74', '75_TO_84', '85_AND_OVER']
    df.columns = cols
    df['ORIGIN_FIPS'] = df['ORIGIN_FIPS'].str[-5:]
    df = df.melt(id_vars='ORIGIN_FIPS', var_name='POPULATION_AGE_GROUP', value_name='ORIGIN_POPULATION_P')

    df = make_fips_changes(df)

    assert not df.isna().any().any(), "NaN values present after cleaning"

    return df


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

    xls = pd.ExcelFile(os.path.join(ACS_FOLDER, 'migration', xl_filename))
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


def calculate_5_to_17_migration_rate(df):
    '''
    Purpose: calculate the 5_TO_17 migration rate by county
    '''
    df = df[['ORIGIN_FIPS', 'FLOW', 'ORIGIN_POPULATION']]
    df = df.rename(columns={'ORIGIN_POPULATION': 'ORIGIN_POPULATION_5_TO_17'})
    df = df.groupby(by='ORIGIN_FIPS', as_index=False).sum()
    df['MIGRATION_RATE_5_TO_17'] = df['FLOW'].div(df['ORIGIN_POPULATION_5_TO_17'])
    df = df.drop(columns=['FLOW', 'ORIGIN_POPULATION_5_TO_17'])

    return df


def calculate_flow_percentages(migration, population):
    '''
    Purpose: use the ACS 2011-2015 population-by-age data to disaggregate
    the migration-by-age data. Specifically: dissaggregate the 5_TO_17
    migration age group into 5_TO_14 and 15_TO_17.

    Steps:
    1. MERGE the ACS population dataframe with the ACS migration dataframe
    2. Disaggretate the MIGRATION_AGE_GROUP "5_TO_17" into "5_TO_14" and "15_TO_17"
    3. Rename the MIGRATION_AGE_GROUP "1_TO_4" to "0_TO_4"
    '''

    # calculate the 5_TO_17 migration rate by county
    migrate_5_to_17 = calculate_5_to_17_migration_rate(migration.query("AGE_GROUP == '5_TO_17'"))


    # 1. Calculate the 15_TO_17 population in each county
    subset_migration = migration.loc[migration.AGE_GROUP == '18_TO_19']
    subset_population = population.loc[population.POPULATION_AGE_GROUP == '15_TO_19']
    df_15_17 = subset_migration.merge(right=subset_population,
                                      on='ORIGIN_FIPS',
                                      how='left')
    df_15_17.eval('POP_15_TO_17 = ORIGIN_POPULATION_P - ORIGIN_POPULATION', inplace=True)

    # one Texas county shows negative population for 15_TO_17, set to zero
    assert len(df_15_17.loc[df_15_17.POP_15_TO_17 < 0]) == 1
    df_15_17['POP_15_TO_17'] = df_15_17['POP_15_TO_17'].clip(lower=0)

    df_15_17 = df_15_17.rename(columns={'POP_15_TO_17': '15_TO_17'})
    df_15_17 = df_15_17.drop(columns=['POPULATION_AGE_GROUP', 'ORIGIN_POPULATION_P'])

    # Additional steps needed:
    # - calculate the 5_TO_17 migration rate by county; use this rate for the
    #   newly created 15_TO_17 age group

    # 5. Rename the MIGRATION_AGE_GROUP "1_TO_4" to "0_TO_4"
    df.loc[df.MIGRATION_AGE_GROUP == '1_TO_4', 'MIGRATION_AGE_GROUP'] = '0_TO_4'

    # Do some clean up
    df = df.rename(columns={'MIGRATION_AGE_GROUP': 'AGE_GROUP'})
    df = df.groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'AGE_GROUP'], as_index=False).sum()
    assert df.FLOW.sum() == migration.FLOW.sum(), "Error in aggregation: Total flow mismatch"

    df['GROSS_MIGRATION_RATE'] = df['FLOW'].div(df['ORIGIN_POPULATION_ACS'])
    df = df.dropna(subset='GROSS_MIGRATION_RATE')

    assert not df.isna().any().any(), "NaN values present after calculating migration rates"

    return df


def get_gross_migration_ratios_by_age():
    migration = get_acs_2011_2015_migration()
    population = get_acs_2011_2015_population_by_age()

    df = calculate_flow_percentages(migration, population)

    con = sqlite3.connect(ACS_DB)
    df.to_sql(name='acs_gross_migration_ratios_2011_2015_age',
              con=con,
              if_exists='replace',
              index=False)
    con.close()

def main():
    get_gross_migration_ratios_by_age()

if __name__ == '__main__':
    main()
