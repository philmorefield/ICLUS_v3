import os
import sqlite3

from itertools import product

from numpy import float64
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
CENSUS_CSV_PATH = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\Census\\2020\\decennial\\population_by_age')

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

CENSUS_AGE_GROUPS = ['0_TO_4', '5_TO_9', '10_TO_14', '15_TO_17', '18_TO_19',
                     '20', '21', '22_TO_24', '25_TO_29',
                     '30_TO_34', '35_TO_39', '40_TO_44', '45_TO_49',
                     '50_TO_54', '55_TO_59', '60_TO_61', '62_TO_64',
                     '65_TO_66', '67_TO_69', '70_TO_74', '75_TO_79',
                     '80_TO_84', '85_AND_OVER']

CENSUS_AGE_GROUP_MAP = {'15_TO_17': '15_TO_19',
                        '18_TO_19': '15_TO_19',
                        '20': '20_TO_24',
                        '21': '20_TO_24',
                        '22_TO_24': '20_TO_24',
                        '60_TO_61': '60_TO_64',
                        '62_TO_64': '60_TO_64',
                        '65_TO_66': '65_TO_69',
                        '67_TO_69': '65_TO_69'}

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
        elif 'ORIGIN_AGE_GROUP_CENSUS' in df.columns:
            df = df.groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'ORIGIN_AGE_GROUP_CENSUS'], as_index=False).sum()
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
        elif 'ORIGIN_AGE_GROUP_CENSUS' in df.columns:
            df = df.groupby(by=['COFIPS', 'ORIGIN_AGE_GROUP_CENSUS'], as_index=False).sum()
        else:
            df = df.groupby(by='COFIPS', as_index=False).sum()

    return df


def get_euclidean_distance():
    query = 'SELECT ORIGIN_FIPS, DESTINATION_FIPS, Dij \
             FROM county_to_county_distance_2020'
    con = sqlite3.connect(ANALYSIS_DB)
    df = pd.read_sql_query(sql=query, con=con)
    con.close()

    assert not df.isnull().any().any()

    return df

def get_census_2020_county_population_by_age_():
    csv_name = 'DECENNIALDHC2020.P12-Data.csv'
    csv = os.path.join(CENSUS_CSV_PATH, csv_name)
    df = pd.read_csv(filepath_or_buffer=csv,
                     skiprows=1,
                     encoding='latin-1')

    other_columns = [f'{gender}_{age_group}' for gender, age_group in list(product(['MALE', 'FEMALE'], CENSUS_AGE_GROUPS))]
    df.columns = ['COFIPS'] + other_columns
    df['COFIPS'] = df['COFIPS'].str[-5:]

    df = make_fips_changes(df)

    df = df.melt(id_vars='COFIPS', var_name='ORIGIN_AGE_GROUP', value_name='ORIGIN_POPULATION')
    df['SEX'] = df['ORIGIN_AGE_GROUP'].str.split(pat='_', n=1).str[0]
    df['ORIGIN_AGE_GROUP'] = df['ORIGIN_AGE_GROUP'].str.split(pat='_', n=1).str[1]
    df = df.rename(columns={'ORIGIN_AGE_GROUP': 'ORIGIN_AGE_GROUP_CENSUS', \
                            'ORIGIN_POPULATION': 'ORIGIN_POPULATION_CENSUS'})

    return df


def get_acs_2011_2015_population_by_age():
    csv_name = 'ACSDP5Y2015.DP05-Data.csv'
    csv = os.path.join(ACS_CSV_PATH, csv_name)
    df = pd.read_csv(filepath_or_buffer=csv,
                     skiprows=[1],
                     encoding='latin-1')

    cols = ['GEOID'] + [f'DP05_00{str(i).zfill(2)}E' for i in range(4, 17)]
    df = df[cols]

    df = df.melt(id_vars='COFIPS', var_name='ORIGIN_AGE_GROUP', value_name='ORIGIN_POPULATION')
    df['SEX'] = df['ORIGIN_AGE_GROUP'].str.split(pat='_', n=1).str[0]
    df['ORIGIN_AGE_GROUP'] = df['ORIGIN_AGE_GROUP'].str.split(pat='_', n=1).str[1]

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
               'ORIGIN_POPULATION_ACS', 'O_POP_MOE', 'O_NONMOVERS',
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
    df = df.loc[~df.O_STFIPS.isin(foreign), ['D_STFIPS', 'D_COFIPS', 'O_STFIPS', 'O_COFIPS', 'MIGRATION_AGE_GROUP', 'ORIGIN_POPULATION_ACS', 'FLOW']]

    df['D_STFIPS'] = df.D_STFIPS.astype(int).astype(str).str.zfill(2)
    df['D_COFIPS'] = df.D_COFIPS.astype(int).astype(str).str.zfill(3)
    df['DESTINATION_FIPS'] = df.D_STFIPS + df.D_COFIPS

    df['O_STFIPS'] = df.O_STFIPS.astype(int).astype(str).str.zfill(2)
    df['O_COFIPS'] = df.O_COFIPS.astype(int).astype(str).str.zfill(3)
    df['ORIGIN_FIPS'] = df.O_STFIPS + df.O_COFIPS

    df['MIGRATION_AGE_GROUP'] = df['MIGRATION_AGE_GROUP'].replace(to_replace=ACS_AGE_GROUP_MAP)
    df = df[['ORIGIN_FIPS', 'DESTINATION_FIPS', 'MIGRATION_AGE_GROUP', 'FLOW']]
    df = make_fips_changes(df)

    df = df.sort_values(by=['ORIGIN_FIPS', 'MIGRATION_AGE_GROUP', 'DESTINATION_FIPS'])

    return df

def disaggregate_5_to_17_age_group(df_orig):
    '''
    Disaggregate migration for 5_TO_17 age group into 5_TO_9, 10_TO_14, and
    15_TO_17. I use the population of each age group in origin counties to
    disaggregate the migration flows.
    '''
    df = df_orig.copy()

    # Select only the 5_TO_17 age group
    df = df.query('MIGRATION_AGE_GROUP == "5_TO_17"')

    # Calculate total population in the 5_TO_17 age group by origin county
    df['TOTAL_POPULATION_5_17'] = (df.groupby(by='COFIPS')['ORIGIN_POPULATION_CENSUS'].transform('sum'))

    # Calculate the fraction of the population in each age group within the 5_TO_17 category
    df['FRACTION_5_17'] = df['ORIGIN_POPULATION_CENSUS'].div(df['TOTAL_POPULATION_5_17'])

    # Calculate the within-flow weights for each origin-destination dyad
    df['SUM_FRACTION_5_17'] = df.groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS'])['FRACTION_5_17'].transform('sum')
    df['MIGRATION_FLOW_FRACTION'] = df['FRACTION_5_17'].div(df['SUM_FRACTION_5_17'])

    # Assign migration flows for each disaggregated age group based on the calculated fractions
    df['FLOW_DISAGGREGATED'] = df['MIGRATION_FLOW_FRACTION'].mul(df['FLOW'])
    assert round(df['FLOW_DISAGGREGATED'].sum()) == round(df['FLOW'].sum() / 3.0), "Disaggregation error: Total flow mismatch"

    df.loc[:, 'MIGRATION_AGE_GROUP'] = df.loc[:, 'ORIGIN_AGE_GROUP_CENSUS']
    df['FLOW'] = df['FLOW'].astype(float)
    df.loc[:, 'FLOW'] = df.loc[:, 'FLOW_DISAGGREGATED']

    df = df.drop(columns=['FRACTION_5_17',
                          'TOTAL_POPULATION_5_17',
                          'FRACTION_5_17',
                          'SUM_FRACTION_5_17',
                          'MIGRATION_FLOW_FRACTION',
                          'FLOW_DISAGGREGATED'])

    df_orig = df_orig.loc[~df_orig.MIGRATION_AGE_GROUP.isin(['5_TO_17'])]
    df = pd.concat([df_orig, df], ignore_index=True)

    return df

def disaggregate_75_AND_OVER_age_group(df_orig):
    '''
    Disaggregate the 5_TO_17 age group into 5_TO_9, 10_TO_14, and 15_TO_17.
    '''
    df = df_orig.copy()

    # Select only the 75_AND_OVER age group
    df = df.query('MIGRATION_AGE_GROUP == "75_AND_OVER"')

    # Calculate the total population in the 75_AND_OVER age group by origin county
    df['TOTAL_POPULATION_75_OVER'] = (df.groupby(by='COFIPS')['ORIGIN_POPULATION_CENSUS'].transform('sum'))

    # Calculate the fraction of the population in each age group within the 75_AND_OVER category
    df['FRACTION_75_OVER'] = df['ORIGIN_POPULATION_CENSUS'].div(df['TOTAL_POPULATION_75_OVER']).mul(df['FLOW'])

    # Calculate the within-flow weights for each origin-destination dyad
    df['SUM_FRACTION_75_OVER'] = df.groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS'])['FRACTION_75_OVER'].transform('sum')
    df['MIGRATION_FLOW_FRACTION'] = df['FRACTION_75_OVER'].div(df['SUM_FRACTION_75_OVER'])

    # Assign migration flows for each disaggregated age group based on the calculated fractions
    df['FLOW_DISAGGREGATED'] = df['MIGRATION_FLOW_FRACTION'].mul(df['FLOW'])
    assert round(df['FLOW_DISAGGREGATED'].sum()) == round(df['FLOW'].sum() / 3.0), "Disaggregation error: Total flow mismatch"

    df.loc[:, 'MIGRATION_AGE_GROUP'] = df.loc[:, 'ORIGIN_AGE_GROUP_CENSUS']
    df['FLOW'] = df['FLOW'].astype(float)
    df.loc[:, 'FLOW'] = df.loc[:, 'FLOW_DISAGGREGATED']
    df = df.drop(columns=['FRACTION_75_OVER',
                          'TOTAL_POPULATION_75_OVER',
                          'FRACTION_75_OVER',
                          'SUM_FRACTION_75_OVER',
                          'MIGRATION_FLOW_FRACTION',
                          'FLOW_DISAGGREGATED'])

    df_orig = df_orig.loc[~df_orig.MIGRATION_AGE_GROUP.isin(['75_AND_OVER'])]
    df = pd.concat([df_orig, df], ignore_index=True)

    return df

def calculate_flow_percentages(migration, census_population):
    '''
    Purpose: use the 2020 Census data (five-year age groups) to disaggregate
    the 5_TO_17 and 75_AND_OVER age groups in the ACS 2011-2015
    county_to_county migratione. Ideally I would use the ACS population
    estimates to do that, but the Census has ideal age groups for this purpose,
    e.g., 15-17.

    1. MAP the MIGRATION_AGE_GROUP to the ORIGIN_AGE_GROUP_CENSUS
    2. Combine ORIGIN_AGE_GROUP_CENSUS "20", "21", and "22_TO_24" into "20_TO_24"
    3. Combine ORIGIN_AGE_GROUP_CENSUS "60_TO_61" and "62_TO_64" into "60_TO_64"
    4. Combine ORIGIN_AGE_GROUP_CENSUS "65_TO_66" and "67_TO_69" into "65_TO_69"
    5. GROUPBY the ORIGIN_AGE_GROUP_CENSUS and update the ORIGIN_POPULATION
    6. MERGE the migration dataframe with the origin dataframe
    7. Disaggretate the MIGRATION_AGE_GROUP "5_TO_17" into "5_TO_9", "10_TO_14", and "15_TO_17"
    8. Disaggregate the MIGRATION_AGE_GROUP "75_AND_OVER" into "75_TO_79", "80_TO_84", and "85_AND_OVER"
    9. Combine ORIGIN_AGE_GROUP_CENSUS "15_TO_17" and "18_TO_19" into "15_TO_19"
    10. Rename the MIGRATION_AGE_GROUP "1_TO_4" to "0_TO_4"
    11. Determine fraction of FLOW by ORIGIN_AGE_GROUP_CENSUS and decompose aggregate MIGRATION_AGE_GROUPs

    '''
    pop_to_migration_map = {'0_TO_4': '1_TO_4',
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

    # 1. MAP the MIGRATION_AGE_GROUP to the ORIGIN_AGE_GROUP_CENSUS
    census_population['MIGRATION_AGE_GROUP'] = census_population.ORIGIN_AGE_GROUP_CENSUS.map(pop_to_migration_map)

    # 2. Combine ORIGIN_AGE_GROUP_CENSUS "20", "21", and "22_TO_24" into "20_TO_24"
    census_population.loc[census_population.ORIGIN_AGE_GROUP_CENSUS.isin(['20', '21', '22_TO_24']), 'ORIGIN_AGE_GROUP_CENSUS'] = '20_TO_24'

    # 3. Combine ORIGIN_AGE_GROUP_CENSUS "60_TO_61" and "62_TO_64" into "60_TO_64"
    census_population.loc[census_population.ORIGIN_AGE_GROUP_CENSUS.isin(['60_TO_61', '62_TO_64']), 'ORIGIN_AGE_GROUP_CENSUS'] = '60_TO_64'

    # 4. Combine ORIGIN_AGE_GROUP_CENSUS "65_TO_66" and "67_TO_69" into "65_TO_69"
    census_population.loc[census_population.ORIGIN_AGE_GROUP_CENSUS.isin(['65_TO_66', '67_TO_69']), 'ORIGIN_AGE_GROUP_CENSUS'] = '65_TO_69'

    # 5. GROUPBY the ORIGIN_AGE_GROUP_CENSUS and update the ORIGIN_POPULATION
    census_population = census_population.drop(columns='SEX')
    census_population = census_population.groupby(by=['COFIPS', 'ORIGIN_AGE_GROUP_CENSUS', 'MIGRATION_AGE_GROUP'], as_index=False).sum()

    # 6. MERGE
    df = migration.merge(right=census_population,
                         left_on=['ORIGIN_FIPS', 'MIGRATION_AGE_GROUP'],
                         right_on=['COFIPS', 'MIGRATION_AGE_GROUP'],
                         how='left')

    # 7. Disaggretate the MIGRATION_AGE_GROUP "5_TO_17" into "5_TO_9", "10_TO_14", and "15_TO_17"
    df = disaggregate_5_to_17_age_group(df)

    # 8. Disaggregate the MIGRATION_AGE_GROUP "75_AND_OVER" into "75_TO_79", "80_TO_84", and "85_AND_OVER"
    df = disaggregate_75_AND_OVER_age_group(df)

    # 9. Combine ORIGIN_AGE_GROUP_CENSUS "15_TO_17" and "18_TO_19" into "15_TO_19"
    df.loc[df.ORIGIN_AGE_GROUP_CENSUS.isin(['15_TO_17', '18_TO_19']), 'ORIGIN_AGE_GROUP_CENSUS'] = '15_TO_19'
    df.loc[df.MIGRATION_AGE_GROUP.isin(['15_TO_17', '18_TO_19']), 'MIGRATION_AGE_GROUP'] = '15_TO_19'

    # 10. Rename the MIGRATION_AGE_GROUP "1_TO_4" to "0_TO_4"
    df.loc[df.MIGRATION_AGE_GROUP == '1_TO_4', 'MIGRATION_AGE_GROUP'] = '0_TO_4'

    # 11. Determine fraction of FLOW by ORIGIN_AGE_GROUP_CENSUS and decompose aggregate MIGRATION_AGE_GROUPs
    df = df.drop(columns=['COFIPS', 'ORIGIN_AGE_GROUP_CENSUS', 'ORIGIN_POPULATION_CENSUS'])
    df = df.rename(columns={'MIGRATION_AGE_GROUP': 'AGE_GROUP'})
    df = df.groupby(by=['ORIGIN_FIPS', 'DESTINATION_FIPS', 'AGE_GROUP'], as_index=False).sum()
    assert df.FLOW.sum() == migration.FLOW.sum(), "Error in aggregation: Total flow mismatch"

    # 12. MERGE the ACS 2011-2015 population estimates to calculate cohort migration rates
    acs_population = get_acs_2011_2015_population_by_age()


    return df


def get_gross_migration_ratios_by_age():
    census_population = get_census_2020_county_population_by_age_()
    migration = get_acs_2011_2015_migration()

    df = calculate_flow_percentages(migration, census_population)

    con = sqlite3.connect(ACS_DB)
    df.to_sql(name='acs_gross_migration_weights_2011_2015_age',
              con=con,
              if_exists='replace',
              index=False)
    con.close()

def main():
    get_gross_migration_ratios_by_age()

if __name__ == '__main__':
    main()
