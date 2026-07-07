
"""
Author:  Phil Morefield (pmorefie@gmu.edu)
Purpose:
Created:
"""
import os
import sqlite3

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 10)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

DISSERTATION_FOLDER = 'E:\\Dissertation'
PART_3_INPUT_FOLDER = 'E:\\Dissertation\\analysis\\part_3\\inputs'
PART_3_A_INPUT_FOLDER = 'E:\\Dissertation\\analysis\\part_3_a\\inputs'
MIGRATION_DB = 'E:\\Dissertation\\databases\\migration.sqlite'
conn = sqlite3.connect(MIGRATION_DB)
cy2st = pd.read_sql_query(sql='SELECT GEOID, STABBR, STNAME FROM fips_to_state', con=conn)
FIPS_CHANGES = pd.read_sql_query(sql='SELECT "OLD_FIPS", "NEW_FIPS" from fips_or_name_changes', con=conn)
conn.close()


def update_cofips(df_orig):
    df = df_orig.merge(right=FIPS_CHANGES, how='left', left_on='GEOID', right_on='OLD_FIPS')
    df.loc[~df.NEW_FIPS.isnull(), 'GEOID'] = df.NEW_FIPS
    df.drop(columns=['OLD_FIPS', 'NEW_FIPS'], inplace=True)

    assert ~df.isnull().any().any()

    return df


def get_state_census_estimate_2020():
    db = os.path.join(PART_3_A_INPUT_FOLDER, 'part_3_a_inputs.sqlite')
    con = sqlite3.connect(db)

    query = 'SELECT STATE_NAME, "2020" as "CENSUS_2020" \
             FROM Census_state_estimates_2010_2020'
    df = pd.read_sql(sql=query, con=con)
    con.close()

    return df


def get_st(df_orig):
    df = df_orig.merge(right=cy2st, how='left', on='GEOID')
    df.drop(columns='GEOID', inplace=True)

    if 'SCENARIO' in df.columns:
        df = df.groupby(by=['STABBR', 'STNAME', 'SCENARIO']).sum()
    else:
        df = df.groupby(by=['STABBR', 'STNAME']).sum()

    assert ~df.isnull().any().any()

    return df


def get_census_1997():
    db = os.path.join(PART_3_INPUT_FOLDER, 'census', 'census.sqlite')
    con = sqlite3.connect(db)

    df_A = pd.read_sql(sql='SELECT STNAME, "2015", "2025" \
                            FROM Census_1997_projection_Series_A', con=con)
    df_B = pd.read_sql(sql='SELECT STNAME, "2015", "2025" \
                            FROM Census_1997_projection_Series_B', con=con)
    con.close()

    df_A['Series A'] = ((df_A["2015"] + df_A["2025"]) / 2).round().astype(int)
    df_B['Series B'] = ((df_B["2015"] + df_B["2025"]) / 2).round().astype(int)

    df = df_A.merge(right=df_B, how='left', on='STNAME')
    df = df[['STNAME', 'Series A', 'Series B']]
    df = df.melt(id_vars='STNAME', var_name='SCENARIO', value_name='PROJECTED')

    assert ~df.isnull().any().any()

    return df


def get_census_2005():
    db = os.path.join(PART_3_INPUT_FOLDER, 'census', 'census.sqlite')
    con = sqlite3.connect(db)

    query = 'SELECT STATE AS STNAME, Projected_Population AS PROJECTED \
             FROM Census_2005_projection_demographics \
             WHERE Year == "2020"'

    df = pd.read_sql(sql=query, con=con)
    con.close()

    df = df.groupby(by='STNAME', as_index=False).sum()

    assert ~df.isnull().any().any()

    return df


def get_iclus_v2():
    db = os.path.join(PART_3_INPUT_FOLDER, 'epa', 'epa.sqlite')
    con = sqlite3.connect(db)

    df = pd.read_sql(sql='SELECT STNAME, SCENARIO, "2020" AS PROJECTED \
                          FROM ICLUS_v2', con=con)

    return df


def get_iclus_v1():
    db = os.path.join(PART_3_INPUT_FOLDER, 'epa', 'epa.sqlite')
    con = sqlite3.connect(db)

    df = pd.read_sql(sql='SELECT FIPS AS GEOID, SCENARIO, "2020" AS PROJECTED \
                          FROM ICLUS_v1', con=con)

    df = df.merge(right=cy2st[['GEOID', 'STNAME']], how='left', on='GEOID')
    assert ~df.isnull().any().any()
    df = df[['STNAME', 'SCENARIO', 'PROJECTED']].groupby(by=['STNAME', 'SCENARIO'], as_index=False).sum()

    return df


def get_jiang():
    db = os.path.join(PART_3_INPUT_FOLDER, 'jiang', 'jiang.sqlite')
    con = sqlite3.connect(db)

    df_list = []

    for scenario in ('SSP2', 'SSP3', 'SSP5'):
        df = pd.read_sql(sql=f'SELECT STATE AS STABBR, "2020" AS PROJECTED \
                               FROM state_pop_projections_{scenario}', con=con)
        df = df.groupby(by='STABBR', as_index=False).sum()
        df['SCENARIO'] = scenario

        df = df.merge(right=cy2st[['STABBR', 'STNAME']], how='left', on='STABBR')
        assert ~df.isnull().any().any()
        df = df[['STNAME', 'SCENARIO', 'PROJECTED']]

        df_list.append(df)

    con.close()
    df = pd.concat(objs=df_list, ignore_index=True)

    return df


def get_usfs():
    db = os.path.join(PART_3_INPUT_FOLDER, 'usfs', 'usfs.sqlite')
    con = sqlite3.connect(db)

    df_list = []
    for i in range(1, 6):
        temp = pd.read_sql(sql=f'SELECT COFIPS, "2020" AS PROJECTED FROM "SSP{i} POP"', con=con)
        temp['SSP'] = f'SSP{i}'
        df_list.append(temp)
    df = pd.concat(objs=df_list, ignore_index=True)
    df['PROJECTED'] *= 1000
    df.rename(columns={'COFIPS': 'GEOID', 'SSP': 'SCENARIO'}, inplace=True)

    df = update_cofips(df)
    df = get_st(df)

    df.reset_index(inplace=True)
    df = df[['STNAME', 'SCENARIO', 'PROJECTED']]

    return df


def get_urban_institute():
    columns = '"FERTILITY", "MORTALITY", "MIGRATION", "ST", "POP"'

    query = f'SELECT {columns} \
              FROM states \
              WHERE RACE_ETH == "All" \
              AND AGEGRP == 0 \
              AND YEAR == 2020'

    db = os.path.join(PART_3_INPUT_FOLDER, 'urban_institute', 'urban_institute.sqlite')
    con = sqlite3.connect(db)

    df = pd.read_sql_query(sql=query, con=con)
    con.close()

    df['SCENARIO'] = df['FERTILITY'].str[0] + df['MORTALITY'].str[0] + df['MIGRATION'].str[0]
    df = df.merge(right=cy2st[['STABBR', 'STNAME']].drop_duplicates(), how='left', left_on='ST', right_on='STABBR')
    df = df[['STNAME', 'POP', 'SCENARIO']]
    df = df.groupby(by=['STNAME', 'SCENARIO'], as_index=False).sum()
    df['STNAME'] = df.STNAME.str.capitalize()
    df.rename(columns={'POP': 'PROJECTED'}, inplace=True)

    assert ~df.isnull().any().any()

    return df


def get_gao():
    db = os.path.join(PART_3_INPUT_FOLDER, 'ncar', 'ncar.sqlite')
    con = sqlite3.connect(db)
    df = pd.concat(objs=[pd.read_sql_query(sql=f'SELECT GEOID, SSP AS SCENARIO, "2020" AS "PROJECTED" FROM ssp{ssp}_total', con=con) for ssp in range(1, 6)])
    con.close()

    df = update_cofips(df)
    df = get_st(df)
    df.reset_index(inplace=True)

    assert ~df.isnull().any().any()

    return df


def get_ornl():
    '''
    The ORNL projections only include 2030 and 2050. Use the 2010 Census values
    to interpolate a 2020 value.
    '''
    db = 'E:\\Dissertation\\databases\\population.sqlite'
    con = sqlite3.connect(db)
    query = 'SELECT GEOID, POPULATION AS POP2010 \
             FROM county_population_genderraceage_Census2010'
    pop2010 = pd.read_sql(sql=query, con=con)
    con.close()
    pop2010['GEOID'] = pop2010['GEOID'].astype(str).str.zfill(5)
    pop2010 = pop2010.groupby(by='GEOID', as_index=False).sum()
    pop2010 = update_cofips(pop2010)
    pop2010 = get_st(pop2010)
    pop2010.reset_index(inplace=True)
    pop2010 = pop2010[['STNAME', 'POP2010']]

    db = os.path.join(PART_3_INPUT_FOLDER, 'ornl', 'ornl.sqlite')
    con = sqlite3.connect(db)
    df = pd.concat(objs=[pd.read_sql_query(sql=f'SELECT * FROM landcast_{year}', con=con, index_col='GEOID') for year in (2030, 2050)], axis=1, copy=False)
    con.close()
    df = df.reset_index()

    df = update_cofips(df)
    df = get_st(df)
    df.reset_index(inplace=True)
    df = df[['STNAME', '2030']]

    assert ~df.isnull().any().any()

    df = df.merge(right=pop2010, how='left', on='STNAME')
    df['PROJECTED'] = ((df['POP2010'] + df['2030']) / 2).round()
    df = df[['STNAME', 'PROJECTED']]

    return df


def get_uva():
    query = 'SELECT STNAME, "2020" AS "PROJECTED" \
             FROM states_2018 \
             WHERE SEX == "BOTH" \
             AND AGE_GROUP == "ALL"'

    db = os.path.join(PART_3_INPUT_FOLDER, 'uva', 'uva.sqlite')
    con = sqlite3.connect(db)
    df = pd.read_sql_query(sql=query, con=con)
    con.close()

    assert ~df.isnull().any().any()

    return df


def get_hauer():
    query = 'SELECT GEOID, SSP1, SSP2, SSP3, SSP4, SSP5 \
             FROM hauer_ssp \
             WHERE YEAR == 2020'

    db = os.path.join(PART_3_INPUT_FOLDER, 'hauer', 'hauer.sqlite')
    con = sqlite3.connect(db)
    df = pd.read_sql_query(sql=query, con=con)
    con.close()

    df = df.merge(right=cy2st[['GEOID', 'STNAME']], how='left', on='GEOID')
    assert ~df.isnull().any().any()
    df = df[['STNAME', 'SSP1', 'SSP2', 'SSP3', 'SSP4', 'SSP5']]
    df = df.groupby(by='STNAME', as_index=False).sum()

    df = df.melt(id_vars='STNAME', var_name='SCENARIO', value_name='PROJECTED')

    return df


def get_woodspoole():
    query = 'SELECT GEOID, POP2020 AS PROJECTED \
             FROM woodspoole_2020_2050'

    db = os.path.join(PART_3_INPUT_FOLDER, 'woodspoole', 'woodspoole.sqlite')
    con = sqlite3.connect(db)
    df = pd.read_sql_query(sql=query, con=con)
    con.close()

    df = df.merge(right=cy2st[['GEOID', 'STNAME']], how='left', on='GEOID')
    assert ~df.isnull().any().any()
    df = df[['STNAME', 'PROJECTED']]
    df = df.groupby(by='STNAME', as_index=False).sum()

    return df


def get_W2_2_b_iii():
    p = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_3', 'outputs')
    db = os.path.join(p, 'wittgenstein_v2_2_c_ii_2_b_iii.sqlite')
    con = sqlite3.connect(db)

    df = None
    for ssp in ('SSP1', 'SSP2', 'SSP3'):
        query = f'SELECT GEOID, "2020" as {ssp} \
                  FROM population_by_race_gender_age_{ssp}'
        result = pd.read_sql(sql=query, con=con)
        result = result.groupby(by='GEOID').sum()
        if df is not None:
            df = pd.concat(objs=[df, result], axis=1)
        else:
            df = result.copy()

    con.close()

    df = df.reset_index().melt(id_vars='GEOID', var_name='SCENARIO', value_name='PROJECTED')
    df = df.merge(right=cy2st[['GEOID', 'STNAME']], how='left', on='GEOID')
    assert ~df.isnull().any().any()
    df = df[['STNAME', 'SCENARIO', 'PROJECTED']]
    df = df.groupby(by=['STNAME', 'SCENARIO'], as_index=False).sum()

    assert ~df.isnull().any().any()
    return df


def get_W1_2_b_iii():
    p = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_3', 'outputs')
    db = os.path.join(p, 'wittgenstein_v1_2_c_ii_2_b_iii.sqlite')
    con = sqlite3.connect(db)

    df = None
    for ssp in ('SSP1', 'SSP2', 'SSP3', 'SSP4', 'SSP5'):
        query = f'SELECT GEOID, "2020" as {ssp} \
                  FROM population_by_race_gender_age_{ssp}'
        result = pd.read_sql(sql=query, con=con)
        result = result.groupby(by='GEOID').sum()
        if df is not None:
            df = pd.concat(objs=[df, result], axis=1)
        else:
            df = result.copy()

    con.close()

    df = df.reset_index().melt(id_vars='GEOID', var_name='SCENARIO', value_name='PROJECTED')
    df = df.merge(right=cy2st[['GEOID', 'STNAME']], how='left', on='GEOID')
    assert ~df.isnull().any().any()
    df = df[['STNAME', 'SCENARIO', 'PROJECTED']]
    df = df.groupby(by=['STNAME', 'SCENARIO'], as_index=False).sum()

    assert ~df.isnull().any().any()
    return df


def get_W2_2_c_i():
    p = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_3', 'outputs')
    db = os.path.join(p, 'wittgenstein_v2_2_c_ii_2_c_i.sqlite')
    con = sqlite3.connect(db)

    df = None
    for ssp in ('SSP1', 'SSP2', 'SSP3'):
        query = f'SELECT GEOID, "2020" as {ssp} \
                  FROM population_by_race_gender_age_{ssp}'
        result = pd.read_sql(sql=query, con=con)
        result = result.groupby(by='GEOID').sum()
        if df is not None:
            df = pd.concat(objs=[df, result], axis=1)
        else:
            df = result.copy()

    con.close()

    df = df.reset_index().melt(id_vars='GEOID', var_name='SCENARIO', value_name='PROJECTED')
    df = df.merge(right=cy2st[['GEOID', 'STNAME']], how='left', on='GEOID')
    assert ~df.isnull().any().any()
    df = df[['STNAME', 'SCENARIO', 'PROJECTED']]
    df = df.groupby(by=['STNAME', 'SCENARIO'], as_index=False).sum()

    assert ~df.isnull().any().any()
    return df


def get_W2_2_b_v():
    p = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_3', 'outputs')
    db = os.path.join(p, 'wittgenstein_v2_2_c_ii_2_b_v.sqlite')
    con = sqlite3.connect(db)

    df = None
    for ssp in ('SSP1', 'SSP2', 'SSP3'):
        query = f'SELECT GEOID, "2020" as {ssp} \
                  FROM population_by_race_gender_age_{ssp}'
        result = pd.read_sql(sql=query, con=con)
        result = result.groupby(by='GEOID').sum()
        if df is not None:
            df = pd.concat(objs=[df, result], axis=1)
        else:
            df = result.copy()

    con.close()

    df = df.reset_index().melt(id_vars='GEOID', var_name='SCENARIO', value_name='PROJECTED')
    df = df.merge(right=cy2st[['GEOID', 'STNAME']], how='left', on='GEOID')
    assert ~df.isnull().any().any()
    df = df[['STNAME', 'SCENARIO', 'PROJECTED']]
    df = df.groupby(by=['STNAME', 'SCENARIO'], as_index=False).sum()

    assert ~df.isnull().any().any()
    return df


def get_W1_2_c_i():
    p = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_3', 'outputs')
    db = os.path.join(p, 'wittgenstein_v1_2_c_ii_2_c_i.sqlite')
    con = sqlite3.connect(db)

    df = None
    for ssp in ('SSP1', 'SSP2', 'SSP3', 'SSP4', 'SSP5'):
        query = f'SELECT GEOID, "2020" as {ssp} \
                  FROM population_by_race_gender_age_{ssp}'
        result = pd.read_sql(sql=query, con=con)
        result = result.groupby(by='GEOID').sum()
        if df is not None:
            df = pd.concat(objs=[df, result], axis=1)
        else:
            df = result.copy()

    con.close()

    df = df.reset_index().melt(id_vars='GEOID', var_name='SCENARIO', value_name='PROJECTED')
    df = df.merge(right=cy2st[['GEOID', 'STNAME']], how='left', on='GEOID')
    assert ~df.isnull().any().any()
    df = df[['STNAME', 'SCENARIO', 'PROJECTED']]
    df = df.groupby(by=['STNAME', 'SCENARIO'], as_index=False).sum()

    assert ~df.isnull().any().any()
    return df


def get_W1_2_b_v():
    p = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_3', 'outputs')
    db = os.path.join(p, 'wittgenstein_v1_2_c_ii_2_b_v.sqlite')
    con = sqlite3.connect(db)

    df = None
    for ssp in ('SSP1', 'SSP2', 'SSP3', 'SSP4', 'SSP5'):
        query = f'SELECT GEOID, "2020" as {ssp} \
                  FROM population_by_race_gender_age_{ssp}'
        result = pd.read_sql(sql=query, con=con)
        result = result.groupby(by='GEOID').sum()
        if df is not None:
            df = pd.concat(objs=[df, result], axis=1)
        else:
            df = result.copy()

    con.close()

    df = df.reset_index().melt(id_vars='GEOID', var_name='SCENARIO', value_name='PROJECTED')
    df = df.merge(right=cy2st[['GEOID', 'STNAME']], how='left', on='GEOID')
    assert ~df.isnull().any().any()
    df = df[['STNAME', 'SCENARIO', 'PROJECTED']]
    df = df.groupby(by=['STNAME', 'SCENARIO'], as_index=False).sum()

    assert ~df.isnull().any().any()
    return df


def get_W1_3_a():
    p = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_3', 'outputs')
    db = os.path.join(p, 'wittgenstein_v1_2_c_ii_3_a_remainders.sqlite')
    con = sqlite3.connect(db)

    df = None
    for ssp in ('SSP1', 'SSP2', 'SSP3', 'SSP4', 'SSP5'):
        query = f'SELECT GEOID, "2020" as {ssp} \
                  FROM population_by_race_gender_age_{ssp}'
        try:
            result = pd.read_sql(sql=query, con=con)
        except:
            continue
        result = result.groupby(by='GEOID').sum()
        if df is not None:
            df = pd.concat(objs=[df, result], axis=1)
        else:
            df = result.copy()

    con.close()

    df = df.reset_index().melt(id_vars='GEOID', var_name='SCENARIO', value_name='PROJECTED')
    df = df.merge(right=cy2st[['GEOID', 'STNAME']], how='left', on='GEOID')
    assert ~df.isnull().any().any()
    df = df[['STNAME', 'SCENARIO', 'PROJECTED']]
    df = df.groupby(by=['STNAME', 'SCENARIO'], as_index=False).sum()

    assert ~df.isnull().any().any()
    return df


def get_W2_3_a():
    p = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_3', 'outputs', '2050')
    db = os.path.join(p, 'wittgenstein_v2_2_c_ii_3_a_remainders.sqlite')
    con = sqlite3.connect(db)

    df = None
    for ssp in ('SSP1', 'SSP2', 'SSP3'):
        query = f'SELECT GEOID, "2020" as {ssp} \
                  FROM population_by_race_gender_age_{ssp}'
        result = pd.read_sql(sql=query, con=con)
        result = result.groupby(by='GEOID').sum()
        if df is not None:
            df = pd.concat(objs=[df, result], axis=1)
        else:
            df = result.copy()

    con.close()

    df = df.reset_index().melt(id_vars='GEOID', var_name='SCENARIO', value_name='PROJECTED')
    df = df.merge(right=cy2st[['GEOID', 'STNAME']], how='left', on='GEOID')
    assert ~df.isnull().any().any()
    df = df[['STNAME', 'SCENARIO', 'PROJECTED']]
    df = df.groupby(by=['STNAME', 'SCENARIO'], as_index=False).sum()

    assert ~df.isnull().any().any()
    return df


def get_W2_5():
    p = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_5', 'outputs')
    db = os.path.join(p, 'wittgenstein_v2.sqlite')
    con = sqlite3.connect(db)

    df = None
    for ssp in ('SSP1', 'SSP2', 'SSP3'):
        query = f'SELECT GEOID, "2020" as {ssp} \
                  FROM population_by_race_gender_age_{ssp}'
        try:
            result = pd.read_sql(sql=query, con=con)
        except:
            continue
        result = result.groupby(by='GEOID').sum()
        if df is not None:
            df = pd.concat(objs=[df, result], axis=1)
        else:
            df = result.copy()

    con.close()

    df = df.reset_index().melt(id_vars='GEOID', var_name='SCENARIO', value_name='PROJECTED')
    df = df.merge(right=cy2st[['GEOID', 'STNAME']], how='left', on='GEOID')
    assert ~df.isnull().any().any()
    df = df[['STNAME', 'SCENARIO', 'PROJECTED']]
    df = df.groupby(by=['STNAME', 'SCENARIO'], as_index=False).sum()

    assert ~df.isnull().any().any()
    return df


def calculate_accuracy_W1_2_b_v():
    projection = get_W1_2_b_v()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'Morefield_W1_2_b_v'
    df['Published'] = 2021
    df['Launch year'] = 2015

    return df


def calculate_accuracy_W2_2_b_v():
    projection = get_W2_2_b_v()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'Morefield_W2_2_b_v'
    df['Published'] = 2021
    df['Launch year'] = 2015

    return df


def calculate_accuracy_W2_2_b_iii():
    projection = get_W2_2_b_iii()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'Morefield_W2_2_b_iii'
    df['Published'] = 2021
    df['Launch year'] = 2015

    return df


def calculate_accuracy_W1_2_b_iii():
    projection = get_W1_2_b_iii()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'Morefield_W1_2_b_iii'
    df['Published'] = 2021
    df['Launch year'] = 2015

    return df


def calculate_accuracy_W2_2_c_i():
    projection = get_W2_2_c_i()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'Morefield_W2_2_c_i'
    df['Published'] = 2021
    df['Launch year'] = 2015

    return df


def calculate_accuracy_W1_2_c_i():
    projection = get_W1_2_c_i()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'Morefield_W1_2_c_i'
    df['Published'] = 2021
    df['Launch year'] = 2015

    return df


def calculate_accuracy_W2_3_a():
    projection = get_W2_3_a()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'Morefield_W2_3_a'
    df['Published'] = 2021
    df['Launch year'] = 2015

    return df


def calculate_accuracy_W1_3_a():
    projection = get_W1_3_a()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'Morefield_W1_3_a'
    df['Published'] = 2021
    df['Launch year'] = 2015

    return df

def calculate_accuracy_W2_5():
    projection = get_W2_5()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'Morefield'
    df['Published'] = 2021
    df['Launch year'] = 2015

    return df


def calculate_accuracy_urban_institute():
    projection = get_urban_institute()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'Urban Institute'
    df['Published'] = 2015
    df['Launch year'] = 2010

    min_ = df.MAPE.min()
    max_ = df.MAPE.max()
    med_ = df.MAPE.median()

    df = df.loc[df.MAPE.isin((min_, med_, max_)), :]

    return df


def calculate_accuracy_hauer():
    projection = get_hauer()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'Hauer'
    df['Published'] = 2019
    df['Launch year'] = 2015

    return df


def calculate_accuracy_gao():
    projection = get_gao()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'Gao'
    df['Published'] = 2017
    df['Launch year'] = 2000

    return df


def calculate_accuracy_ornl():
    projection = get_ornl()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'ORNL'
    df['Published'] = 2015
    df['Launch year'] = 2010

    return df


def calculate_accuracy_usfs():
    projection = get_usfs()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'USFS'
    df['Published'] = 2019
    df['Launch year'] = 2010

    return df


def calculate_accuracy_jiang():
    projection = get_jiang()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'Jiang et al'
    df['Published'] = 2020
    df['Launch year'] = 2010

    return df


def calculate_accuracy_iclus_v1():
    projection = get_iclus_v1()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'U.S. EPA'
    df['Published'] = 2010
    df['Launch year'] = 2000

    return df


def calculate_accuracy_iclus_v2():
    projection = get_iclus_v2()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'U.S. EPA'
    df['Published'] = 2017
    df['Launch year'] = 2010

    return df


def calculate_accuracy_uva():
    projection = get_uva()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'UVA'
    df['Published'] = 2018
    df['Launch year'] = 2017

    return df


def calculate_accuracy_census_1997():
    projection = get_census_1997()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'U.S. Census'
    df['Published'] = 1997
    df['Launch year'] = 1995

    return df


def calculate_accuracy_census_2005():
    projection = get_census_2005()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'U.S. Census'
    df['Published'] = 2005
    df['Launch year'] = 2000

    return df


def calculate_accuracy_woodspoole():
    projection = get_woodspoole()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'Woods & Poole'
    df['Published'] = 2010
    df['Launch year'] = 2010

    return df


def calculate_accuracy_metrics(projection):
    population = get_state_census_estimate_2020()

    df = projection.merge(right=population, how='left', left_on='STNAME', right_on='STATE_NAME')

    df['MAPE'] = df.eval('(abs(PROJECTED - CENSUS_2020) / CENSUS_2020) * 100')
    df['Q'] = np.log(df['PROJECTED'] / df['CENSUS_2020']) ** 2
    if 'SCENARIO' in df.columns:
        df['SPE'] = ((df['PROJECTED'] - df['CENSUS_2020']) / df['CENSUS_2020']) ** 2
        df['MSPE'] = df.groupby(by='SCENARIO')['SPE'].transform('mean')
        df['RMSPE'] = df['MSPE'] ** 0.5
        df = df[['SCENARIO', 'MAPE', 'RMSPE', 'Q']].groupby(by='SCENARIO', as_index=False).mean()
    else:
        df['RMSPE'] = ((df['PROJECTED'] - df['CENSUS_2020']) / df['CENSUS_2020']) ** 2
        df = pd.DataFrame(df[['MAPE', 'Q', 'RMSPE']].mean()).T
        df['RMSPE'] = df['RMSPE'] ** 0.5

    return df


def main():

    list_of_dataframes = []

    # uses 1995 launch population
    list_of_dataframes.append(calculate_accuracy_census_1997())

    # uses 2000 launch population
    list_of_dataframes.append(calculate_accuracy_gao())

    # uses 2005 launch population
    list_of_dataframes.append(calculate_accuracy_census_2005())

    # uses 2005 launch population
    list_of_dataframes.append(calculate_accuracy_iclus_v1())

    # uses 2010 launch population
    list_of_dataframes.append(calculate_accuracy_usfs())
    list_of_dataframes.append(calculate_accuracy_uva())
    list_of_dataframes.append(calculate_accuracy_urban_institute())
    list_of_dataframes.append(calculate_accuracy_iclus_v2())
    # list_of_dataframes.append(calculate_accuracy_woodspoole())

    # 2030 and 2040 projections only
    list_of_dataframes.append(calculate_accuracy_ornl())

    # uses 2015 demographic information(?)
    list_of_dataframes.append(calculate_accuracy_jiang())

    # uses 2015 launch population
    list_of_dataframes.append(calculate_accuracy_hauer())

#     list_of_dataframes.append(calculate_accuracy_W1_2_b_iii())
#     list_of_dataframes.append(calculate_accuracy_W1_2_b_v())
#     list_of_dataframes.append(calculate_accuracy_W1_2_c_i())
    list_of_dataframes.append(calculate_accuracy_W1_3_a())

#     list_of_dataframes.append(calculate_accuracy_W2_2_b_iii())
#     list_of_dataframes.append(calculate_accuracy_W2_2_c_i())
#     list_of_dataframes.append(calculate_accuracy_W2_2_b_v())
#     list_of_dataframes.append(calculate_accuracy_W2_3_a())
    list_of_dataframes.append(calculate_accuracy_W2_5())

    df = pd.concat(objs=list_of_dataframes)
    df['SCENARIO'].fillna(value='*', inplace=True)
    df = df[['Source', 'SCENARIO', 'Q', 'MAPE', 'RMSPE', 'Published', 'Launch year']]
    df.sort_values(by='MAPE', inplace=True)
    print(df)
    p = 'E:\\Dissertation\\analysis\\part_5\\outputs'
    f = 'accuracy_2020_by_state.csv'
    df.to_csv(path_or_buf=os.path.join(p, f))

    # remove all of my models except the best performer
    min_ = df.loc[df.Source.str.contains('Morefield'), 'MAPE'].min()
    df = df[(~df.Source.str.contains('Morefield')) | (df.MAPE == min_)]
    df.sort_values(by='Source', inplace=True)
    df = df.query('Source != "Morefield"')

    #     plt.gca().plot(smith_years, smith_mapes, color='black')
    #     plt.gca().plot(smith_years, morefield_mapes, color='black', linestyle='-')

    smith_years = (2020, 2015, 2010, 2005, 2000, 1995)
    smith_mapes = (0, 3, 6, 9, 12, 15)
    morefield_mapes = (0, 2, 4, 6, 8, 10)
    aspirational_mapes = (0, 1, 2, 3, 4, 5)

    sns.scatterplot(x="Launch year",
                    y='MAPE',
                    hue='Source',
                    style='Source',
                    data=df,
                    palette='bright',
                    edgecolor='none',
                    zorder=10,
                    s=150)

    sns.lineplot(x=smith_years, y=smith_mapes, color='black', zorder=1, ax=plt.gca(), **{'label': '6%/decade (Smith et al)'})
    sns.lineplot(x=smith_years, y=morefield_mapes, color='black', zorder=1, linestyle='--', ax=plt.gca(), **{'label': '4%/decade'})
    sns.lineplot(x=smith_years, y=aspirational_mapes, color='black', zorder=1, linestyle=':', ax=plt.gca(), **{'label': '2%/decade'})

    plt.legend(loc='center right', bbox_to_anchor=(1.37, 0.5), frameon=False, markerscale=1)
    plt.gcf().set_size_inches(10, 5)
    plt.title(label="State-level validation against 2020 Census:\nMean absolute percentage error (MAPE)")
    plt.tight_layout()
    plt.subplots_adjust(right=0.748)
    plt.show()

    print("Finished!")


if __name__ == '__main__':
    main()
