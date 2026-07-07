"""
Author:  Phil Morefield (pmorefie@gmu.edu)
Purpose:
Created:
"""
import os
import sqlite3

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 10)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

DISSERTATION_FOLDER = 'E:\\Dissertation'
PART_3_INPUT_FOLDER = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_3', 'inputs')
PART_3_A_INPUT_FOLDER = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_3_a', 'inputs')
MIGRATION_DB = os.path.join(DISSERTATION_FOLDER, 'databases', 'migration.sqlite')
con = sqlite3.connect(MIGRATION_DB)
cofips_changes = pd.read_sql(sql='SELECT OLD_FIPS, NEW_FIPS FROM fips_or_name_changes', con=con)
con.close()


def update_cofips(df_orig):
    df = df_orig.merge(right=cofips_changes, how='left', left_on='COFIPS', right_on='OLD_FIPS')
    df.loc[~df.NEW_FIPS.isnull(), 'COFIPS'] = df.NEW_FIPS
    df.drop(columns=['OLD_FIPS', 'NEW_FIPS'], inplace=True)

    assert ~df.isnull().any().any()
    return df


def get_county_census_estimate_2020():

    db = os.path.join(DISSERTATION_FOLDER, 'databases', 'population.sqlite')
    con = sqlite3.connect(database=db)
    query = 'SELECT COFIPS, POPESTIMATE2020 AS CENSUS_2020 \
             FROM Census_total_county_population_total_2020'
    df = pd.read_sql(sql=query, con=con)
    con.close()

    df = update_cofips(df)
    df = df.groupby(by='COFIPS', as_index=False).sum()

    assert ~df.isnull().any().any()
    return df


def get_county_census_estimate_2019():
    db = os.path.join(PART_3_A_INPUT_FOLDER, 'part_3_a_inputs.sqlite')
    con = sqlite3.connect(db)

    query = 'SELECT COFIPS, POPESTIMATE2019 AS CENSUS_2019 \
             FROM Census_county_estimates_2010_2019'
    df = pd.read_sql(sql=query, con=con)
    con.close()

    df = update_cofips(df)
    df = df.groupby(by='COFIPS', as_index=False).sum()

    assert ~df.isnull().any().any()
    return df


def get_iclus_v2():
    db = os.path.join(PART_3_INPUT_FOLDER, 'epa', 'epa.sqlite')
    con = sqlite3.connect(db)

    df = pd.read_sql(sql='SELECT * FROM ICLUS_v2_county', con=con)
    con.close()

    df.drop(columns=['TOTALPOP90', 'TOTALPOP00', 'TOTALPOP10'], inplace=True)
    df = df.melt(id_vars='COFIPS', var_name='SCENARIO', value_name='PROJECTED')
    df = df.loc[df.SCENARIO.str.contains('2020'), :]
    df['PROJECTED'] = df['PROJECTED'].round().astype(int)
    df['SCENARIO'] = df['SCENARIO'].str.upper()

    assert ~df.isnull().any().any()
    return df


def get_iclus_v1():
    db = os.path.join(PART_3_INPUT_FOLDER, 'epa', 'epa.sqlite')
    con = sqlite3.connect(db)

    df = pd.read_sql(sql='SELECT FIPS AS COFIPS, SCENARIO, "2020" AS PROJECTED \
                          FROM ICLUS_v1', con=con)

    df = update_cofips(df)
    df = df.groupby(by=['COFIPS', 'SCENARIO'], as_index=False).sum()

    assert ~df.isnull().any().any()
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
    df.rename(columns={'SSP': 'SCENARIO'}, inplace=True)

    df = update_cofips(df)

    df = df.groupby(by=['COFIPS', 'SCENARIO'], as_index=False).sum()

    assert ~df.isnull().any().any()
    return df


def get_gao():
    db = os.path.join(PART_3_INPUT_FOLDER, 'ncar', 'ncar.sqlite')
    con = sqlite3.connect(db)
    df = pd.concat(objs=[pd.read_sql_query(sql=f'SELECT GEOID AS COFIPS, SSP AS SCENARIO, "2020" AS "PROJECTED" FROM ssp{ssp}_total', con=con) for ssp in range(1, 6)])
    con.close()

    df = update_cofips(df)
    df = df.groupby(by=['COFIPS', 'SCENARIO'], as_index=False).sum()

    assert ~df.isnull().any().any()
    return df


def get_ornl():
    '''
    The ORNL projections only include 2030 and 2050. Use the 2010 Census values
    to interpolate a 2020 value.
    '''
    # get the 2010 population values first
    db = 'E:\\Dissertation\\databases\\population.sqlite'
    con = sqlite3.connect(db)

    # query = 'SELECT * FROM county_population_sexracehisp_intercensal2010'
    # pop2010 = pd.read_sql(sql=query, con=con)
    # con.close()
    # pop2010.drop(columns=['YEAR', 'AGEGRP'], inplace=True)
    # pop2010 = pop2010.groupby(by='COFIPS', as_index=False).sum()
    # pop2010['POP2010'] = pop2010.iloc[:, 1:].sum(axis=1)
    # pop2010 = pop2010[['COFIPS', 'POP2010']]

    # pop2010 = update_cofips(pop2010)
    # pop2010 = pop2010.groupby(by='COFIPS', as_index=False).sum()

    query = 'SELECT GEOID AS COFIPS, POPULATION AS POP2010 \
             FROM county_population_genderraceage_Census2010'
    pop2010 = pd.read_sql(sql=query, con=con)
    con.close()
    pop2010['COFIPS'] = pop2010['COFIPS'].astype(str).str.zfill(5)
    pop2010 = pop2010.groupby(by='COFIPS', as_index=False).sum()
    pop2010 = update_cofips(pop2010)
    pop2010 = pop2010.groupby(by='COFIPS', as_index=False).sum()

    # get the ORNL projections next
    db = os.path.join(PART_3_INPUT_FOLDER, 'ornl', 'ornl.sqlite')
    con = sqlite3.connect(db)
    df = pd.read_sql_query(sql='SELECT GEOID AS COFIPS, "2030" FROM landcast_2030', con=con)
    con.close()

    df = update_cofips(df)
    df = df.groupby(by='COFIPS', as_index=False).sum()
    df = df.merge(right=pop2010, how='left', on='COFIPS')

    # Loving County, TX is not included in the 2010 population file
    df.dropna(inplace=True)

    df['PROJECTED'] = ((df['POP2010'] + df['2030']) / 2).round().astype(int)
    df = df[['COFIPS', 'PROJECTED']]

    assert ~df.isnull().any().any()
    return df


def get_hauer():
    query = 'SELECT GEOID AS COFIPS, SSP1, SSP2, SSP3, SSP4, SSP5 \
             FROM hauer_ssp \
             WHERE YEAR == 2020'

    db = os.path.join(PART_3_INPUT_FOLDER, 'hauer', 'hauer.sqlite')
    con = sqlite3.connect(db)
    df = pd.read_sql_query(sql=query, con=con)
    con.close()

    df = update_cofips(df)
    df = df.groupby(by='COFIPS', as_index=False).sum()
    df = df.melt(id_vars='COFIPS', var_name='SCENARIO', value_name='PROJECTED')
    df = df.groupby(by=['COFIPS', 'SCENARIO'], as_index=False).sum()

    assert ~df.isnull().any().any()
    return df


def get_part_2_c_ii_2_b_iii():
    p = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_2_c_ii_2_b_iii', 'outputs')
    db = os.path.join(p, 'wittgenstein_v2.sqlite')
    con = sqlite3.connect(db)

    df = None
    for ssp in ('SSP1', 'SSP2', 'SSP3'):
        query = f'SELECT GEOID AS COFIPS, "2020" as {ssp} \
                  FROM population_by_race_gender_age_{ssp}'
        result = pd.read_sql(sql=query, con=con)
        result = result.groupby(by='COFIPS').sum()
        if df is not None:
            df = pd.concat(objs=[df, result], axis=1)
        else:
            df = result.copy()

    con.close()

    df = df.reset_index().melt(id_vars='COFIPS', var_name='SCENARIO', value_name='PROJECTED')

    assert ~df.isnull().any().any()
    return df


def get_woodspoole():
    query = 'SELECT GEOID AS COFIPS, POP2020 AS PROJECTED \
             FROM woodspoole_2020_2050'

    db = os.path.join(PART_3_INPUT_FOLDER, 'woodspoole', 'woodspoole.sqlite')
    con = sqlite3.connect(db)
    df = pd.read_sql_query(sql=query, con=con)
    con.close()

    df = update_cofips(df)
    assert ~df.isnull().any().any()

    return df


def get_W1_2_b_iii():
    p = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_3', 'outputs')
    db = os.path.join(p, 'wittgenstein_v1_2_c_ii_2_b_iii.sqlite')
    con = sqlite3.connect(db)

    df = None
    for ssp in ('SSP1', 'SSP2', 'SSP3', 'SSP4', 'SSP5'):
        query = f'SELECT GEOID AS COFIPS, "2020" as {ssp} \
                  FROM population_by_race_gender_age_{ssp}'
        result = pd.read_sql(sql=query, con=con)
        result = result.groupby(by='COFIPS').sum()
        if df is not None:
            df = pd.concat(objs=[df, result], axis=1)
        else:
            df = result.copy()

    con.close()

    df = df.reset_index().melt(id_vars='COFIPS', var_name='SCENARIO', value_name='PROJECTED')

    assert ~df.isnull().any().any()
    return df


def get_W2_2_c_i():
    p = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_3', 'outputs')
    db = os.path.join(p, 'wittgenstein_v2_2_c_ii_2_c_i.sqlite')
    con = sqlite3.connect(db)

    df = None
    for ssp in ('SSP1', 'SSP2', 'SSP3'):
        query = f'SELECT GEOID AS COFIPS, "2020" as {ssp} \
                  FROM population_by_race_gender_age_{ssp}'
        result = pd.read_sql(sql=query, con=con)
        result = result.groupby(by='COFIPS').sum()
        if df is not None:
            df = pd.concat(objs=[df, result], axis=1)
        else:
            df = result.copy()

    con.close()

    df = df.reset_index().melt(id_vars='COFIPS', var_name='SCENARIO', value_name='PROJECTED')

    assert ~df.isnull().any().any()
    return df


def get_W2_2_b_v():
    p = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_3', 'outputs')
    db = os.path.join(p, 'wittgenstein_v2_2_c_ii_2_b_v.sqlite')
    con = sqlite3.connect(db)

    df = None
    for ssp in ('SSP1', 'SSP2', 'SSP3'):
        query = f'SELECT GEOID AS COFIPS, "2020" as {ssp} \
                  FROM population_by_race_gender_age_{ssp}'
        result = pd.read_sql(sql=query, con=con)
        result = result.groupby(by='COFIPS').sum()
        if df is not None:
            df = pd.concat(objs=[df, result], axis=1)
        else:
            df = result.copy()

    con.close()

    df = df.reset_index().melt(id_vars='COFIPS', var_name='SCENARIO', value_name='PROJECTED')

    assert ~df.isnull().any().any()
    return df


def get_W2_3_a():
    p = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_3', 'outputs', '2050')
    db = os.path.join(p, 'wittgenstein_v2_2_c_ii_3_a_remainders.sqlite')
    con = sqlite3.connect(db)

    df = None
    for ssp in ('SSP1', 'SSP2', 'SSP3'):
        query = f'SELECT GEOID AS COFIPS, "2020" as {ssp} \
                  FROM population_by_race_gender_age_{ssp}'
        try:
            result = pd.read_sql(sql=query, con=con)
        except:
            continue
        result = result.groupby(by='COFIPS').sum()
        if df is not None:
            df = pd.concat(objs=[df, result], axis=1)
        else:
            df = result.copy()

    con.close()

    df = df.reset_index().melt(id_vars='COFIPS', var_name='SCENARIO', value_name='PROJECTED')

    assert ~df.isnull().any().any()
    return df


def get_W2_4():
    p = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_4', 'outputs')
    db = os.path.join(p, 'wittgenstein_v2.sqlite')
    con = sqlite3.connect(db)

    df = None
    for ssp in ('SSP1', 'SSP2', 'SSP3'):
        query = f'SELECT GEOID AS COFIPS, "2020" as {ssp} \
                  FROM population_by_race_gender_age_{ssp}'
        try:
            result = pd.read_sql(sql=query, con=con)
        except:
            continue
        result = result.groupby(by='COFIPS').sum()
        if df is not None:
            df = pd.concat(objs=[df, result], axis=1)
        else:
            df = result.copy()

    con.close()

    df = df.reset_index().melt(id_vars='COFIPS', var_name='SCENARIO', value_name='PROJECTED')

    assert ~df.isnull().any().any()
    return df


def get_W1_2_c_i():
    p = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_3', 'outputs')
    db = os.path.join(p, 'wittgenstein_v1_2_c_ii_2_c_i.sqlite')
    con = sqlite3.connect(db)

    df = None
    for ssp in ('SSP1', 'SSP2', 'SSP3', 'SSP4', 'SSP5'):
        query = f'SELECT GEOID AS COFIPS, "2020" as {ssp} \
                  FROM population_by_race_gender_age_{ssp}'
        result = pd.read_sql(sql=query, con=con)
        result = result.groupby(by='COFIPS').sum()
        if df is not None:
            df = pd.concat(objs=[df, result], axis=1)
        else:
            df = result.copy()

    con.close()

    df = df.reset_index().melt(id_vars='COFIPS', var_name='SCENARIO', value_name='PROJECTED')

    assert ~df.isnull().any().any()
    return df


def get_W1_2_b_v():
    p = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_3', 'outputs')
    db = os.path.join(p, 'wittgenstein_v1_2_c_ii_2_b_v.sqlite')
    con = sqlite3.connect(db)

    df = None
    for ssp in ('SSP1', 'SSP2', 'SSP3', 'SSP4', 'SSP5'):
        query = f'SELECT GEOID AS COFIPS, "2020" as {ssp} \
                  FROM population_by_race_gender_age_{ssp}'
        result = pd.read_sql(sql=query, con=con)
        result = result.groupby(by='COFIPS').sum()
        if df is not None:
            df = pd.concat(objs=[df, result], axis=1)
        else:
            df = result.copy()

    con.close()

    df = df.reset_index().melt(id_vars='COFIPS', var_name='SCENARIO', value_name='PROJECTED')

    assert ~df.isnull().any().any()
    return df


def get_W2_2_b_iii():
    p = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_3', 'outputs')
    db = os.path.join(p, 'wittgenstein_v2_2_c_ii_2_b_iii.sqlite')
    con = sqlite3.connect(db)

    df = None
    for ssp in ('SSP1', 'SSP2', 'SSP3'):
        query = f'SELECT GEOID AS COFIPS, "2020" as {ssp} \
                  FROM population_by_race_gender_age_{ssp}'
        result = pd.read_sql(sql=query, con=con)
        result = result.groupby(by='COFIPS').sum()
        if df is not None:
            df = pd.concat(objs=[df, result], axis=1)
        else:
            df = result.copy()

    con.close()

    df = df.reset_index().melt(id_vars='COFIPS', var_name='SCENARIO', value_name='PROJECTED')

    assert ~df.isnull().any().any()
    return df


def get_W1_3_a():
    p = os.path.join(DISSERTATION_FOLDER, 'analysis', 'part_3', 'outputs', '2050')
    db = os.path.join(p, 'wittgenstein_v1_2_c_ii_3_a_remainders.sqlite')
    con = sqlite3.connect(db)

    df = None
    for ssp in ('SSP1', 'SSP2', 'SSP3', 'SSP4', 'SSP5'):
        query = f'SELECT GEOID AS COFIPS, "2020" as {ssp} \
                  FROM population_by_race_gender_age_{ssp}'
        result = pd.read_sql(sql=query, con=con)
        result = result.groupby(by='COFIPS').sum()
        if df is not None:
            df = pd.concat(objs=[df, result], axis=1)
        else:
            df = result.copy()

    con.close()

    df = df.reset_index().melt(id_vars='COFIPS', var_name='SCENARIO', value_name='PROJECTED')

    assert ~df.isnull().any().any()
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


def calculate_accuracy_iclus_v1():
    projection = get_iclus_v1()
    df = calculate_accuracy_metrics(projection, source='ICLUS v1')

    df['Source'] = 'U.S. EPA'
    df['Published'] = 2010
    df['Launch year'] = 2000

    return df


def calculate_accuracy_iclus_v2():
    projection = get_iclus_v2()
    df = calculate_accuracy_metrics(projection, source='ICLUS v2')

    df['Source'] = 'U.S. EPA'
    df['Published'] = 2017
    df['Launch year'] = 2010

    return df


def calculate_accuracy_woodspoole():
    projection = get_woodspoole()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'Woods & Poole'
    df['Published'] = 2010
    df['Launch year'] = 2010

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


def calculate_accuracy_W2_3_a():
    projection = get_W2_3_a()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'Morefield_W2_3_a'
    df['Published'] = 2021
    df['Launch year'] = 2015

    return df


def calculate_accuracy_W2_4():
    projection = get_W2_4()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'Morefield_W2_4'
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


def calculate_accuracy_W1_2_c_i():
    projection = get_W1_2_c_i()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'Morefield_W1_2_c_i'
    df['Published'] = 2021
    df['Launch year'] = 2015

    return df


def calculate_accuracy_W1_2_b_v():
    projection = get_W1_2_b_v()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'Morefield_W1_2_b_v'
    df['Published'] = 2021
    df['Launch year'] = 2015

    return df

def calculate_accuracy_W1_3_a():
    projection = get_W1_3_a()
    df = calculate_accuracy_metrics(projection)

    df['Source'] = 'Morefield'
    df['Published'] = 2021
    df['Launch year'] = 2015

    return df


def create_county_accuracy_maps(df, source):
    for metric in ('MAPE', 'Q', 'MPE'):
        if metric == 'MAPE':
            bins = (0, 1, 5, 10, 15, 1000)
            labels = ('<1%', '<5%', '<10%', '<15%', '>=15%')
        elif metric == 'Q':
            bins = (0, 0.001, 0.01, 0.1, 1.0, 10.0)
            labels = ('<0.001', '<0.01', '<0.1', '<1.0', '>=1')
        elif metric == 'MPE':
            bins = (-100, -50, -10, 0, 10, 50, 100)
            labels = ('<-50%', '<-10%', '<0%', '<10%', '<50%', '>=50%'),
        else:
            raise Exception
        df['RATE_BINS'] = pd.cut(x=df[metric],
                                 bins=bins,
                                 right=True,
                                 labels=labels,
                                 include_lowest=True)
        gdf = read_county_shapefile()
        states = read_state_shapefile()
        gdf = gdf.merge(right=df, how='left', on='COFIPS')

        if gdf.loc[gdf.COFIPS == '02999', metric].isnull().values[0]:
            gdf.query('COFIPS != "02999"', inplace=True)
        if gdf.loc[gdf.COFIPS == '15999', metric].isnull().values[0]:
            gdf.query('COFIPS != "15999"', inplace=True)

        if 'SCENARIO' in df.columns:
            for scenario in df['SCENARIO'].unique():
                to_plot = gdf.query('SCENARIO == @scenario')
                to_plot.plot(column='RATE_BINS',
                             categorical=True,
                             cmap='flare',
                             legend=True,
                             legend_kwds={'bbox_to_anchor': (0.23, 0.27),
                                          'facecolor': 'silver',
                                          'fancybox': True})
                states.boundary.plot(ax=plt.gca(), edgecolor='lightgray', linewidth=0.2)
                plt.gca().set_xlim(-2371000, 2278000)
                plt.gca().set_ylim(246000, 3186000)
                plt.gca().axis('off')
                plt.title(label=f"{metric} by county\nSource: {source}\nScenario: {scenario}")
                plt.tight_layout()
                plt.show()


def read_county_shapefile():
    gdb = 'E:\\Dissertation\\analysis\\part_3_a\\inputs\\part_3_a_inputs.gdb'
    f = 'counties_conus_and_AKHI'
    gdf = gpd.read_file(filename=gdb, layer=f)
    gdf.rename(columns={'GEOID': 'COFIPS'}, inplace=True)
    gdf = gdf.to_crs("EPSG:5070")

    return gdf


def read_state_shapefile():
    gdb = 'E:\\Dissertation\\analysis\\part_3_a\\inputs\\part_3_a_inputs.gdb'
    f = 'states_conus_and_AKHI'
    gdf = gpd.read_file(filename=gdb, layer=f)
    gdf = gdf.to_crs("EPSG:5070")

    return gdf


def calculate_accuracy_metrics(projection, source=None):
    population = get_county_census_estimate_2020()
    df = projection.merge(right=population, how='left', on='COFIPS')

    df['MAPE'] = df.eval('(abs(PROJECTED - CENSUS_2020) / CENSUS_2020) * 100')
    df['Q'] = np.log(df['PROJECTED'] / df['CENSUS_2020']) ** 2
    df['MPE'] = df.eval('((PROJECTED - CENSUS_2020) / CENSUS_2020) * 100')
    # create_county_accuracy_maps(df, source)
    if 'SCENARIO' in df.columns:
        df['SPE'] = ((df['PROJECTED'] - df['CENSUS_2020']) / df['CENSUS_2020']) ** 2
        df['MSPE'] = df.groupby(by='SCENARIO')['SPE'].transform('mean')
        df['RMSPE'] = df['MSPE'] ** 0.5
        df = df[['SCENARIO', 'MAPE', 'RMSPE', 'Q', 'MPE']].groupby(by='SCENARIO', as_index=False).mean()
    else:
        df['RMSPE'] = ((df['PROJECTED'] - df['CENSUS_2020']) / df['CENSUS_2020']) ** 2
        df = pd.DataFrame(df[['MAPE', 'Q', 'RMSPE']].mean()).T
        df['RMSPE'] = df['RMSPE'] ** 0.5

    assert ~df.isnull().any().any()
    return df


def main():

    list_of_dataframes = []

    # uses 2005 launch population
    list_of_dataframes.append(calculate_accuracy_iclus_v1())

    # uses 2015 launch population
#     list_of_dataframes.append(calculate_accuracy_W1_2_b_iii())
#     list_of_dataframes.append(calculate_accuracy_W1_2_b_v())
#     list_of_dataframes.append(calculate_accuracy_W1_2_c_i())
 #   list_of_dataframes.append(calculate_accuracy_W1_3_a())
#     list_of_dataframes.append(calculate_accuracy_W2_4())

#     list_of_dataframes.append(calculate_accuracy_W2_2_b_iii())
#     list_of_dataframes.append(calculate_accuracy_W2_2_b_v())
#     list_of_dataframes.append(calculate_accuracy_W2_2_c_i())
#     list_of_dataframes.append(calculate_accuracy_W2_3_a())


    list_of_dataframes.append(calculate_accuracy_hauer())

    # 2030 and 2040 projections only
    list_of_dataframes.append(calculate_accuracy_ornl())

    # uses 2010 launch population
    list_of_dataframes.append(calculate_accuracy_usfs())
    list_of_dataframes.append(calculate_accuracy_iclus_v2())
    list_of_dataframes.append(calculate_accuracy_woodspoole())

    # uses 2000 launch population
    list_of_dataframes.append(calculate_accuracy_gao())

    df = pd.concat(objs=list_of_dataframes)
    df['SCENARIO'].fillna(value='*', inplace=True)
    df = df[['Source', 'SCENARIO', 'Q', 'MAPE', 'RMSPE', 'Published', 'Launch year']]
    df.sort_values(by='MAPE', inplace=True)
    print(df)
    p = 'E:\\Dissertation\\analysis\\part_3_a\\outputs'
    f = 'accuracy_2020_by_county.csv'
    df.to_csv(path_or_buf=os.path.join(p, f))

    # remove all of my models except the best performer
    # min_ = df.loc[df.Source.str.contains('Morefield'), 'MAPE'].min()
    # df = df[(~df.Source.str.contains('Morefield')) | (df.MAPE == min_)]
    # df.sort_values(by='Source', inplace=True)

    smith_years = (2015, 2010, 2005, 2000, 1995)
    smith_mapes = (6, 12, 18, 24, 30)
    morefield_mapes = (4, 8, 12, 16, 20)
    aspirational_mapes = (2, 4, 6, 8, 10)

    sns.scatterplot(x="Launch year",
                    y='MAPE',
                    hue='Source',
                    style='Source',
                    data=df,
                    palette='bright',
                    edgecolor='none',
                    zorder=10,
                    s=150)

    plt.gca().set_xticks(range(1995, 2020, 5))

    sns.lineplot(x=smith_years, y=smith_mapes, color='black', zorder=1, ax=plt.gca(), **{'label': '12%/decade (Smith et al)'})
    sns.lineplot(x=smith_years, y=morefield_mapes, color='black', zorder=1, linestyle='--', ax=plt.gca(), **{'label': '8%/decade'})
    sns.lineplot(x=smith_years, y=aspirational_mapes, color='black', zorder=1, linestyle=':', ax=plt.gca(), **{'label': '4%/decade'})


    plt.legend(loc='center right', bbox_to_anchor=(1.37, 0.5), frameon=False, markerscale=1)
    plt.gcf().set_size_inches(10, 5)
    plt.title(label="County-level validation against 2020 Census:\nMean absolute percentage error (MAPE)")
    plt.tight_layout()
    plt.subplots_adjust(right=0.748)
    plt.show()

    print("Finished!")


if __name__ == '__main__':
    main()
