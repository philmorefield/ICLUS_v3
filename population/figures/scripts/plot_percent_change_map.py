import os
import sqlite3

import geopandas as gpd
import pandas as pd

from matplotlib import pyplot as plt


PART_3_a_INPUTS = 'D:\\projects\\ICLUS_v3\\population\\inputs\\part_3_a\\part_3_a_inputs.sqlite'


def create_dataframe():
    # historical population

    query = 'SELECT COFIPS as GEOID, POPESTIMATE2020 as POP2020 \
             FROM Census_county_estimates_2010_2020'
    con = sqlite3.connect(PART_3_a_INPUTS)
    pop2020 = pd.read_sql(sql=query, con=con, index_col='GEOID')
    con.close()

    # future population
    con = sqlite3.connect('D:\\projects\\ICLUS_v3\\population\\outputs\\wittgenstein_v2_2023211154336.sqlite', timeout=300)
    pop2030 = pd.read_sql('SELECT GEOID, "2030" as POP2030 from population_by_race_gender_age_SSP3', con=con)
    con.close()
    pop2030 = pop2030.groupby(by='GEOID').sum()

    df = pop2030.join(other=pop2020)
    df.eval('PercentChange = ((POP2030 - POP2020) / POP2020) * 100', inplace=True)
    df.reset_index(inplace=True)

    return df


def create_map(df):

    bins = (-200, -6, -4, -2, 2, 4, 6, 200)
    labels = ('<-12%', '<-8%', '<-4%', '+-4%', '>4%', '>8%', '>12%')

    df['RATE_BINS'] = pd.cut(x=df['PercentChange'],
                             bins=bins,
                             right=True,
                             labels=labels,
                             include_lowest=True)
    gdf = read_county_shapefile()
    states = read_state_shapefile()
    gdf = gdf.merge(right=df, how='left', on='GEOID')

    # gdf.query('COFIPS != "02999"', inplace=True)
    # gdf.query('COFIPS != "15999"', inplace=True)

    # to_plot = gdf.query('SCENARIO == @scenario')
    gdf.plot(column='RATE_BINS',
             categorical=True,
             cmap='RdYlBu',
             legend=True,
             legend_kwds={'bbox_to_anchor': (1.05, 0.4),
                          'facecolor': 'silver',
                          'fancybox': True,
                          'title': 'Error per\ndecade'})
    states.boundary.plot(ax=plt.gca(), edgecolor='lightgray', linewidth=0.2)
    plt.gca().set_xlim(-2371000, 2278000)
    plt.gca().set_ylim(246000, 3186000)
    plt.gca().axis('off')
    plt.title(label="Percent change in population: 2020 to 2030")
    plt.tight_layout()
    plt.show()

    return


def read_county_shapefile():
    gdb = PART_3_a_INPUTS.replace('sqlite', 'gdb')
    f = 'counties_conus'
    gdf = gpd.read_file(filename=gdb, layer=f)
    gdf.rename(columns={'GEOID10': 'GEOID'}, inplace=True)
    gdf = gdf.to_crs("EPSG:5070")

    return gdf


def read_state_shapefile():
    gdb = PART_3_a_INPUTS.replace('sqlite', 'gdb')
    f = 'states_conus'
    gdf = gpd.read_file(filename=gdb, layer=f)
    gdf = gdf.to_crs("EPSG:5070")

    return gdf


def main():
    df = create_dataframe()
    create_map(df)


if __name__ == '__main__':
    main()
