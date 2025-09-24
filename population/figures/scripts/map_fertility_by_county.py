import os
import sqlite3

import geopandas as gpd
import pandas as pd

from matplotlib import pyplot as plt


CDC_DATABASE = 'D:\\OneDrive\\ICLUS_v3\\population\\inputs\\databases\\cdc.sqlite'
GDB = 'D:\\OneDrive\\ICLUS_v3\\geospatial\\iclusv3_geospatial.gdb'


def create_dataframe():
    # fertility rates
    sql = 'SELECT * FROM fertility_2018_2022_county'
    con = sqlite3.connect(CDC_DATABASE)
    df = pd.read_sql(sql=sql, con=con)
    con.close()

    return df


def create_map(df):

    bins = (0.5, 1, 5, 10, 25, 100, 1000)
    labels = ('<0.5', '<1', '<5', '<10', '<25', '>=200')

    df['RATE_BINS'] = pd.cut(x=df['FERTILITY'],
                             bins=bins,
                             right=True,
                             labels=labels,
                             include_lowest=True)
    gdf = read_county_shapefile()
    # states = read_state_shapefile()
    gdf = gdf.merge(right=df, how='left', on='COFIPS')

    for age_group in gdf.AGE_GROUP.unique():
        for race in gdf.RACE.unique():
            result = gdf.query('AGE_GROUP == @age_group & RACE == @race')
            result.plot(column='RATE_BINS',
                        categorical=True,
                        cmap='Purples',
                        legend=True,
                        legend_kwds={'bbox_to_anchor': (1.15, 0.4),
                                     'facecolor': 'silver',
                                     'fancybox': True,
                                     'title': 'Per 1,000 women'},
                        missing_kwds={'color': 'black'})
            # states.boundary.plot(ax=plt.gca(), edgecolor='lightgray', linewidth=0.2)
            plt.gca().set_xlim(-2371000, 2278000)
            plt.gca().set_ylim(246000, 3186000)
            plt.gca().axis('off')
            plt.title(label=f"2018-2022 age-specific birth rate (ASBR):\n {race}, {age_group}")
            plt.tight_layout()
            # plt.show()

            outdir = 'D:\\OneDrive\\ICLUS_v3\\population\\figures\\fertility_maps'
            outfn = f'fertility_{age_group}_{race}.png'
            plt.savefig(os.path.join(outdir, outfn), dpi=300)

    return


def read_county_shapefile():
    f = 'County_DISSOLVE1'
    gdf = gpd.read_file(filename=GDB, layer=f)
    gdf.rename(columns={'NEW_FIPS': 'COFIPS'}, inplace=True)
    gdf = gdf.to_crs("EPSG:5070")

    return gdf


def read_state_shapefile():
    f = 'states_conus'
    gdf = gpd.read_file(filename=gdb, layer=f)
    gdf = gdf.to_crs("EPSG:5070")

    return gdf


def main():
    df = create_dataframe()
    create_map(df)


if __name__ == '__main__':
    main()
