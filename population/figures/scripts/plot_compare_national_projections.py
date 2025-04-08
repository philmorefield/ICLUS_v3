import os

import pandas as pd
import seaborn as sns

from matplotlib import pyplot as plt

BASE_FOLDER = 'D:\\projects\\ICLUS_v3\\population'
if os.path.isdir('D:\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'

CENSUS_CSV_PATH = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\Census')
POPULATION_DB = os.path.join(BASE_FOLDER, 'inputs', 'databases', 'population.sqlite')
WITTGENSTEIN_PATH = os.path.join(BASE_FOLDER, 'inputs', 'raw_files', 'Wittgenstein', 'v3')

POP2020 = 331.526933


def get_census_2017_projections():
    data_dir = os.path.join(CENSUS_CSV_PATH, '2017\\projections')
    df = None
    for scenario in ('0', 'high', 'low', 'mid'):
        fp = os.path.join(data_dir, f'np2017_d4_{scenario}.csv')
        result = (pd.read_csv(filepath_or_buffer=fp, usecols=['RACE_HISP', 'SEX', 'YEAR', 'TOTAL_NIM'])
                  .query('RACE_HISP == 0 & SEX == 0')
                  .drop(columns=['RACE_HISP', 'SEX'])
                  .rename(columns={'TOTAL_NIM': f'np2017_d4_{scenario}'})
                  .set_index(keys='YEAR'))
        if df is None:
            df = result.copy()
        else:
            df = df.join(other=result)

    df = df.melt(var_name='Scenario', value_name='Total Population', ignore_index=False)
    # df['Total Population'] = (df['Total Population'] / 1000000.0).round().astype(int)
    df['Data Source'] = 'U.S. Census (2017)'
    df['Scenario'] = df['Scenario'].map(arg={'np2017_d4_0': 'Zero immigration (Census)',
                                             'np2017_d4_low': 'Low immigration (Census)',
                                             'np2017_d4_mid': 'Medium immigration (Census)',
                                             'np2017_d4_high': 'High immigration (Census)'})
    df.reset_index(inplace=True)
    df.rename(columns={'YEAR': 'Year'}, inplace=True)

    return df


def get_wittgenstein_v3_projections():
    fp = os.path.join(WITTGENSTEIN_PATH, 'wcde_total_population_united_states.csv')
    df = pd.read_csv(filepath_or_buffer=fp,
                     skiprows=8,
                     usecols=['Scenario', 'Year', 'Population'])
    df.sort_values(by=['Scenario', 'Year'], inplace=True, ignore_index=True)
    df['Population'] /= 1000.0
    df.rename(columns={'Population': 'Total Population'}, inplace=True)
    df['Data Source'] = 'Wittgenstein'
    df['Version'] = 'v3'

    return df


def get_historical_population():
    hist_2024 = get_historical_population_to_2024()
    hist_2020 = get_historical_population_to_2020()

    df = pd.concat(objs=[hist_2020, hist_2024], ignore_index=True)

    return df


def get_historical_population_to_2020():

    data_dir = os.path.join(CENSUS_CSV_PATH, '2020')
    usecols = ['SUMLEV'] + [f'POPESTIMATE{year}' for year in range(2010, 2021)]
    fp = os.path.join(data_dir, 'co-est2020-alldata.csv')
    df = pd.read_csv(filepath_or_buffer=fp, usecols=usecols, encoding='latin1').query('SUMLEV == 40')
    df.drop(columns='SUMLEV', inplace=True)
    df = df.sum(axis=0).reset_index()
    df.rename(columns={'index': 'YEAR', 0: 'Total Population'}, inplace=True)
    df['YEAR'] = df['YEAR'].str.replace('POPESTIMATE', '').astype(int)
    df['Total Population'] /= 1000000

    df['Data Source'] = 'U.S. Census'

    return df



def get_historical_population_to_2024():
    data_dir = os.path.join(CENSUS_CSV_PATH, '2024')
    usecols = ['SUMLEV'] + [f'POPESTIMATE{year}' for year in range(2020, 2024)]
    fp = os.path.join(data_dir, 'co-est2024-alldata.csv')
    df = pd.read_csv(filepath_or_buffer=fp, usecols=usecols, encoding='latin1').query('SUMLEV == 40')
    df.drop(columns='SUMLEV', inplace=True)
    df = df.sum(axis=0).reset_index()
    df.rename(columns={'index': 'YEAR', 0: 'Total Population'}, inplace=True)
    df['YEAR'] = df['YEAR'].str.replace('POPESTIMATE', '').astype(int)
    df['Total Population'] /= 1000000

    df['Data Source'] = 'U.S. Census'

    return df


def main():
    obs = get_historical_population()
    census_projections = get_census_2017_projections()
    witt_v3 = get_wittgenstein_v3_projections()

    df = pd.concat(objs=[census_projections, witt_v3], ignore_index=True, verify_integrity=True)

    sns.lineplot(data=df,
                 x='Year',
                 y='Total Population',
                 hue='Data Source',
                 style='Scenario',
                 dashes=False,
                 markers=True)
    #['1', '_', '|', 'x', '4', '+', '2'])

    sns.lineplot(x='YEAR',
                 y='Total Population',
                 data=obs,
                 color='black',
                 label='Historical',
                 ax=plt.gca())
    plt.gca().set_ylabel('U.S. Population (millions)')
    plt.gca().get_legend().set_bbox_to_anchor((1.01, 0.75))
    plt.gcf().set_size_inches((8, 5))

    plt.tight_layout()
    plt.show()
    ...


if __name__ == '__main__':
    main()
