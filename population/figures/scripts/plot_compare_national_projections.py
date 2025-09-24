import os

from matplotlib.artist import get
import pandas as pd
import seaborn as sns

from matplotlib import pyplot as plt

sns.set_theme(style='whitegrid')

BASE_FOLDER = 'D:\\projects\\ICLUS_v3\\population'
if os.path.isdir('D:\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'

CENSUS_CSV_PATH = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\Census')
POPULATION_DB = os.path.join(BASE_FOLDER, 'inputs', 'databases', 'population.sqlite')
WITTGENSTEIN_PATH = os.path.join(BASE_FOLDER, 'inputs', 'raw_files', 'Wittgenstein', 'v3')

POP2023 = 336806231  # from Census 2023 estimate


def get_census_2017_projections():
    data_dir = os.path.join(CENSUS_CSV_PATH, '2017\\projections\\total_population')
    df = None
    for scenario in ('0', 'high', 'low', 'mid'):
        fp = os.path.join(data_dir, f'np2017_d1_{scenario}.csv')
        result = (pd.read_csv(filepath_or_buffer=fp, usecols=['SEX', 'ORIGIN', 'RACE', 'YEAR', 'TOTAL_POP'])
                  .query('SEX == 0 & RACE == 0 & ORIGIN == 0')
                  .drop(columns=['SEX', 'ORIGIN', 'RACE'])
                  .rename(columns={'TOTAL_POP': f'np2017_d1_{scenario}'})
                  .set_index(keys='YEAR'))
        if df is None:
            df = result.copy()
        else:
            df = df.join(other=result)

    df = df.melt(var_name='Scenario', value_name='Total Population', ignore_index=False)
    df['Total Population'] = (df['Total Population'] / 1000000.0).round().astype(int)
    df['Data Source'] = 'U.S. Census (2017)'
    df['Scenario'] = df['Scenario'].map(arg={'np2017_d1_0': 'Zero immigration (Census)',
                                             'np2017_d1_low': 'Low immigration (Census)',
                                             'np2017_d1_mid': 'Medium immigration (Census)',
                                             'np2017_d1_high': 'High immigration (Census)'})
    df.reset_index(inplace=True)
    df.rename(columns={'YEAR': 'Year'}, inplace=True)

    return df


def get_census_2023_projections():
    data_dir = os.path.join(CENSUS_CSV_PATH, '2023\\projections\\total_population')
    df = None
    for scenario in ('zero', 'hi', 'low', 'mid'):
        fp = os.path.join(data_dir, f'np2023_d1_{scenario}.csv')
        result = (pd.read_csv(filepath_or_buffer=fp, usecols=['SEX', 'ORIGIN', 'RACE', 'YEAR', 'TOTAL_POP'])
                  .query('SEX == 0 & RACE == 0 & ORIGIN == 0')
                  .drop(columns=['SEX', 'ORIGIN', 'RACE'])
                  .rename(columns={'TOTAL_POP': f'np2023_d1_{scenario}'})
                  .set_index(keys='YEAR'))
        if df is None:
            df = result.copy()
        else:
            df = df.join(other=result)

    # harmonize all projections to the observed Census time series
    df = df.div(df.loc[[2023]].values, axis='columns').mul(POP2023)

    df = df.melt(var_name='Scenario', value_name='Total Population', ignore_index=False)
    df['Total Population'] = (df['Total Population'] / 1000000.0)
    df['Data Source'] = 'U.S. Census (2023)'
    df['Scenario'] = df['Scenario'].map(arg={'np2023_d1_zero': 'Zero immigration (Census)',
                                             'np2023_d1_low': 'Low immigration (Census)',
                                             'np2023_d1_mid': 'Medium immigration (Census)',
                                             'np2023_d1_hi': 'High immigration (Census)'})
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

    # harmonize all projections to the observed Census time series
    for scenario in df['Scenario'].unique():
        sub2020 = df.query('Year == 2020 & Scenario == @scenario')['Total Population'].values[0]
        sub2025 = df.query('Year == 2025 & Scenario == @scenario')['Total Population'].values[0]
        sub2023 = (sub2020 + sub2025) / 2
        df.loc[df['Scenario'] == scenario, 'Total Population'] = df['Total Population'].div(sub2023).mul(POP2023 / 1000000)
        df.loc[len(df)] = [scenario, 2023, POP2023 / 1000000, 'Wittgenstein', 'v3']

    return df


def get_historical_population():
    hist_2024 = get_historical_population_to_2024()
    hist_2020 = get_historical_population_to_2020()

    df = pd.concat(objs=[hist_2020, hist_2024], ignore_index=True)

    return df


def get_historical_population_to_2020():

    data_dir = os.path.join(CENSUS_CSV_PATH, '2020', 'intercensal')
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

def get_cbo_projections():
    folder = "D:\\OneDrive\\Data\\population_projections\\raw_files\\CBO\\CSV files"
    filename = 'censusThrough2020+CBOProjection_byYearAgeSex.csv'
    df = pd.read_csv(os.path.join(folder, filename))
    df.columns = ['Year', 'Age', 'Sex', 'Total Population']
    df = df[['Year', 'Total Population']].groupby(by='Year').sum().reset_index()
    df['Total Population'] = (df['Total Population'] / 1000000.0).round().astype(int)

    # harmonize all projections to the observed Census time series
    sub2023 = df.query('Year == 2023')['Total Population'].values[0]
    df['Total Population'] = df['Total Population'].div(sub2023).mul(POP2023 / 1000000)

    df['Data Source'] = 'CBO'
    return df


def main():
    obs = get_historical_population()
    census_projections = get_census_2023_projections().query('Year >= 2023 & Year <= 2050')
    witt_v3 = get_wittgenstein_v3_projections().query('Year >= 2023 & Year <= 2050')
    cbo = get_cbo_projections().query('Year >= 2023 & Year <= 2050')

    # plot Census lines in blue and shaded area between high and low
    for scenario in census_projections['Scenario'].unique():
        df = census_projections.query(f'Scenario == "{scenario}"')
        if scenario == 'Medium immigration (Census)':
            linewidth=2
            label='Census, Main Series (2023)'
        else:
            linewidth=0
            label = None
        sns.lineplot(x='Year',
                     y='Total Population',
                     data=df,
                     color='blue',
                     label=label,
                     linewidth=linewidth,
                     ax=plt.gca())

    line = plt.gca().get_lines()
    plt.fill_between(x=line[0].get_xdata(),
                     y1=line[0].get_ydata(),
                     y2=line[1].get_ydata(),
                     color='blue', alpha=0.2)

    # df = pd.concat(objs=[census_projections, witt_v3], ignore_index=True, verify_integrity=True)

    # sns.lineplot(data=df,
    #              x='Year',
    #              y='Total Population',
    #              hue='Data Source',
    #              style='Scenario',
    #              dashes=False,
    #              markers=True)
    #['1', '_', '|', 'x', '4', '+', '2'])

    # plot Wittgenstein line in red and shaded area between high and low
    for scenario in witt_v3['Scenario'].unique():
        df = witt_v3.query(f'Scenario == "{scenario}"')
        if scenario == 'SSP2':
            linewidth=2
            label='IPCC, SSP2 (2024)'
        else:
            linewidth=0
            label=None
        sns.lineplot(x='Year',
                    y='Total Population',
                    data=df,
                    color='red',
                    label=label,
                    linewidth=linewidth,
                    ax=plt.gca())

    line = plt.gca().get_lines()
    plt.fill_between(x=line[4].get_xdata(),
                     y1=line[8].get_ydata(),
                     y2=line[7].get_ydata(),
                     color='red', alpha=0.2)

    # plot CBO projection in green
    sns.lineplot(x='Year',
                 y='Total Population',
                 data=cbo,
                 color='gold',
                 linewidth=2,
                 label='CBO (2024)',
                 ax=plt.gca())

    sns.lineplot(x='YEAR',
                y='Total Population',
                data=obs,
                color='black',
                linewidth=2,
                label='Historical',
                ax=plt.gca())

    plt.xlim(left=2010, right=2050)
    plt.gca().set_ylabel('U.S. Population (millions)')
    plt.gca().xaxis.grid(False)
    # plt.gcf().set_size_inches((8, 5))
    plt.gca().get_legend().get_frame().set_alpha(1.0)
    # plt.tight_layout()

    plt.show()
    ...


if __name__ == '__main__':
    main()
