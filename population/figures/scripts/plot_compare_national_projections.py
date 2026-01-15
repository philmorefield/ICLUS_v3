import os

from matplotlib import pyplot as plt
import pandas as pd
import seaborn as sns


sns.set_theme(style='whitegrid')

BASE_FOLDER = 'D:\\projects\\ICLUS_v3\\population'
if os.path.isdir('D:\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'

CENSUS_CSV_PATH = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\Census')
POPULATION_DB = os.path.join(BASE_FOLDER, 'inputs', 'databases', 'population.sqlite')
WITTGENSTEIN_PATH = os.path.join(BASE_FOLDER, 'inputs', 'raw_files', 'Wittgenstein', 'v3')

YEAR_MIN = 2010
YEAR_MAX = 2050

POP2023 = 336806231  # from Census 2023 estimate

POP2020 = 331577720  # from Census 2020 estimate


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
    df['Total Population'] = df['Total Population'] / 1000000.0
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
    fp = os.path.join(data_dir, 'intercensal\\co-est2024-alldata.csv')
    df = pd.read_csv(filepath_or_buffer=fp, usecols=usecols, encoding='latin1').query('SUMLEV == 40')
    df.drop(columns='SUMLEV', inplace=True)
    df = df.sum(axis=0).reset_index()
    df.rename(columns={'index': 'YEAR', 0: 'Total Population'}, inplace=True)
    df['YEAR'] = df['YEAR'].str.replace('POPESTIMATE', '').astype(int)
    df['Total Population'] /= 1000000

    df['Data Source'] = 'U.S. Census'

    return df


def get_cbo_projections():

    cols = ['AGE',
            'TOTAL_POPULATION',
            'BLANK1',
            'TOTAL_MALE',
            'TOTAL_MALE_SINGLE',
            'TOTAL_MALE_MARRIED',
            'TOTAL_MALE_WIDOWED',
            'TOTAL_MALE_DIVORCED',
            'BLANK2',
            'TOTAL_FEMALE',
            'TOTAL_FEMALE_SINGLE',
            'TOTAL_FEMALE_MARRIED',
            'TOTAL_FEMALE_WIDOWED',
            'TOTAL_FEMALE_DIVORCED']

    csv_folder = os.path.join(BASE_FOLDER, 'inputs', 'raw_files', 'CBO', '57059-2025-09-Demographic-Projections')
    csv_fn = '57059-2025-09-Demographic-Projections.xlsx'
    df = pd.read_excel(io=os.path.join(csv_folder, csv_fn),
                       sheet_name='2. Pop by age, sex, marital',
                       names=cols,
                       skiprows=9,
                       skipfooter=6).dropna(axis='columns', how='all').dropna()

    df = df[['AGE', 'TOTAL_POPULATION', 'TOTAL_MALE', 'TOTAL_FEMALE']].dropna()
    df = df.loc[df['AGE'] != 'Age']
    df['TOTAL_POPULATION'] = df['TOTAL_POPULATION'].astype(int)
    df['TOTAL_MALE'] = df['TOTAL_MALE'].astype(int)
    df['TOTAL_FEMALE'] = df ['TOTAL_FEMALE'].astype(int)

    n = 101
    df_list = [df[i:i+n] for i in range(0, df.shape[0], n)]

    i = 0
    for df in df_list:
        df['YEAR'] = 2022 + i
        i += 1

    df = pd.concat(df_list, ignore_index=True)

    df = df[['YEAR', 'TOTAL_POPULATION']].groupby(by='YEAR').sum().reset_index()
    df['TOTAL_POPULATION'] = (df['TOTAL_POPULATION'] / 1000000.0).round().astype(int)

    # harmonize all projections to the observed Census time series
    sub2023 = df.query('YEAR == 2023')['TOTAL_POPULATION'].values[0]
    df['TOTAL_POPULATION'] = df['TOTAL_POPULATION'].div(sub2023).mul(POP2023 / 1000000)

    df.columns = ['Year', 'Total Population']
    df['Data Source'] = 'CBO'

    return df


def get_uva_population_data():
    """Get UVA population data for comparison"""
    columns = ['STFIPS', 'NAME', 'SEX', 'POPULATION', '0_TO_4', '5_TO_9', '10_TO_14', '15_TO_19', '20_TO_24',
               '25_TO_29', '30_TO_34', '35_TO_39', '40_TO_44', '45_TO_49', '50_TO_54', '55_TO_59', '60_TO_64',
               '65_TO_69', '70_TO_74', '75_TO_79', '80_TO_84', '85_PLUS',]

    df = None
    for year in range(2020, 2060, 10):
        temp = pd.read_excel(io=os.path.join(BASE_FOLDER, 'inputs', 'raw_files', 'UVA', f'NationalProjections_ProjectedAgeSexDistribution_2030-2050.xlsx'),
                             sheet_name=f'{year}',
                             names=columns,
                             header=None,
                             skiprows=4,
                             skipfooter=1)
        temp['YEAR'] = year
        temp = temp.query('STFIPS == 0 & SEX == "Total"')[['POPULATION', 'YEAR']]
        if df is None:
            df = temp
        else:
            df = pd.concat([df, temp], ignore_index=True)

    df['POPULATION'] = df['POPULATION'] / 1000000

    # harmonize all projections to the observed Census time series
    sub2020 = df.query('YEAR == 2020')['POPULATION'].values[0]
    df['POPULATION'] = df['POPULATION'].div(sub2020).mul(POP2020 / 1000000)

    return df


def main():
    obs = get_historical_population()
    census_projections = get_census_2023_projections().query(f'Year >= 2023 & Year <= {YEAR_MAX}')
    witt_v3 = get_wittgenstein_v3_projections().query(f'Year >= 2023 & Year <= {YEAR_MAX}')
    cbo = get_cbo_projections().query(f'Year >= 2023 & Year <= {YEAR_MAX}')
    uva = get_uva_population_data().query(f'YEAR <= {YEAR_MAX}')

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

    # plot CBO projection in orange
    sns.lineplot(x='Year',
                 y='Total Population',
                 data=cbo,
                 color='orange',
                 linewidth=2,
                 label='CBO (Sept 2025)',
                 ax=plt.gca())

    # plot UVA projections in green
    sns.lineplot(x='YEAR',
                 y='POPULATION',
                 data=uva,
                 color='green',
                 linewidth=2,
                 label='UVA (2024)',
                 ax=plt.gca())

    # plot historical estimates in black
    sns.lineplot(x='YEAR',
                y='Total Population',
                data=obs,
                color='black',
                linewidth=2,
                label='Historical',
                ax=plt.gca())

    plt.xlim(left=YEAR_MIN, right=YEAR_MAX)
    plt.gca().set_ylabel('U.S. Population (millions)')
    plt.gca().xaxis.grid(False)
    plt.gca().get_legend().get_frame().set_alpha(1.0)
    plt.tight_layout()

    plt.show()


if __name__ == '__main__':
    main()
