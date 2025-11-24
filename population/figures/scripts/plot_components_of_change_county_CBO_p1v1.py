import datetime
import os
import sqlite3

import pandas as pd
import seaborn as sns

from matplotlib import pyplot as plt

BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'
if os.path.isdir('C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'C:\\Users\\philm\\OneDrive\\ICLUS_v3\\population'

CENSUS_CSV_PATH = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\Census')
POPULATION_DB = os.path.join(BASE_FOLDER, 'inputs', 'databases', 'population.sqlite')
PROJECTIONS_DB = os.path.join(BASE_FOLDER, 'outputs', 'CBO', 'p1v01.sqlite')

SCENARIO = 'CBO'
YEAR_MIN = 2010
YEAR_MAX = 2050
GEOID = '06037'  # Los Angeles County, CA


# def get_census_sya_population():
#     '''
#     2024 launch population is taken from U.S. Census Intercensal Population
#     Estimates.
#     '''
#     census_sya_input_folder = os.path.join(INPUT_FOLDER, 'raw_files', 'Census', '2024', 'intercensal', 'syasex')

#     df_list = None
#     for csv in os.listdir(census_sya_input_folder):
#         if csv.endswith('.csv'):
#             temp = pl.read_csv(source=os.path.join(census_sya_input_folder, csv),
#                                   encoding='latin1').filter(pl.col('YEAR') >= 2)
#             temp = temp.with_columns((pl.col('STATE').cast(pl.Utf8).str.zfill(2) +
#                         pl.col('COUNTY').cast(pl.Utf8).str.zfill(3))
#                         .alias('GEOID')).rename({'TOT_MALE': 'MALE', 'TOT_FEMALE': 'FEMALE'})
#             temp = temp.select(['GEOID', 'AGE', 'MALE', 'FEMALE'])
#             temp = temp.unpivot(index=['GEOID', 'AGE'], variable_name='SEX', value_name='POPULATION')

#             if df_list is None:
#                 df_list = [temp]
#             else:
#                 df_list.append(temp)
#     df = pl.concat(items=df_list, how='vertical')

#     df = df.sort(['GEOID', 'AGE', 'SEX'])
#     df = make_fips_changes(df)
#     assert df.shape == (538016, 4)

#     return df


def main():

    # cbo = get_cbo_population()
    # census = get_census_sya_population()

    fig = plt.figure(constrained_layout=True)
    gs = fig.add_gridspec(3, 2)

    ######################
    ## TOTAL POPULATION ##
    ######################

    ax_pop = fig.add_subplot(gs[0, :1])

    # historical population, 2010-2020
    csv_folder = os.path.join(CENSUS_CSV_PATH, '2020', 'intercensal')
    fips_csv = os.path.join(csv_folder, 'CC-EST2020-ALLDATA.csv')
    fips = pd.read_csv(filepath_or_buffer=fips_csv,
                       encoding='latin-1',
                       usecols=['STATE', 'COUNTY', 'STNAME', 'CTYNAME']).drop_duplicates()

    fips['GEOID'] = fips['STATE'].astype(str).str.zfill(2) + fips['COUNTY'].astype(str).str.zfill(3)


    csv_fn = 'co-est2020int-pop.xlsx'
    df = pd.read_excel(os.path.join(csv_folder, csv_fn),
                       skipfooter=6,
                       skiprows=5,
                       names=['COUNTY_STATE', '2010_base'] + [year for year in range(2010, 2021)])
    df[['CTYNAME', 'STNAME']] = df['COUNTY_STATE'].str.split(',', expand=True)
    df['CTYNAME'] = df['CTYNAME'].str.lstrip('.').str.strip()
    df['STNAME'] = df['STNAME'].str.strip()
    df = df.drop(columns=['2010_base', 'COUNTY_STATE'])
    df = df.merge(fips, on=['CTYNAME', 'STNAME'], how='left')
    df = df.query(f'GEOID == "{GEOID}"')
    county_name = df['CTYNAME'].values[0]
    state_name = df['STNAME'].values[0]

    pre2020pop = df.drop(columns=['STATE', 'COUNTY', 'CTYNAME', 'STNAME', 'GEOID']).T.reset_index()
    pre2020pop.columns = ['YEAR', 'POPULATION']
    pre2020pop['POPULATION'] = pre2020pop['POPULATION']

    # historical population, 2020-2024
    csv_folder = os.path.join(CENSUS_CSV_PATH, '2024', 'intercensal')
    csv_fn = 'co-est2024-alldata.csv'
    df = pd.read_csv(os.path.join(csv_folder, csv_fn), encoding='latin-1')
    df['GEOID'] = df['STATE'].astype(str).str.zfill(2) + df['COUNTY'].astype(str).str.zfill(3)
    df = df.query(f'GEOID == "{GEOID}"')
    columns = ['ESTIMATESBASE2020'] + [f'POPESTIMATE{year}' for year in range(2021, 2025)]
    post2020pop = df[columns]
    post2020pop = post2020pop.rename(columns={'ESTIMATESBASE2020': 'POPESTIMATE2020'})

    post2020pop = post2020pop.T.reset_index()
    post2020pop.columns = ['YEAR', 'POPULATION']
    post2020pop['YEAR'] = post2020pop['YEAR'].str[-4:].astype(int)
    post2020pop['POPULATION'] = post2020pop['POPULATION']

    histpop = pd.concat([pre2020pop, post2020pop], ignore_index=True)

    # future population
    query = f'SELECT * FROM population_by_age_sex_{SCENARIO}'
    con = sqlite3.connect(PROJECTIONS_DB)
    proj_pop = pd.read_sql_query(sql=query, con=con)
    con.close()

    proj_pop = proj_pop.query('GEOID == @GEOID').drop(columns=['SEX', 'AGE'])
    proj_pop = proj_pop.groupby(by='GEOID').sum().reset_index(drop=True).T.reset_index()
    proj_pop.columns = ['YEAR', 'POPULATION']
    proj_pop['YEAR'] = proj_pop['YEAR'].astype(int)
    proj_pop['POPULATION'] = proj_pop['POPULATION']

    sns.lineplot(x='YEAR', y='POPULATION', data=histpop, linewidth=2, color='gray', legend=False, ax=ax_pop, label='U.S. Census\n(intercensal estimate)')
    sns.lineplot(x='YEAR', y='POPULATION', data=proj_pop, linewidth=2, color='orange', legend=False, ax=ax_pop, label='P1v0 projection')

    plt.title('TOTAL POPULATION\n' + f'{county_name}, {state_name}')
    ax_pop.set_xticklabels([])
    plt.gca().set_xlabel("")
    plt.gca().set_ylabel("")
    plt.gca().set_xlim(xmin=YEAR_MIN, xmax=YEAR_MAX)
    fig.legend(bbox_to_anchor=(0.925, 0.925))

    ############
    ## BIRTHS ##
    ############

    # historical births
    columns = ['BIRTHS' + str(year) for year in range(2010, 2021)]
    ax_births = fig.add_subplot(gs[1, :1])
    csv = os.path.join(CENSUS_CSV_PATH, '2020\\intercensal\\co-est2020-alldata.csv')
    hist_births = pd.read_csv(csv, encoding='latin-1')
    hist_births['GEOID'] = hist_births['STATE'].astype(str).str.zfill(2) + hist_births['COUNTY'].astype(str).str.zfill(3)

    hist_births = hist_births.loc[hist_births.GEOID == GEOID, columns]
    hist_births = hist_births.T.reset_index()
    hist_births.columns = ['YEAR', 'BIRTHS']
    hist_births['YEAR'] = hist_births['YEAR'].str[-4:].astype(int)
    hist_births.loc[hist_births['YEAR'] == 2010, 'BIRTHS'] *= 4
    hist_births['BIRTHS'] = hist_births['BIRTHS']

    # historical births, 2020-2024
    columns = ['BIRTHS' + str(year) for year in range(2020, 2025)]
    csv = os.path.join(CENSUS_CSV_PATH, '2024\\intercensal\\co-est2024-alldata.csv')
    post2020_births = pd.read_csv(csv, encoding='latin-1')
    post2020_births['GEOID'] = post2020_births['STATE'].astype(str).str.zfill(2) + post2020_births['COUNTY'].astype(str).str.zfill(3)

    post2020_births = post2020_births.loc[post2020_births.GEOID == GEOID, columns]
    post2020_births = post2020_births.T.reset_index()
    post2020_births.columns = ['YEAR', 'BIRTHS']
    post2020_births['YEAR'] = post2020_births['YEAR'].str[-4:].astype(int)
    post2020_births.loc[post2020_births['YEAR'] == 2020, 'BIRTHS'] *= 4
    post2020_births['BIRTHS'] = post2020_births['BIRTHS']

    # future births
    query = f'SELECT * FROM births_by_age_sex_{SCENARIO}'
    con = sqlite3.connect(PROJECTIONS_DB)
    proj_births = pd.read_sql(sql=query, con=con)
    con.close()

    proj_births = proj_births.query('GEOID == @GEOID').drop(columns=['SEX', 'AGE'])
    proj_births = proj_births.groupby(by='GEOID').sum().reset_index(drop=True).T.reset_index()
    proj_births.columns = ['YEAR', 'BIRTHS']
    proj_births['YEAR'] = proj_births['YEAR'].astype(int)
    proj_births['BIRTHS'] = proj_births['BIRTHS']

    sns.lineplot(x='YEAR', y='BIRTHS', data=hist_births, linewidth=2, color='gray', legend=False, ax=ax_births)
    sns.lineplot(x='YEAR', y='BIRTHS', data=proj_births, linewidth=2, color='orange', legend=False, ax=ax_births)
    sns.lineplot(x='YEAR', y='BIRTHS', data=post2020_births, linewidth=2, color='gray', legend=False, ax=ax_births)

    plt.title('BIRTHS')
    ax_births.set_xticklabels([])
    ax_births.set_xlabel("")
    ax_births.set_ylabel("Millions")
    plt.gca().set_xlim(xmin=YEAR_MIN, xmax=YEAR_MAX)

    ############################
    ## NET DOMESTIC MIGRATION ##
    ############################

    # historical migration
    ax_migration = fig.add_subplot(gs[1, 1:])

    columns = ['DOMESTICMIG' + str(year) for year in range(2010, 2021)]
    csv = os.path.join(CENSUS_CSV_PATH, '2020\\intercensal\\co-est2020-alldata.csv')
    hist_migration = pd.read_csv(csv, encoding='latin-1')
    hist_migration['GEOID'] = hist_migration['STATE'].astype(str).str.zfill(2) + hist_migration['COUNTY'].astype(str).str.zfill(3)

    hist_migration = hist_migration.loc[hist_migration.GEOID == GEOID, columns]
    hist_migration = hist_migration.T.reset_index()
    hist_migration.columns = ['YEAR', 'MIGRATION']
    hist_migration['YEAR'] = hist_migration['YEAR'].str[-4:].astype(int)
    hist_migration.loc[hist_migration['YEAR'] == 2010, 'MIGRATION'] *= 4
    hist_migration['MIGRATION'] = hist_migration['MIGRATION']

    # historical migration, 2020-2024
    columns = ['DOMESTICMIG' + str(year) for year in range(2020, 2025)]
    csv = os.path.join(CENSUS_CSV_PATH, '2024\\intercensal\\co-est2024-alldata.csv')
    post2020_migration = pd.read_csv(csv, encoding='latin-1')
    post2020_migration['GEOID'] = post2020_migration['STATE'].astype(str).str.zfill(2) + post2020_migration['COUNTY'].astype(str).str.zfill(3)

    post2020_migration = post2020_migration.loc[post2020_migration.GEOID == GEOID, columns]
    post2020_migration = post2020_migration.T.reset_index()
    post2020_migration.columns = ['YEAR', 'MIGRATION']
    post2020_migration['YEAR'] = post2020_migration['YEAR'].str[-4:].astype(int)
    post2020_migration.loc[post2020_migration['YEAR'] == 2020, 'MIGRATION'] *= 4
    post2020_migration['MIGRATION'] = post2020_migration['MIGRATION']

    # future migration
    columns = (', ').join([f'NETMIG{year}' for year in range(2025, 2099)])
    columns = 'GEOID, AGE ,' + columns
    query = f'SELECT {columns} FROM migration_by_age_sex_{SCENARIO}'
    con = sqlite3.connect(PROJECTIONS_DB)
    proj_migration = pd.read_sql(sql=query, con=con)
    con.close()

    proj_migration = proj_migration.query('GEOID == @GEOID').drop(columns='AGE')
    proj_migration = proj_migration.groupby(by='GEOID').sum().reset_index(drop=True).T.reset_index()
    proj_migration.columns = ['YEAR', 'MIGRATION']
    proj_migration['YEAR'] = proj_migration['YEAR'].str[-4:].astype(int)

    sns.lineplot(x='YEAR', y='MIGRATION', data=hist_migration, linewidth=2, color='gray', legend=False, ax=ax_migration)
    sns.lineplot(x='YEAR', y='MIGRATION', data=proj_migration, linewidth=2, color='orange', legend=False, ax=ax_migration)
    sns.lineplot(x='YEAR', y='MIGRATION', data=post2020_migration, linewidth=2, color='gray', legend=False, ax=ax_migration)

    plt.title('MIGRATION')
    ax_migration.set_xticklabels([])
    ax_migration.set_xlabel("")
    ax_migration.set_ylabel("")
    plt.gca().set_xlim(xmin=YEAR_MIN, xmax=YEAR_MAX)

    ############
    ## DEATHS ##
    ############

    ax_deaths = fig.add_subplot(gs[2, :1])

    # historical deaths, 2010-2020
    columns = ['DEATHS' + str(year) for year in range(2010, 2021)]
    csv = os.path.join(CENSUS_CSV_PATH, '2020\\intercensal\\co-est2020-alldata.csv')
    hist_deaths = pd.read_csv(csv, encoding='latin-1')
    hist_deaths['GEOID'] = hist_deaths['STATE'].astype(str).str.zfill(2) + hist_deaths['COUNTY'].astype(str).str.zfill(3)

    hist_deaths = hist_deaths.loc[hist_deaths.GEOID == GEOID, columns]
    hist_deaths = hist_deaths.T.reset_index()
    hist_deaths.columns = ['YEAR', 'DEATHS']
    hist_deaths['YEAR'] = hist_deaths['YEAR'].str[-4:].astype(int)
    hist_deaths.loc[hist_deaths['YEAR'] == 2010, 'DEATHS'] *= 4
    hist_deaths['DEATHS'] = hist_deaths['DEATHS']

    # historical deaths, 2020-2024
    columns = ['SUMLEV'] + ['DEATHS' + str(year) for year in range(2020, 2025)]
    csv = os.path.join(CENSUS_CSV_PATH, '2024\\intercensal\\co-est2024-alldata.csv')
    post2020_deaths = pd.read_csv(csv, encoding='latin-1')
    post2020_deaths = post2020_deaths[columns]
    post2020_deaths = post2020_deaths.query('SUMLEV == 50')
    post2020_deaths = post2020_deaths.drop(columns='SUMLEV').sum().reset_index()
    post2020_deaths.columns = ['YEAR', 'DEATHS']
    post2020_deaths['YEAR'] = post2020_deaths['YEAR'].str[-4:].astype(int)
    post2020_deaths.loc[post2020_deaths['YEAR'] == 2020, 'DEATHS'] *= 4
    post2020_deaths['DEATHS'] = post2020_deaths['DEATHS']

    # future deaths
    query = f'SELECT * FROM deaths_by_age_sex_{SCENARIO}'
    con = sqlite3.connect(PROJECTIONS_DB)
    proj_deaths = pd.read_sql(sql=query, con=con)
    con.close()

    proj_deaths = proj_deaths.drop(columns=['GEOID', 'SEX', 'AGE']).sum().T.reset_index()
    proj_deaths.columns = ['YEAR', 'DEATHS']
    proj_deaths['YEAR'] = proj_deaths['YEAR'].astype(int)
    proj_deaths['DEATHS'] = proj_deaths['DEATHS']

    # CBO future deaths
    mort_csv_folder = os.path.join(BASE_FOLDER, 'inputs', 'raw_files', 'CBO', '57059-2025-09-Demographic-Projections', 'CSV files')
    mort_csv_fn = 'mortalityRates_byYearAgeSex.csv'
    mort_df = pd.read_csv(os.path.join(mort_csv_folder, mort_csv_fn))
    mort_df.columns = ['YEAR', 'AGE', 'SEX', 'MORTALITY_RATE_PER_K']
    mort_df = mort_df[mort_df['YEAR'] >= 2025].set_index(['YEAR', 'AGE', 'SEX'])
    mort_df = mort_df.rename(columns={'MORTALITY_RATE_PER_K': 'VALUE'})

    sns.lineplot(x='YEAR', y='DEATHS', data=hist_deaths, linewidth=2, color='gray', legend=False, ax=ax_deaths)
    sns.lineplot(x='YEAR', y='DEATHS', data=proj_deaths, linewidth=2, color='orange', legend=False, ax=ax_deaths)
    sns.lineplot(x='YEAR', y='DEATHS', data=post2020_deaths, linewidth=2, color='gray', legend=False, ax=ax_deaths)

    plt.title('DEATHS')
    ax_deaths.set_xlabel('')
    ax_deaths.set_ylabel('')
    plt.gca().set_xlim(xmin=YEAR_MIN, xmax=YEAR_MAX)

    #####################
    ## NET IMMIGRATION ##
    #####################

    ax_immig = fig.add_subplot(gs[2, 1:])

    # historical immigration, 2010-2020
    csv = os.path.join(CENSUS_CSV_PATH, '2020\\intercensal\\co-est2020-alldata.csv')
    hist_immig = pd.read_csv(csv, encoding='latin-1')
    columns = ['SUMLEV'] + ['INTERNATIONALMIG' + str(year) for year in range(2010, 2021)]
    hist_immig = hist_immig[columns]
    hist_immig = hist_immig.query('SUMLEV == 50')
    hist_immig = hist_immig.drop(columns='SUMLEV').sum().reset_index()
    hist_immig.columns = ['YEAR', 'IMMIGRATION']
    hist_immig['YEAR'] = hist_immig['YEAR'].str[-4:].astype(int)
    hist_immig.loc[hist_immig['YEAR'] == 2010, 'IMMIGRATION'] *= 4
    hist_immig['IMMIGRATION'] = hist_immig['IMMIGRATION']

    # historical immigration, 2020-2024
    columns = ['SUMLEV'] + ['INTERNATIONALMIG' + str(year) for year in range(2020, 2025)]
    csv = os.path.join(CENSUS_CSV_PATH, '2024\\intercensal\\co-est2024-alldata.csv')
    post2020_immig = pd.read_csv(csv, encoding='latin-1')
    post2020_immig = post2020_immig[columns]
    post2020_immig = post2020_immig.query('SUMLEV == 50')
    post2020_immig = post2020_immig.drop(columns='SUMLEV').sum().reset_index()
    post2020_immig.columns = ['YEAR', 'IMMIGRATION']
    post2020_immig['YEAR'] = post2020_immig['YEAR'].str[-4:].astype(int)
    post2020_immig.loc[post2020_immig['YEAR'] == 2020, 'IMMIGRATION'] *= 4
    post2020_immig['IMMIGRATION'] = post2020_immig['IMMIGRATION']

    # future immigration
    query = f'SELECT * FROM immigration_by_age_sex_{SCENARIO}'
    con = sqlite3.connect(PROJECTIONS_DB)
    proj_immig = pd.read_sql(sql=query, con=con)
    con.close()

    proj_immig = proj_immig.drop(columns=['GEOID', 'SEX', 'AGE']).sum().T.reset_index()
    proj_immig.columns = ['YEAR', 'IMMIGRATION']
    proj_immig['YEAR'] = proj_immig['YEAR'].astype(int)
    proj_immig['IMMIGRATION'] = proj_immig['IMMIGRATION']

    sns.lineplot(x='YEAR', y='IMMIGRATION', data=hist_immig, linewidth=2, color='gray', legend=False, ax=ax_immig)
    sns.lineplot(x='YEAR', y='IMMIGRATION', data=proj_immig, linewidth=2, color='orange', legend=False, ax=ax_immig)
    sns.lineplot(x='YEAR', y='IMMIGRATION', data=post2020_immig, linewidth=2, color='gray', legend=False, ax=ax_immig)

    plt.title('IMMIGRATION')
    ax_immig.set_xlabel("")
    ax_immig.set_ylabel("")
    plt.gca().set_xlim(xmin=YEAR_MIN, xmax=YEAR_MAX)

    # plt.tight_layout()
    month = datetime.date.today().month
    day = datetime.date.today().day
    year = datetime.date.today().year
    plt.figtext(x=0.85, y=0.95, s=f'Created: {month}/{day}/{year}', size=5)
    plt.show()

    return


if __name__ == '__main__':
    main()
