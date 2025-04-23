import os
import sqlite3

import pandas as pd
import seaborn as sns

from matplotlib import pyplot as plt

BASE_FOLDER = 'D:\\projects\\ICLUS_v3\\population'
if os.path.isdir('D:\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'

CENSUS_CSV_PATH = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\Census')
POPULATION_DB = os.path.join(BASE_FOLDER, 'inputs', 'databases', 'population.sqlite')
PROJECTIONS_DB = os.path.join(BASE_FOLDER, 'outputs', 'wittgenstein_v3_202549211314.sqlite')

SCENARIO = 'SSP3'


def main():
    fig = plt.figure(constrained_layout=True)
    gs = fig.add_gridspec(3, 2)

    ######################
    ## TOTAL POPULATION ##
    ######################

    ax_pop = fig.add_subplot(gs[0, :])

    # historical population
    con = sqlite3.connect(POPULATION_DB)
    sql = 'SELECT YEAR, RACE, POPULATION FROM county_population_ageracegender_2010_to_2020'
    hist_pop = pd.read_sql_query(sql=sql, con=con)
    con.close()
    hist_pop = hist_pop.groupby(by=['YEAR', 'RACE'], as_index=False).sum()
    hist_pop = hist_pop.pivot(columns='RACE', index='YEAR', values='POPULATION').reset_index()
    hist_pop.columns.name = None

    # future population
    query = f'SELECT * FROM population_by_race_sex_age_{SCENARIO}'
    con = sqlite3.connect(PROJECTIONS_DB)
    proj_pop = pd.read_sql_query(sql=query, con=con)
    con.close()

    proj_pop = proj_pop.drop(columns=['GEOID', 'AGE_GROUP', 'SEX'])
    proj_pop = proj_pop.groupby(by='RACE', as_index=False).sum()
    proj_pop = proj_pop.set_index('RACE').T
    proj_pop = proj_pop.reset_index()
    proj_pop = proj_pop.rename(columns={'index': 'YEAR'})
    proj_pop.columns.name = None

    df = pd.concat([hist_pop, proj_pop], axis=0, ignore_index=True)
    df['YEAR'] = df['YEAR'].astype(int)
    df.plot.area(x='YEAR', ax=ax_pop)
    plt.axvline(x=2020, color='black', linestyle='--')

    plt.title(f'TOTAL U.S. POPULATION: {SCENARIO}')
    plt.gca().set_xlabel("")
    plt.gca().set_ylabel('')
    xmin = df.YEAR.astype(int).min()
    xmax = df.YEAR.astype(int).max()
    plt.gca().set_xlim(xmin=xmin, xmax=xmax)
    plt.gca().legend(bbox_to_anchor=(1.05, 1.10), reverse=True)
    # plt.gca().get_legend().set_bbox_to_anchor((1.05, 1.10))
    # plt.gca().get_legend().reversed = True

    ############
    ## BIRTHS ##
    ############

    # historical births
    columns = ['SUMLEV'] + ['BIRTHS' + str(year) for year in range(2010, 2021)]
    ax_births = fig.add_subplot(gs[1, :1])
    csv = os.path.join(CENSUS_CSV_PATH, '2020\\co-est2020-alldata.csv')
    hist_births = pd.read_csv(csv, encoding='latin-1')

    hist_births = hist_births[columns]
    hist_births = hist_births.query('SUMLEV == 50')
    hist_births = hist_births.drop(columns='SUMLEV').sum().reset_index()
    hist_births.columns = ['YEAR', 'BIRTHS']
    hist_births['YEAR'] = hist_births['YEAR'].str[-4:].astype(int)
    hist_births.loc[hist_births['YEAR'] == 2010, 'BIRTHS'] *= 4

    # historical births, 2020-2024
    columns = ['SUMLEV'] + ['BIRTHS' + str(year) for year in range(2020, 2025)]
    csv = os.path.join(CENSUS_CSV_PATH, '2024\\co-est2024-alldata.csv')
    post2020_births = pd.read_csv(csv, encoding='latin-1')
    post2020_births = post2020_births[columns]
    post2020_births = post2020_births.query('SUMLEV == 50')
    post2020_births = post2020_births.drop(columns='SUMLEV').sum().reset_index()
    post2020_births.columns = ['YEAR', 'BIRTHS']
    post2020_births['YEAR'] = post2020_births['YEAR'].str[-4:].astype(int)
    post2020_births.loc[post2020_births['YEAR'] == 2020, 'BIRTHS'] *= 4

    # future births
    query = f'SELECT * FROM births_by_race_sex_age_{SCENARIO}'
    con = sqlite3.connect(PROJECTIONS_DB)
    proj_births = pd.read_sql(sql=query, con=con)
    con.close()

    proj_births = proj_births.drop(columns=['GEOID', 'RACE', 'SEX', 'AGE_GROUP']).sum().T.reset_index()
    proj_births.columns = ['YEAR', 'BIRTHS']
    proj_births['YEAR'] = proj_births['YEAR'].astype(int)

    sns.lineplot(x='YEAR', y='BIRTHS', data=hist_births, color='gray', legend=False, ax=ax_births)
    sns.lineplot(x='YEAR', y='BIRTHS', data=proj_births, color='orange', legend=False, ax=ax_births)
    sns.lineplot(x='YEAR', y='BIRTHS', data=post2020_births, color='black', legend=False, ax=ax_births)

    plt.title('BIRTHS')
    ax_births.set_xticklabels([])
    ax_births.set_xlabel('')
    ax_births.set_ylabel('')

    ############################
    ## NET DOMESTIC MIGRATION ##
    ############################

    # historical migration
    ax_migration = fig.add_subplot(gs[1, 1:])

    columns = ['SUMLEV'] + ['DOMESTICMIG' + str(year) for year in range(2010, 2021)]
    csv = os.path.join(CENSUS_CSV_PATH, '2020\\co-est2020-alldata.csv')
    hist_migration = pd.read_csv(csv, encoding='latin-1')

    hist_migration = hist_migration[columns]
    hist_migration = hist_migration.query('SUMLEV == 50').clip(lower=0)
    hist_migration = hist_migration.drop(columns='SUMLEV').sum().reset_index()
    hist_migration.columns = ['YEAR', 'MIGRATION']
    hist_migration['YEAR'] = hist_migration['YEAR'].str[-4:].astype(int)
    hist_migration.loc[hist_migration['YEAR'] == 2010, 'MIGRATION'] *= 4

    # historical migration, 2020-2024
    columns = ['SUMLEV'] + ['DOMESTICMIG' + str(year) for year in range(2020, 2025)]
    csv = os.path.join(CENSUS_CSV_PATH, '2024\\co-est2024-alldata.csv')
    post2020_migration = pd.read_csv(csv, encoding='latin-1')
    post2020_migration = post2020_migration[columns]
    post2020_migration = post2020_migration.query('SUMLEV == 50').clip(lower=0)
    post2020_migration = post2020_migration.drop(columns='SUMLEV').sum().reset_index()
    post2020_migration.columns = ['YEAR', 'MIGRATION']
    post2020_migration['YEAR'] = post2020_migration['YEAR'].str[-4:].astype(int)
    post2020_migration.loc[post2020_migration['YEAR'] == 2020, 'MIGRATION'] *= 4

    # future births
    query = f'SELECT * FROM migration_by_race_sex_age_{SCENARIO}'
    con = sqlite3.connect(PROJECTIONS_DB)
    proj_migration = pd.read_sql(sql=query, con=con)
    con.close()

    proj_migration = proj_migration.drop(columns=['GEOID', 'RACE', 'SEX', 'AGE_GROUP'])
    proj_migration = proj_migration.clip(lower=0).sum().T.reset_index()
    proj_migration.columns = ['YEAR', 'MIGRATION']
    proj_migration['YEAR'] = proj_migration['YEAR'].astype(int)

    sns.lineplot(x='YEAR', y='MIGRATION', data=hist_migration, color='gray', legend=False, ax=ax_migration)
    sns.lineplot(x='YEAR', y='MIGRATION', data=proj_migration, color='orange', legend=False, ax=ax_migration)
    sns.lineplot(x='YEAR', y='MIGRATION', data=post2020_migration, color='black', legend=False, ax=ax_migration)

    plt.title('MIGRATION')
    ax_migration.set_xticklabels([])
    ax_migration.set_xlabel('')
    ax_migration.set_ylabel('')

    ############
    ## DEATHS ##
    ############

    ax_deaths = fig.add_subplot(gs[2, :1])

    # historical deaths, 2010-2020
    columns = ['SUMLEV'] + ['DEATHS' + str(year) for year in range(2010, 2021)]
    csv = os.path.join(CENSUS_CSV_PATH, '2020\\co-est2020-alldata.csv')
    hist_deaths = pd.read_csv(csv, encoding='latin-1')
    hist_deaths = hist_deaths[columns]
    hist_deaths = hist_deaths.query('SUMLEV == 50')
    hist_deaths = hist_deaths.drop(columns='SUMLEV').sum().reset_index()
    hist_deaths.columns = ['YEAR', 'DEATHS']
    hist_deaths['YEAR'] = hist_deaths['YEAR'].str[-4:].astype(int)
    hist_deaths.loc[hist_deaths['YEAR'] == 2010, 'DEATHS'] *= 4

    # historical deaths, 2020-2024
    columns = ['SUMLEV'] + ['DEATHS' + str(year) for year in range(2020, 2025)]
    csv = os.path.join(CENSUS_CSV_PATH, '2024\\co-est2024-alldata.csv')
    post2020_deaths = pd.read_csv(csv, encoding='latin-1')
    post2020_deaths = post2020_deaths[columns]
    post2020_deaths = post2020_deaths.query('SUMLEV == 50')
    post2020_deaths = post2020_deaths.drop(columns='SUMLEV').sum().reset_index()
    post2020_deaths.columns = ['YEAR', 'DEATHS']
    post2020_deaths['YEAR'] = post2020_deaths['YEAR'].str[-4:].astype(int)
    post2020_deaths.loc[post2020_deaths['YEAR'] == 2020, 'DEATHS'] *= 4

    # future deaths
    query = f'SELECT * FROM deaths_by_race_sex_age_{SCENARIO}'
    con = sqlite3.connect(PROJECTIONS_DB)
    proj_deaths = pd.read_sql(sql=query, con=con)
    con.close()

    proj_deaths = proj_deaths.drop(columns=['GEOID', 'RACE', 'SEX', 'AGE_GROUP']).sum().T.reset_index()
    proj_deaths.columns = ['YEAR', 'DEATHS']
    proj_deaths['YEAR'] = proj_deaths['YEAR'].astype(int)

    sns.lineplot(x='YEAR', y='DEATHS', data=hist_deaths, color='gray', legend=False, ax=ax_deaths)
    sns.lineplot(x='YEAR', y='DEATHS', data=proj_deaths, color='orange', legend=False, ax=ax_deaths)
    sns.lineplot(x='YEAR', y='DEATHS', data=post2020_deaths, color='black', legend=False, ax=ax_deaths)

    plt.title('DEATHS')
    ax_deaths.set_xlabel('')
    ax_deaths.set_ylabel('')

    #####################
    ## NET IMMIGRATION ##
    #####################

    ax_immig = fig.add_subplot(gs[2, 1:])

    # historical immigration, 2010-2020
    csv = os.path.join(CENSUS_CSV_PATH, '2020\\co-est2020-alldata.csv')
    hist_immig = pd.read_csv(csv, encoding='latin-1')
    columns = ['SUMLEV'] + ['INTERNATIONALMIG' + str(year) for year in range(2010, 2021)]
    hist_immig = hist_immig[columns]
    hist_immig = hist_immig.query('SUMLEV == 50')
    hist_immig = hist_immig.drop(columns='SUMLEV').sum().reset_index()
    hist_immig.columns = ['YEAR', 'IMMIGRATION']
    hist_immig['YEAR'] = hist_immig['YEAR'].str[-4:].astype(int)
    hist_immig.loc[hist_immig['YEAR'] == 2010, 'IMMIGRATION'] *= 4

    # historical immigration, 2020-2024
    columns = ['SUMLEV'] + ['INTERNATIONALMIG' + str(year) for year in range(2020, 2025)]
    csv = os.path.join(CENSUS_CSV_PATH, '2024\\co-est2024-alldata.csv')
    post2020_immig = pd.read_csv(csv, encoding='latin-1')
    post2020_immig = post2020_immig[columns]
    post2020_immig = post2020_immig.query('SUMLEV == 50')
    post2020_immig = post2020_immig.drop(columns='SUMLEV').sum().reset_index()
    post2020_immig.columns = ['YEAR', 'IMMIGRATION']
    post2020_immig['YEAR'] = post2020_immig['YEAR'].str[-4:].astype(int)
    post2020_immig.loc[post2020_immig['YEAR'] == 2020, 'IMMIGRATION'] *= 4

    # future immigration
    query = f'SELECT * FROM immigration_by_race_sex_age_{SCENARIO}'
    con = sqlite3.connect(PROJECTIONS_DB)
    proj_immig = pd.read_sql(sql=query, con=con)
    con.close()

    proj_immig = proj_immig.drop(columns=['GEOID', 'RACE', 'SEX', 'AGE_GROUP']).sum().T.reset_index()
    proj_immig.columns = ['YEAR', 'IMMIGRATION']
    proj_immig['YEAR'] = proj_immig['YEAR'].astype(int)

    sns.lineplot(x='YEAR', y='IMMIGRATION', data=hist_immig, color='gray', legend=False, ax=ax_immig)
    sns.lineplot(x='YEAR', y='IMMIGRATION', data=proj_immig, color='orange', legend=False, ax=ax_immig)
    sns.lineplot(x='YEAR', y='IMMIGRATION', data=post2020_immig, color='black', legend=False, ax=ax_immig)

    plt.title('IMMIGRATION')
    ax_immig.set_xlabel('')
    ax_immig.set_ylabel('')

    plt.tight_layout()
    plt.show()

    return


if __name__ == '__main__':
    main()
