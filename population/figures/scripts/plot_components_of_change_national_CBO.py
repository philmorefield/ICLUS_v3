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
PROJECTIONS_DB = os.path.join(BASE_FOLDER, 'outputs', 'CBO', 'phase1_v0.sqlite')

SCENARIO = 'CBO'

YEAR_MIN = 2015
YEAR_MAX = 2050

def main():
    fig = plt.figure(constrained_layout=True)
    gs = fig.add_gridspec(3, 2)

    ######################
    ## TOTAL POPULATION ##
    ######################

    ax_pop = fig.add_subplot(gs[0, :1])

    # historical population, 2010-2020
    csv_folder = os.path.join(CENSUS_CSV_PATH, '2020', 'intercensal')
    csv_fn = 'co-est2020int-pop.xlsx'
    df = pd.read_excel(os.path.join(csv_folder, csv_fn),
                       skipfooter=6,
                       skiprows=5,
                       names=['COUNTY_STATE', '2010_base'] + [year for year in range(2010, 2021)])
    pre2020pop = df.drop(columns=['COUNTY_STATE', '2010_base']).sum().T.reset_index()
    pre2020pop.columns = ['YEAR', 'POPULATION']
    pre2020pop['POPULATION'] = pre2020pop['POPULATION'] / 1000000

    # historical population, 2020-2024
    csv_folder = os.path.join(CENSUS_CSV_PATH, '2024', 'intercensal')
    csv_fn = 'co-est2024-alldata.csv'
    df = pd.read_csv(os.path.join(csv_folder, csv_fn), encoding='latin-1')
    columns = ['SUMLEV', 'ESTIMATESBASE2020'] + [f'POPESTIMATE{year}' for year in range(2020, 2025)]
    post2020pop = df[columns].rename(columns={'ESTIMATESBASE2020': 'POPESTIMATE2020'})
    post2020pop = post2020pop.query('SUMLEV == 40').drop(columns='SUMLEV').sum().reset_index()
    post2020pop.columns = ['YEAR', 'POPULATION']
    post2020pop['YEAR'] = post2020pop['YEAR'].str[-4:].astype(int)
    post2020pop['POPULATION'] = post2020pop['POPULATION'] / 1000000

    histpop = pd.concat([pre2020pop, post2020pop], ignore_index=True)


    # future population
    query = f'SELECT * FROM population_by_age_sex_{SCENARIO}'
    con = sqlite3.connect(PROJECTIONS_DB)
    proj_pop = pd.read_sql_query(sql=query, con=con)
    con.close()

    proj_pop = proj_pop.drop(columns=['GEOID', 'AGE_GROUP', 'SEX']).sum()
    proj_pop = proj_pop.reset_index()
    proj_pop.columns = ['YEAR', 'POPULATION']
    proj_pop['YEAR'] = proj_pop['YEAR'].astype(int)
    proj_pop['POPULATION'] = proj_pop['POPULATION'] / 1000000

    # CBO future population
    csv_folder = os.path.join(BASE_FOLDER, 'inputs', 'raw_files', 'CBO', 'demographic_projections_2025_9', 'CSV files')
    csv_fn = 'censusThrough2020+CBOProjection_byYearAgeSex.csv'
    cbo = pd.read_csv(os.path.join(csv_folder, csv_fn))
    cbo.columns = ['YEAR', 'AGE', 'SEX', 'POPULATION']
    cbo = cbo.drop(columns=['AGE', 'SEX'])
    cbo = cbo.groupby(by='YEAR', as_index=False).sum()
    cbo = cbo[cbo['YEAR'] >= 2025]
    cbo['POPULATION'] = cbo['POPULATION'] / 1000000

    sns.lineplot(x='YEAR', y='POPULATION', data=pre2020pop, linewidth=2, color='gray', legend=False, ax=ax_pop, label='U.S. Census\n(intercensal estimate)')
    sns.lineplot(x='YEAR', y='POPULATION', data=post2020pop, linewidth=2, color='gray', legend=False, ax=ax_pop)
    sns.lineplot(x='YEAR', y='POPULATION', data=proj_pop, linewidth=2, color='orange', legend=False, ax=ax_pop, label='P1v0 projection')
    sns.lineplot(x='YEAR', y='POPULATION', data=cbo, linewidth=2, color='purple', legend=False, ax=ax_pop, label='CBO projection')

    plt.title('U.S. POPULATION')
    ax_pop.set_xticklabels([])
    plt.gca().set_xlabel("")
    plt.gca().set_ylabel("")
    plt.gca().set_xlim(xmin=YEAR_MIN, xmax=YEAR_MAX)
    fig.legend(bbox_to_anchor=(0.925, 0.925))

    ############
    ## BIRTHS ##
    ############

    # historical births
    columns = ['SUMLEV'] + ['BIRTHS' + str(year) for year in range(2010, 2021)]
    ax_births = fig.add_subplot(gs[1, :1])
    csv = os.path.join(CENSUS_CSV_PATH, '2020\\intercensal\\co-est2020-alldata.csv')
    hist_births = pd.read_csv(csv, encoding='latin-1')

    hist_births = hist_births[columns]
    hist_births = hist_births.query('SUMLEV == 50')
    hist_births = hist_births.drop(columns='SUMLEV').sum().reset_index()
    hist_births.columns = ['YEAR', 'BIRTHS']
    hist_births['YEAR'] = hist_births['YEAR'].str[-4:].astype(int)
    hist_births.loc[hist_births['YEAR'] == 2010, 'BIRTHS'] *= 4
    hist_births['BIRTHS'] = hist_births['BIRTHS'] / 1000000

    # historical births, 2020-2024
    columns = ['SUMLEV'] + ['BIRTHS' + str(year) for year in range(2020, 2025)]
    csv = os.path.join(CENSUS_CSV_PATH, '2024\\intercensal\\co-est2024-alldata.csv')
    post2020_births = pd.read_csv(csv, encoding='latin-1')
    post2020_births = post2020_births[columns]
    post2020_births = post2020_births.query('SUMLEV == 50')
    post2020_births = post2020_births.drop(columns='SUMLEV').sum().reset_index()
    post2020_births.columns = ['YEAR', 'BIRTHS']
    post2020_births['YEAR'] = post2020_births['YEAR'].str[-4:].astype(int)
    post2020_births.loc[post2020_births['YEAR'] == 2020, 'BIRTHS'] *= 4
    post2020_births['BIRTHS'] = post2020_births['BIRTHS'] / 1000000

    # future births
    query = f'SELECT * FROM births_by_age_sex_{SCENARIO}'
    con = sqlite3.connect(PROJECTIONS_DB)
    proj_births = pd.read_sql(sql=query, con=con)
    con.close()

    proj_births = proj_births.drop(columns=['GEOID', 'SEX', 'AGE_GROUP']).sum().T.reset_index()
    proj_births.columns = ['YEAR', 'BIRTHS']
    proj_births['YEAR'] = proj_births['YEAR'].astype(int)
    proj_births['BIRTHS'] = proj_births['BIRTHS'] / 1000000

    # CBO future births
    fert_csv_folder = os.path.join(BASE_FOLDER, 'inputs', 'raw_files', 'CBO', 'demographic_projections_2025_9', 'CSV files')
    fert_csv_fn = 'fertilityRates_byYearAgePlace.csv'
    fert_df = pd.read_csv(os.path.join(fert_csv_folder, fert_csv_fn))
    fert_df.columns = ['YEAR', 'AGE', 'PLACE', 'FERTILITY_RATE_PER_K']
    fert_df = fert_df.query('PLACE == "all"').drop(columns='PLACE')
    fert_df = fert_df[fert_df['YEAR'] >= 2025].set_index(['YEAR', 'AGE'])
    fert_df = fert_df.rename(columns={'FERTILITY_RATE_PER_K': 'VALUE'})

    pop_csv_folder = os.path.join(BASE_FOLDER, 'inputs', 'raw_files', 'CBO', 'demographic_projections_2025_9', 'CSV files')
    pop_csv_fn = 'censusThrough2020+CBOProjection_byYearAgeSex.csv'
    pop_df = pd.read_csv(os.path.join(pop_csv_folder, pop_csv_fn))
    pop_df.columns = ['YEAR', 'AGE', 'SEX', 'POPULATION']
    pop_df.AGE = pop_df.AGE.str.replace('+', '').astype(int)
    pop_df = pop_df.query('YEAR >= 2025 & AGE >= 14 & AGE <= 49 & SEX == "female"').drop(columns='SEX')
    pop_df = pop_df.set_index(['YEAR', 'AGE'])
    pop_df = pop_df.rename(columns={'POPULATION': 'VALUE'})

    cbo_births = pop_df.mul(fert_df, axis=0).div(1000).reset_index().drop(columns='AGE')
    cbo_births = cbo_births.groupby(by='YEAR', as_index=False).sum()
    cbo_births = cbo_births.rename(columns={'VALUE': 'BIRTHS'})
    cbo_births['BIRTHS'] = cbo_births['BIRTHS'] / 1000000

    sns.lineplot(x='YEAR', y='BIRTHS', data=hist_births, linewidth=2, color='gray', legend=False, ax=ax_births)
    sns.lineplot(x='YEAR', y='BIRTHS', data=proj_births, linewidth=2, color='orange', legend=False, ax=ax_births)
    sns.lineplot(x='YEAR', y='BIRTHS', data=post2020_births, linewidth=2, color='gray', legend=False, ax=ax_births)
    sns.lineplot(x='YEAR', y='BIRTHS', data=cbo_births, linewidth=2, color='purple', legend=False, ax=ax_births)

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

    columns = ['SUMLEV'] + ['DOMESTICMIG' + str(year) for year in range(2010, 2021)]
    csv = os.path.join(CENSUS_CSV_PATH, '2020\\intercensal\\co-est2020-alldata.csv')
    hist_migration = pd.read_csv(csv, encoding='latin-1')

    hist_migration = hist_migration[columns]
    hist_migration = hist_migration.query('SUMLEV == 50').clip(lower=0)
    hist_migration = hist_migration.drop(columns='SUMLEV').sum().reset_index()
    hist_migration.columns = ['YEAR', 'MIGRATION']
    hist_migration['YEAR'] = hist_migration['YEAR'].str[-4:].astype(int)
    hist_migration.loc[hist_migration['YEAR'] == 2010, 'MIGRATION'] *= 4
    hist_migration['MIGRATION'] = hist_migration['MIGRATION'] / 1000000

    # historical migration, 2020-2024
    columns = ['SUMLEV'] + ['DOMESTICMIG' + str(year) for year in range(2020, 2025)]
    csv = os.path.join(CENSUS_CSV_PATH, '2024\\intercensal\\co-est2024-alldata.csv')
    post2020_migration = pd.read_csv(csv, encoding='latin-1')
    post2020_migration = post2020_migration[columns]
    post2020_migration = post2020_migration.query('SUMLEV == 50').clip(lower=0)
    post2020_migration = post2020_migration.drop(columns='SUMLEV').sum().reset_index()
    post2020_migration.columns = ['YEAR', 'MIGRATION']
    post2020_migration['YEAR'] = post2020_migration['YEAR'].str[-4:].astype(int)
    post2020_migration.loc[post2020_migration['YEAR'] == 2020, 'MIGRATION'] *= 4
    post2020_migration['MIGRATION'] = post2020_migration['MIGRATION'] / 1000000

    # future migration
    columns = (', ').join([f'NETMIG{year}' for year in range(2025, 2099)])
    columns = 'GEOID, AGE_GROUP ,' + columns
    query = f'SELECT {columns} FROM migration_by_age_sex_{SCENARIO}'
    con = sqlite3.connect(PROJECTIONS_DB)
    proj_migration = pd.read_sql(sql=query, con=con)
    con.close()

    proj_migration.columns = [col.replace('NETMIG', '') for col in proj_migration.columns]
    proj_migration = proj_migration.drop(columns=['GEOID', 'AGE_GROUP'])
    proj_migration = proj_migration.clip(lower=0).sum().T.reset_index()
    proj_migration.columns = ['YEAR', 'MIGRATION']
    proj_migration['YEAR'] = proj_migration['YEAR'].astype(int)
    proj_migration['MIGRATION'] = proj_migration['MIGRATION'] / 1000000

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
    columns = ['SUMLEV'] + ['DEATHS' + str(year) for year in range(2010, 2021)]
    csv = os.path.join(CENSUS_CSV_PATH, '2020\\intercensal\\co-est2020-alldata.csv')
    hist_deaths = pd.read_csv(csv, encoding='latin-1')
    hist_deaths = hist_deaths[columns]
    hist_deaths = hist_deaths.query('SUMLEV == 50')
    hist_deaths = hist_deaths.drop(columns='SUMLEV').sum().reset_index()
    hist_deaths.columns = ['YEAR', 'DEATHS']
    hist_deaths['YEAR'] = hist_deaths['YEAR'].str[-4:].astype(int)
    hist_deaths.loc[hist_deaths['YEAR'] == 2010, 'DEATHS'] *= 4
    hist_deaths['DEATHS'] = hist_deaths['DEATHS'] / 1000000

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
    post2020_deaths['DEATHS'] = post2020_deaths['DEATHS'] / 1000000

    # future deaths
    query = f'SELECT * FROM deaths_by_age_sex_{SCENARIO}'
    con = sqlite3.connect(PROJECTIONS_DB)
    proj_deaths = pd.read_sql(sql=query, con=con)
    con.close()

    proj_deaths = proj_deaths.drop(columns=['GEOID', 'SEX', 'AGE_GROUP']).sum().T.reset_index()
    proj_deaths.columns = ['YEAR', 'DEATHS']
    proj_deaths['YEAR'] = proj_deaths['YEAR'].astype(int)
    proj_deaths['DEATHS'] = proj_deaths['DEATHS'] / 1000000

    # CBO future deaths
    mort_csv_folder = os.path.join(BASE_FOLDER, 'inputs', 'raw_files', 'CBO', 'demographic_projections_2025_9', 'CSV files')
    mort_csv_fn = 'mortalityRates_byYearAgeSex.csv'
    mort_df = pd.read_csv(os.path.join(mort_csv_folder, mort_csv_fn))
    mort_df.columns = ['YEAR', 'AGE', 'SEX', 'MORTALITY_RATE_PER_K']
    mort_df = mort_df[mort_df['YEAR'] >= 2025].set_index(['YEAR', 'AGE', 'SEX'])
    mort_df = mort_df.rename(columns={'MORTALITY_RATE_PER_K': 'VALUE'})

    pop_csv_folder = os.path.join(BASE_FOLDER, 'inputs', 'raw_files', 'CBO', 'demographic_projections_2025_9', 'CSV files')
    pop_csv_fn = 'censusThrough2020+CBOProjection_byYearAgeSex.csv'
    pop_df = pd.read_csv(os.path.join(pop_csv_folder, pop_csv_fn))
    pop_df.columns = ['YEAR', 'AGE', 'SEX', 'POPULATION']
    pop_df.AGE = pop_df.AGE.str.replace('+', '').astype(int)
    pop_df = pop_df.query('YEAR >= 2025')
    pop_df = pop_df.set_index(['YEAR', 'AGE', 'SEX'])
    pop_df = pop_df.rename(columns={'POPULATION': 'VALUE'})

    cbo_deaths = pop_df.mul(mort_df, axis=0).div(1000).reset_index().drop(columns=['AGE', 'SEX'])
    cbo_deaths = cbo_deaths.groupby(by='YEAR', as_index=False).sum()
    cbo_deaths = cbo_deaths.rename(columns={'VALUE': 'DEATHS'})
    cbo_deaths['DEATHS'] = cbo_deaths['DEATHS'] / 1000000

    sns.lineplot(x='YEAR', y='DEATHS', data=hist_deaths, linewidth=2, color='gray', legend=False, ax=ax_deaths)
    sns.lineplot(x='YEAR', y='DEATHS', data=proj_deaths, linewidth=2, color='orange', legend=False, ax=ax_deaths)
    sns.lineplot(x='YEAR', y='DEATHS', data=post2020_deaths, linewidth=2, color='gray', legend=False, ax=ax_deaths)
    sns.lineplot(x='YEAR', y='DEATHS', data=cbo_deaths, linewidth=2, color='purple', legend=False, ax=ax_deaths)

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
    hist_immig['IMMIGRATION'] = hist_immig['IMMIGRATION'] / 1000000

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
    post2020_immig['IMMIGRATION'] = post2020_immig['IMMIGRATION'] / 1000000

    # future immigration
    query = f'SELECT * FROM immigration_by_age_sex_{SCENARIO}'
    con = sqlite3.connect(PROJECTIONS_DB)
    proj_immig = pd.read_sql(sql=query, con=con)
    con.close()

    proj_immig = proj_immig.drop(columns=['GEOID', 'SEX', 'AGE_GROUP']).sum().T.reset_index()
    proj_immig.columns = ['YEAR', 'IMMIGRATION']
    proj_immig['YEAR'] = proj_immig['YEAR'].astype(int)
    proj_immig['IMMIGRATION'] = proj_immig['IMMIGRATION'] / 1000000

    sns.lineplot(x='YEAR', y='IMMIGRATION', data=hist_immig, linewidth=2, color='gray', legend=False, ax=ax_immig)
    sns.lineplot(x='YEAR', y='IMMIGRATION', data=proj_immig, linewidth=2, color='orange', legend=False, ax=ax_immig)
    sns.lineplot(x='YEAR', y='IMMIGRATION', data=post2020_immig, linewidth=2, color='gray', legend=False, ax=ax_immig)

    plt.title('IMMIGRATION')
    ax_immig.set_xlabel("")
    ax_immig.set_ylabel("")
    plt.gca().set_xlim(xmin=YEAR_MIN, xmax=YEAR_MAX)

    # plt.tight_layout()
    plt.show()

    return


if __name__ == '__main__':
    main()
