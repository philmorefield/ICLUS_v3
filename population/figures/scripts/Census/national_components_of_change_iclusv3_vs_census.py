import os
import sqlite3

from numpy import int64
import pandas as pd

from matplotlib import pyplot as plt

BASE_FOLDER = 'D:\\projects\\ICLUS_v3\\population'
if os.path.isdir('D:\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'

CENSUS_CSV_PATH = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\Census')
POPULATION_DB = os.path.join(BASE_FOLDER, 'inputs', 'databases', 'population.sqlite')
SCENARIO = 'mid'

# # this run had erroneously large immigration values for 2023 and 2024
# PROJECTIONS_DB = os.path.join(BASE_FOLDER, 'outputs', 'iclus_v3_census_202551163547.sqlite')

# PROJECTIONS_DB = os.path.join(BASE_FOLDER, 'outputs', 'iclus_v3_census_202553173742.sqlite')
# CDC_FERT = 'No adj'
# CDC_MORT = 'No adj'

# PROJECTIONS_DB = os.path.join(BASE_FOLDER, 'outputs', 'iclus_v3_census_20255416653.sqlite')
# CDC_FERT = '-4.5%'
# CDC_MORT = '-15%'

# PROJECTIONS_DB = os.path.join(BASE_FOLDER, 'outputs', 'iclus_v3_census_202555143420.sqlite')
# CDC_FERT = '-5.5%'
# CDC_MORT = '-15%'

SUPTITLE = f"ICLUS v3 vs Census\nScenario: {SCENARIO.upper()} | CDC Fert: {CDC_FERT} | CDC Mort: {CDC_MORT}\n{os.path.basename(PROJECTIONS_DB)}"



class FigureMaker():
    def __init__(self):
        self.fig = plt.figure(figsize=(6.5, 6.5), constrained_layout=True)
        self.gs = self.fig.add_gridspec(3, 2)

        # total population
        self.get_historical_total_population()
        self.get_iclus_v3_projected_total_population()
        self.get_census_projected_total_population()
        self.plot_total_population()

        # births
        self.get_historical_births()
        self.get_iclus_v3_projected_births()
        self.get_census_projected_births()
        self.plot_births()

        # deaths
        self.get_historical_deaths()
        self.get_iclus_v3_projected_deaths()
        self.get_census_projected_deaths()
        self.plot_deaths()

        # domestic migration
        self.get_historical_migration()
        self.get_iclus_v3_projected_migration()
        self.plot_migration()

        # immigration
        self.get_historical_immigration()
        self.get_iclus_v3_projected_immigration()
        self.get_census_projected_immigration()
        self.plot_immigration()

        self.fig.suptitle(SUPTITLE)
        plt.tight_layout()
        self.ax_births.legend(bbox_to_anchor=(1.9, 2.25))
        plt.show()

    def get_historical_total_population(self):
        con = sqlite3.connect(POPULATION_DB)
        sql = 'SELECT YEAR, POPULATION FROM county_population_ageracegender_2010_to_2020'
        self.hist_pop = pd.read_sql_query(sql=sql, con=con)
        con.close()

        self.hist_pop = self.hist_pop.groupby(by='YEAR', as_index=False).sum()
        self.hist_pop['POPULATION'] /= 1000000

    def get_iclus_v3_projected_total_population(self):
        query = f'SELECT * FROM population_by_race_sex_age_{SCENARIO}'
        con = sqlite3.connect(PROJECTIONS_DB)
        self.iclusv3_pop = pd.read_sql_query(sql=query, con=con)
        con.close()

        self.iclusv3_pop = self.iclusv3_pop.melt(id_vars=['GEOID', 'AGE_GROUP', 'RACE', 'SEX'],
                                                 var_name='YEAR',
                                                 value_name='POPULATION')
        self.iclusv3_pop = self.iclusv3_pop[['YEAR', 'POPULATION']].groupby(by='YEAR', as_index=False).sum()
        self.iclusv3_pop['YEAR'] = self.iclusv3_pop['YEAR'].astype(int64)
        self.iclusv3_pop['POPULATION'] = self.iclusv3_pop['POPULATION'].astype(int64)
        self.iclusv3_pop['POPULATION'] /= 1000000

    def get_census_projected_total_population(self):
        csv = os.path.join(CENSUS_CSV_PATH, f'2023\\projections\\total_population\\np2023_d1_{SCENARIO}.csv')

        self.census_pop = pd.read_csv(csv, encoding='latin-1')
        self.census_pop = self.census_pop.query('SEX == 0 & ORIGIN == 0 & RACE == 0')
        self.census_pop = self.census_pop[['YEAR', 'TOTAL_POP']]
        self.census_pop.columns = ['YEAR', 'POPULATION']
        self.census_pop['POPULATION'] /= 1000000

    def plot_total_population(self):
        self.ax_pop = self.fig.add_subplot(self.gs[0, :1])
        self.hist_pop.plot(x='YEAR', y='POPULATION', color='black', label='Historical', ax=self.ax_pop)
        self.iclusv3_pop.plot(x='YEAR', y='POPULATION', color='orange', label='ICLUS v3', ax=self.ax_pop)
        self.census_pop.plot(x='YEAR', y='POPULATION', color='blue', label='Census', ax=self.ax_pop)

        plt.title('TOTAL POPULATION')
        plt.gca().set_xlabel("")
        plt.gca().set_ylabel("")
        plt.gca().set_xlim(xmin=2010, xmax=2100)

        self.ax_pop.get_legend().remove()

    def get_historical_births(self):
        # historical births, 2010-2020
        csv = os.path.join(CENSUS_CSV_PATH, '2020\\co-est2020-alldata.csv')
        self.hist_births_one = pd.read_csv(csv, encoding='latin-1')
        columns = ['SUMLEV'] + ['BIRTHS' + str(year) for year in range(2010, 2021)]
        self.hist_births_one = self.hist_births_one[columns]
        self.hist_births_one = self.hist_births_one.query('SUMLEV == 50')
        self.hist_births_one = self.hist_births_one.drop(columns='SUMLEV').sum().reset_index()
        self.hist_births_one.columns = ['YEAR', 'BIRTHS']
        self.hist_births_one['YEAR'] = self.hist_births_one['YEAR'].str[-4:].astype(int)
        self.hist_births_one.loc[self.hist_births_one['YEAR'] == 2010, 'BIRTHS'] *= 4
        self.hist_births_one['BIRTHS'] /= 1000000

        # historical births, 2020-2024
        csv = os.path.join(CENSUS_CSV_PATH, '2024\\co-est2024-alldata.csv')
        self.hist_births_two = pd.read_csv(csv, encoding='latin-1')
        columns = ['SUMLEV'] + ['BIRTHS' + str(year) for year in range(2020, 2025)]
        self.hist_births_two = self.hist_births_two[columns]
        self.hist_births_two = self.hist_births_two.query('SUMLEV == 50')
        self.hist_births_two = self.hist_births_two.drop(columns='SUMLEV').sum().reset_index()
        self.hist_births_two.columns = ['YEAR', 'BIRTHS']
        self.hist_births_two['YEAR'] = self.hist_births_two['YEAR'].str[-4:].astype(int)
        self.hist_births_two.loc[self.hist_births_two['YEAR'] == 2020, 'BIRTHS'] *= 4
        self.hist_births_two['BIRTHS'] /= 1000000

    def get_iclus_v3_projected_births(self):
        query = f'SELECT * FROM births_by_race_sex_age_{SCENARIO}'
        con = sqlite3.connect(PROJECTIONS_DB)
        self.iclusv3_births = pd.read_sql_query(sql=query, con=con)
        con.close()

        self.iclusv3_births = self.iclusv3_births.melt(id_vars=['GEOID', 'AGE_GROUP', 'RACE', 'SEX'],
                                                 var_name='YEAR',
                                                 value_name='BIRTHS')
        self.iclusv3_births = self.iclusv3_births[['YEAR', 'BIRTHS']].groupby(by='YEAR', as_index=False).sum()
        self.iclusv3_births['YEAR'] = self.iclusv3_births['YEAR'].astype(int64)
        self.iclusv3_births['BIRTHS'] = self.iclusv3_births['BIRTHS'].astype(int64)
        self.iclusv3_births['BIRTHS'] /= 1000000

    def get_census_projected_births(self):
        csv = os.path.join(CENSUS_CSV_PATH, f'2023\\projections\\fertility\\np2023_d2_{SCENARIO}.csv')

        self.census_births = pd.read_csv(csv, encoding='latin-1')
        self.census_births = self.census_births.query('RACE_HISP == 0 & SEX == 0')
        self.census_births = self.census_births[['YEAR', 'BIRTHS']]
        self.census_births.columns = ['YEAR', 'BIRTHS']
        self.census_births['BIRTHS'] /= 1000000

    def plot_births(self):
        self.ax_births = self.fig.add_subplot(self.gs[1, :1])
        self.hist_births_one.plot(x='YEAR', y='BIRTHS', color='gray', label='Historical', ax=self.ax_births)
        self.hist_births_two.plot(x='YEAR', y='BIRTHS', color='black', label='Historical', ax=self.ax_births)
        self.iclusv3_births.plot(x='YEAR', y='BIRTHS', color='orange', label='ICLUS v3', ax=self.ax_births)
        self.census_births.plot(x='YEAR', y='BIRTHS', color='blue', label='Census', ax=self.ax_births)

        plt.title('BIRTHS')
        plt.gca().set_xlabel("")
        plt.gca().set_ylabel("Millions")
        plt.gca().set_xlim(xmin=2010, xmax=2100)

    def get_historical_deaths(self):
        # historical deaths, 2010-2020
        csv = os.path.join(CENSUS_CSV_PATH, '2020\\co-est2020-alldata.csv')
        self.hist_deaths_one = pd.read_csv(csv, encoding='latin-1')
        columns = ['SUMLEV'] + ['DEATHS' + str(year) for year in range(2010, 2021)]
        self.hist_deaths_one = self.hist_deaths_one[columns]
        self.hist_deaths_one = self.hist_deaths_one.query('SUMLEV == 50')
        self.hist_deaths_one = self.hist_deaths_one.drop(columns='SUMLEV').sum().reset_index()
        self.hist_deaths_one.columns = ['YEAR', 'DEATHS']
        self.hist_deaths_one['YEAR'] = self.hist_deaths_one['YEAR'].str[-4:].astype(int)
        self.hist_deaths_one.loc[self.hist_deaths_one['YEAR'] == 2010, 'DEATHS'] *= 4
        self.hist_deaths_one['DEATHS'] /= 1000000

        # historical deaths, 2020-2024
        csv = os.path.join(CENSUS_CSV_PATH, '2024\\co-est2024-alldata.csv')
        self.hist_deaths_two = pd.read_csv(csv, encoding='latin-1')
        columns = ['SUMLEV'] + ['DEATHS' + str(year) for year in range(2020, 2025)]
        self.hist_deaths_two = self.hist_deaths_two[columns]
        self.hist_deaths_two = self.hist_deaths_two.query('SUMLEV == 50')
        self.hist_deaths_two = self.hist_deaths_two.drop(columns='SUMLEV').sum().reset_index()
        self.hist_deaths_two.columns = ['YEAR', 'DEATHS']
        self.hist_deaths_two['YEAR'] = self.hist_deaths_two['YEAR'].str[-4:].astype(int)
        self.hist_deaths_two.loc[self.hist_deaths_two['YEAR'] == 2020, 'DEATHS'] *= 4
        self.hist_deaths_two['DEATHS'] /= 1000000

    def get_iclus_v3_projected_deaths(self):
        query = f'SELECT * FROM deaths_by_race_sex_age_{SCENARIO}'
        con = sqlite3.connect(PROJECTIONS_DB)
        self.iclusv3_deaths = pd.read_sql_query(sql=query, con=con)
        con.close()

        self.iclusv3_deaths = self.iclusv3_deaths.melt(id_vars=['GEOID', 'AGE_GROUP', 'RACE', 'SEX'],
                                                 var_name='YEAR',
                                                 value_name='DEATHS')
        self.iclusv3_deaths = self.iclusv3_deaths[['YEAR', 'DEATHS']].groupby(by='YEAR', as_index=False).sum()
        self.iclusv3_deaths['YEAR'] = self.iclusv3_deaths['YEAR'].astype(int64)
        self.iclusv3_deaths['DEATHS'] = self.iclusv3_deaths['DEATHS'].astype(int64)
        self.iclusv3_deaths['DEATHS'] /= 1000000

    def get_census_projected_deaths(self):
        csv = os.path.join(CENSUS_CSV_PATH, f'2023\\projections\\mortality\\np2023_d3_{SCENARIO}.csv')

        self.census_deaths = pd.read_csv(csv, encoding='latin-1')
        self.census_deaths = self.census_deaths.query('RACE_HISP == 0 & SEX == 0')
        self.census_deaths = self.census_deaths[['YEAR', 'TOTAL_DEATHS']]
        self.census_deaths.columns = ['YEAR', 'DEATHS']
        self.census_deaths['DEATHS'] /= 1000000

    def plot_deaths(self):
        self.ax_deaths = self.fig.add_subplot(self.gs[2, :1])
        self.hist_deaths_one.plot(x='YEAR', y='DEATHS', color='gray', label='Historical', ax=self.ax_deaths)
        self.hist_deaths_two.plot(x='YEAR', y='DEATHS', color='black', label='Historical', ax=self.ax_deaths)
        self.iclusv3_deaths.plot(x='YEAR', y='DEATHS', color='orange', label='ICLUS v3', ax=self.ax_deaths)
        self.census_deaths.plot(x='YEAR', y='DEATHS', color='blue', label='Census', ax=self.ax_deaths)

        self.ax_deaths.get_legend().remove()

        plt.title('DEATHS')
        plt.gca().set_xlabel("")
        plt.gca().set_ylabel("")
        plt.gca().set_xlim(xmin=2010, xmax=2100)
        # plt.gca().legend(bbox_to_anchor=(1.05, 1.10), reverse=True)

    def get_historical_migration(self):
        # historical migration, 2010-2020
        csv = os.path.join(CENSUS_CSV_PATH, '2020\\co-est2020-alldata.csv')
        self.hist_migration_one = pd.read_csv(csv, encoding='latin-1')
        columns = ['SUMLEV'] + ['NETMIG' + str(year) for year in range(2010, 2021)]
        self.hist_migration_one = self.hist_migration_one[columns]
        self.hist_migration_one = self.hist_migration_one.query('SUMLEV == 50')
        self.hist_migration_one = self.hist_migration_one.drop(columns='SUMLEV').sum().reset_index()
        self.hist_migration_one.columns = ['YEAR', 'MIGRATION']
        self.hist_migration_one['YEAR'] = self.hist_migration_one['YEAR'].str[-4:].astype(int)
        self.hist_migration_one.loc[self.hist_migration_one['YEAR'] == 2010, 'MIGRATION'] *= 4
        self.hist_migration_one['MIGRATION'] /= 1000000

        # historical migration, 2020-2024
        csv = os.path.join(CENSUS_CSV_PATH, '2024\\co-est2024-alldata.csv')
        self.hist_migration_two = pd.read_csv(csv, encoding='latin-1')
        columns = ['SUMLEV'] + ['NETMIG' + str(year) for year in range(2020, 2025)]
        self.hist_migration_two = self.hist_migration_two[columns]
        self.hist_migration_two = self.hist_migration_two.query('SUMLEV == 50')
        self.hist_migration_two = self.hist_migration_two.drop(columns='SUMLEV').sum().reset_index()
        self.hist_migration_two.columns = ['YEAR', 'MIGRATION']
        self.hist_migration_two['YEAR'] = self.hist_migration_two['YEAR'].str[-4:].astype(int)
        self.hist_migration_two.loc[self.hist_migration_two['YEAR'] == 2020, 'MIGRATION'] *= 4
        self.hist_migration_two['MIGRATION'] /= 1000000

    def get_iclus_v3_projected_migration(self):
        query = f'SELECT * FROM migration_by_race_sex_age_{SCENARIO}'
        con = sqlite3.connect(PROJECTIONS_DB)
        self.iclusv3_migration = pd.read_sql_query(sql=query, con=con)
        con.close()

        self.iclusv3_migration = self.iclusv3_migration.melt(id_vars=['GEOID', 'AGE_GROUP', 'RACE', 'SEX'],
                                                             var_name='YEAR',
                                                             value_name='MIGRATION')
        self.iclusv3_migration = self.iclusv3_migration[['YEAR', 'MIGRATION']].groupby(by='YEAR', as_index=False).sum()
        self.iclusv3_migration['YEAR'] = self.iclusv3_migration['YEAR'].astype(int64)
        self.iclusv3_migration['MIGRATION'] = self.iclusv3_migration['MIGRATION'].astype(int64)
        self.iclusv3_migration['MIGRATION'] /= 1000000

    def plot_migration(self):
        self.ax_migration = self.fig.add_subplot(self.gs[1, 1:])
        self.hist_migration_one.plot(x='YEAR', y='MIGRATION', color='gray', label='Historical', ax=self.ax_migration)
        self.hist_migration_two.plot(x='YEAR', y='MIGRATION', color='black', label='Historical', ax=self.ax_migration)
        self.iclusv3_migration.plot(x='YEAR', y='MIGRATION', color='orange', label='ICLUS v3', ax=self.ax_migration)

        self.ax_migration.get_legend().remove()

        plt.title('MIGRATION')
        plt.gca().set_xlabel("")
        plt.gca().set_ylabel("")
        plt.gca().set_xlim(xmin=2010, xmax=2100)
        # plt.gca().legend(bbox_to_anchor=(1.05, 1.10), reverse=True)

    def get_historical_immigration(self):
        # historical immigration, 2010-2020
        csv = os.path.join(CENSUS_CSV_PATH, '2020\\co-est2020-alldata.csv')
        self.hist_immigration_one = pd.read_csv(csv, encoding='latin-1')
        columns = ['SUMLEV'] + ['INTERNATIONALMIG' + str(year) for year in range(2010, 2021)]
        self.hist_immigration_one = self.hist_immigration_one[columns]
        self.hist_immigration_one = self.hist_immigration_one.query('SUMLEV == 50')
        self.hist_immigration_one = self.hist_immigration_one.drop(columns='SUMLEV').sum().reset_index()
        self.hist_immigration_one.columns = ['YEAR', 'IMMIGRATION']
        self.hist_immigration_one['YEAR'] = self.hist_immigration_one['YEAR'].str[-4:].astype(int)
        self.hist_immigration_one.loc[self.hist_immigration_one['YEAR'] == 2010, 'IMMIGRATION'] *= 4
        self.hist_immigration_one['IMMIGRATION'] /= 1000000

        # historical immigration, 2020-2024
        csv = os.path.join(CENSUS_CSV_PATH, '2024\\co-est2024-alldata.csv')
        self.hist_immigration_two = pd.read_csv(csv, encoding='latin-1')
        columns = ['SUMLEV'] + ['INTERNATIONALMIG' + str(year) for year in range(2020, 2025)]
        self.hist_immigration_two = self.hist_immigration_two[columns]
        self.hist_immigration_two = self.hist_immigration_two.query('SUMLEV == 50')
        self.hist_immigration_two = self.hist_immigration_two.drop(columns='SUMLEV').sum().reset_index()
        self.hist_immigration_two.columns = ['YEAR', 'IMMIGRATION']
        self.hist_immigration_two['YEAR'] = self.hist_immigration_two['YEAR'].str[-4:].astype(int)
        self.hist_immigration_two.loc[self.hist_immigration_two['YEAR'] == 2020, 'IMMIGRATION'] *= 4
        self.hist_immigration_two['IMMIGRATION'] /= 1000000

    def get_iclus_v3_projected_immigration(self):
        query = f'SELECT * FROM immigration_by_race_sex_age_{SCENARIO}'
        con = sqlite3.connect(PROJECTIONS_DB)
        self.iclusv3_immigration = pd.read_sql_query(sql=query, con=con)
        con.close()

        self.iclusv3_immigration = self.iclusv3_immigration.melt(id_vars=['GEOID', 'AGE_GROUP', 'RACE', 'SEX'],
                                                 var_name='YEAR',
                                                 value_name='IMMIGRATION')
        self.iclusv3_immigration = self.iclusv3_immigration[['YEAR', 'IMMIGRATION']].groupby(by='YEAR', as_index=False).sum()
        self.iclusv3_immigration['YEAR'] = self.iclusv3_immigration['YEAR'].astype(int64)
        self.iclusv3_immigration['IMMIGRATION'] = self.iclusv3_immigration['IMMIGRATION'].astype(int64)
        self.iclusv3_immigration['IMMIGRATION'] /= 1000000

    def get_census_projected_immigration(self):
        csv = os.path.join(CENSUS_CSV_PATH, f'2023\\projections\\immigration\\np2023_d4_{SCENARIO}.csv')

        self.census_immigration = pd.read_csv(csv, encoding='latin-1')
        self.census_immigration = self.census_immigration.query('RACE_HISP == 0 & SEX == 0')
        self.census_immigration = self.census_immigration[['YEAR', 'TOTAL_NIM']]
        self.census_immigration.columns = ['YEAR', 'IMMIGRATION']
        self.census_immigration['IMMIGRATION'] /= 1000000

    def plot_immigration(self):
        self.ax_immigration = self.fig.add_subplot(self.gs[2, 1:])
        self.hist_immigration_one.plot(x='YEAR', y='IMMIGRATION', color='gray', label='Historical', ax=self.ax_immigration)
        self.hist_immigration_two.plot(x='YEAR', y='IMMIGRATION', color='black', label='Historical', ax=self.ax_immigration)
        self.iclusv3_immigration.plot(x='YEAR', y='IMMIGRATION', color='orange', linestyle='--', label='ICLUS v3', zorder=10, ax=self.ax_immigration)
        self.census_immigration.plot(x='YEAR', y='IMMIGRATION', color='blue', label='Census', ax=self.ax_immigration)

        self.ax_immigration.get_legend().remove()

        plt.title('IMMIGRATION')
        plt.gca().set_xlabel("")
        plt.gca().set_ylabel("")
        plt.gca().set_xlim(xmin=2010, xmax=2100)
        # plt.gca().legend(bbox_to_anchor=(1.05, 1.10), reverse=True)


def main():
    FigureMaker()


if __name__ == '__main__':
    main()
