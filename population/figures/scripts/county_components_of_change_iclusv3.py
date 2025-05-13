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


# # this run had erroneously large immigration values for 2023 and 2024
# PROJECTIONS_DB = os.path.join(BASE_FOLDER, 'outputs', 'iclus_v3_census_202551163547.sqlite')

# PROJECTIONS_DB = os.path.join(BASE_FOLDER, 'outputs', 'iclus_v3_census_202553173742.sqlite')
# CDC_FERT = 'No adj'
# CDC_MORT = 'No adj'
# CDC_IMM = 'Hist21_24'
# SCENARIO = 'mid'

# PROJECTIONS_DB = os.path.join(BASE_FOLDER, 'outputs', 'iclus_v3_census_20255416653.sqlite')
# CDC_FERT = '-4.5%'
# CDC_MORT = '-15%'
# CDC_IMM = 'Hist21_24'
# SCENARIO = 'mid'

# PROJECTIONS_DB = os.path.join(BASE_FOLDER, 'outputs', 'iclus_v3_census_202555143420.sqlite')
# CDC_FERT = '-5.5%'
# CDC_MORT = '-15%'
# CDC_IMM = 'Hist21_24'
# SCENARIO = 'mid'

# PROJECTIONS_DB = os.path.join(BASE_FOLDER, 'outputs', 'iclus_v3_census_202556221624.sqlite')
# CDC_FERT = '-5.5%'
# CDC_MORT = '-15%'
# CDC_IMM = 'Hist21_24'
# SCENARIO = 'hi'

# PROJECTIONS_DB = os.path.join(BASE_FOLDER, 'outputs', 'iclus_v3_census_202557114026.sqlite')
# CDC_FERT = '-5.5%'
# CDC_MORT = 'No adj'
# CDC_IMM = 'Hist21_24'
# SCENARIO = 'hi'

# PROJECTIONS_DB = os.path.join(BASE_FOLDER, 'outputs', 'iclus_v3_census_202558155654.sqlite')
# CDC_FERT = 'No adj'
# CDC_MORT = 'No adj'
# CDC_IMM = 'Hist21_22'
# SCENARIO = 'mid'

# PROJECTIONS_DB = os.path.join(BASE_FOLDER, 'outputs', 'iclus_v3_census_20255722437.sqlite')
# CDC_FERT = '-5.5%'
# CDC_MORT = 'No adj'
# CDC_IMM = 'Hist21_24'
# SCENARIO = 'low'

# PROJECTIONS_DB = os.path.join(BASE_FOLDER, 'outputs', 'iclus_v3_census_2025597926.sqlite')
# CDC_FERT = 'No adj'
# CDC_MORT = 'No adj'
# CDC_IMM = 'Hist21_24'
# SCENARIO = 'low'

PROJECTIONS_DB = os.path.join(BASE_FOLDER, 'outputs', 'iclus_v3_census_202551072849.sqlite')
CDC_FERT = '-5.5%'
CDC_MORT = '-15%'
CDC_IMM = 'Hist21_22'
SCENARIO = 'low'
COFIPS = '06073'



class FigureMaker():
    def __init__(self):
        self.fig = plt.figure(figsize=(6.5, 6.5), constrained_layout=True)
        self.gs = self.fig.add_gridspec(3, 2)

        # total population
        self.get_historical_total_population()
        self.get_iclus_v3_projected_total_population()
        self.plot_total_population()

        # births
        self.get_historical_births()
        self.get_iclus_v3_projected_births()
        self.plot_births()

        # deaths
        self.get_historical_deaths()
        self.get_iclus_v3_projected_deaths()
        self.plot_deaths()

        # domestic migration
        self.get_historical_migration()
        self.get_iclus_v3_projected_migration()
        self.plot_migration()

        # immigration
        self.get_historical_immigration()
        self.get_iclus_v3_projected_immigration()
        self.plot_immigration()

        self.fig.suptitle(f"ICLUS v3: {self.county_name}, {self.state_name} ({COFIPS})\nScenario: {SCENARIO.upper()} | CDC Fert: {CDC_FERT} | CDC Mort: {CDC_MORT} | CDC Imm: {CDC_IMM}\n{os.path.basename(PROJECTIONS_DB)}"
)
        plt.tight_layout()
        self.ax_births.legend(bbox_to_anchor=(1.9, 2.25))
        plt.show()

    def get_historical_total_population(self):
        # historical population, 2010-2020
        con = sqlite3.connect(POPULATION_DB)
        sql = f'SELECT YEAR, POPULATION \
               FROM county_population_ageracegender_2010_to_2020 \
               WHERE COFIPS == "{COFIPS}"'
        self.hist_pop_one = pd.read_sql_query(sql=sql, con=con)
        con.close()

        self.hist_pop_one = self.hist_pop_one.groupby(by='YEAR', as_index=False).sum()
        self.hist_pop_one['POPULATION'] /= 1000000

        # historical population, 2020-2024
        csv = os.path.join(CENSUS_CSV_PATH, '2024\\co-est2024-alldata.csv')
        self.hist_pop_two = pd.read_csv(csv, encoding='latin-1')
        columns = ['STATE', 'COUNTY', 'STNAME', 'CTYNAME'] + ['POPESTIMATE' + str(year) for year in range(2020, 2025)]
        self.hist_pop_two = self.hist_pop_two[columns]
        self.hist_pop_two['COFIPS'] = self.hist_pop_two['STATE'].astype(str).str.zfill(2) + self.hist_pop_two['COUNTY'].astype(str).str.zfill(3)
        self.hist_pop_two = self.hist_pop_two.query('COFIPS == @COFIPS')
        self.county_name = self.hist_pop_two['CTYNAME'].values[0]
        self.state_name = self.hist_pop_two['STNAME'].values[0]
        self.hist_pop_two.drop(columns=['STATE', 'COUNTY', 'STNAME', 'CTYNAME'], inplace=True)

        self.hist_pop_two = self.hist_pop_two.melt(id_vars='COFIPS', var_name='YEAR', value_name='POPULATION')
        self.hist_pop_two = self.hist_pop_two.drop(columns='COFIPS')
        self.hist_pop_two['YEAR'] = self.hist_pop_two['YEAR'].str.replace('POPESTIMATE', '').astype(int)
        self.hist_pop_two['POPULATION'] /= 1000000

    def get_iclus_v3_projected_total_population(self):
        query = f'SELECT * \
                  FROM population_by_race_sex_age_{SCENARIO} \
                  WHERE GEOID == "{COFIPS}"'
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


    def plot_total_population(self):
        self.ax_pop = self.fig.add_subplot(self.gs[0, :1])
        self.hist_pop_one.plot(x='YEAR', y='POPULATION', color='gray', label='Historical', ax=self.ax_pop)
        self.hist_pop_two.plot(x='YEAR', y='POPULATION', color='black', label='Historical', ax=self.ax_pop)
        self.iclusv3_pop.plot(x='YEAR', y='POPULATION', color='orange', label='ICLUS v3', ax=self.ax_pop)

        plt.title('TOTAL POPULATION')
        plt.gca().set_xlabel("")
        plt.gca().set_ylabel("")
        plt.gca().set_xlim(xmin=2010, xmax=2100)

        self.ax_pop.get_legend().remove()

    def get_historical_births(self):
        # historical births, 2010-2020
        csv = os.path.join(CENSUS_CSV_PATH, '2020\\co-est2020-alldata.csv')
        self.hist_births_one = pd.read_csv(csv, encoding='latin-1')
        columns = ['STATE', 'COUNTY'] + ['BIRTHS' + str(year) for year in range(2010, 2021)]
        self.hist_births_one = self.hist_births_one[columns]
        self.hist_births_one['COFIPS'] = self.hist_births_one['STATE'].astype(str).str.zfill(2) + self.hist_births_one['COUNTY'].astype(str).str.zfill(3)
        self.hist_births_one = self.hist_births_one.query('COFIPS == @COFIPS')
        self.hist_births_one.drop(columns=['STATE', 'COUNTY'], inplace=True)

        self.hist_births_one = self.hist_births_one.melt(id_vars='COFIPS', var_name='YEAR', value_name='BIRTHS')
        self.hist_births_one = self.hist_births_one.drop(columns='COFIPS')
        self.hist_births_one['YEAR'] = self.hist_births_one['YEAR'].str.replace('BIRTHS', '').astype(int)
        # self.hist_births_one['BIRTHS'] /= 1000000

        # historical births, 2020-2024
        csv = os.path.join(CENSUS_CSV_PATH, '2024\\co-est2024-alldata.csv')
        self.hist_births_two = pd.read_csv(csv, encoding='latin-1')
        columns = ['STATE', 'COUNTY'] + ['BIRTHS' + str(year) for year in range(2020, 2025)]
        self.hist_births_two = self.hist_births_two[columns]
        self.hist_births_two['COFIPS'] = self.hist_births_two['STATE'].astype(str).str.zfill(2) + self.hist_births_two['COUNTY'].astype(str).str.zfill(3)
        self.hist_births_two = self.hist_births_two.query('COFIPS == @COFIPS')
        self.hist_births_two.drop(columns=['STATE', 'COUNTY'], inplace=True)

        self.hist_births_two = self.hist_births_two.melt(id_vars='COFIPS', var_name='YEAR', value_name='BIRTHS')
        self.hist_births_two = self.hist_births_two.drop(columns='COFIPS')
        self.hist_births_two['YEAR'] = self.hist_births_two['YEAR'].str.replace('BIRTHS', '').astype(int)
        # self.hist_births_two['BIRTHS'] /= 1000000

    def get_iclus_v3_projected_births(self):
        query = f'SELECT * \
                  FROM births_by_race_sex_age_{SCENARIO} \
                  WHERE GEOID == "{COFIPS}"'
        con = sqlite3.connect(PROJECTIONS_DB)
        self.iclusv3_births = pd.read_sql_query(sql=query, con=con)
        con.close()

        self.iclusv3_births = self.iclusv3_births.melt(id_vars=['GEOID', 'AGE_GROUP', 'RACE', 'SEX'],
                                                       var_name='YEAR',
                                                       value_name='BIRTHS')
        self.iclusv3_births = self.iclusv3_births[['YEAR', 'BIRTHS']].groupby(by='YEAR', as_index=False).sum()
        self.iclusv3_births['YEAR'] = self.iclusv3_births['YEAR'].astype(int64)
        self.iclusv3_births['BIRTHS'] = self.iclusv3_births['BIRTHS'].astype(int64)
        # self.iclusv3_births['BIRTHS'] /= 1000000


    def plot_births(self):
        self.ax_births = self.fig.add_subplot(self.gs[1, :1])
        self.hist_births_one.plot(x='YEAR', y='BIRTHS', color='gray', label='Historical', ax=self.ax_births)
        self.hist_births_two.plot(x='YEAR', y='BIRTHS', color='black', label='Historical', ax=self.ax_births)
        self.iclusv3_births.plot(x='YEAR', y='BIRTHS', color='orange', label='ICLUS v3', ax=self.ax_births)

        plt.title('BIRTHS')
        plt.gca().set_xlabel("")
        plt.gca().set_ylabel("Millions")
        plt.gca().set_xlim(xmin=2010, xmax=2100)

    def get_historical_deaths(self):
        # historical deaths, 2010-2020
        csv = os.path.join(CENSUS_CSV_PATH, '2020\\co-est2020-alldata.csv')
        self.hist_deaths_one = pd.read_csv(csv, encoding='latin-1')
        columns = ['STATE', 'COUNTY'] + ['DEATHS' + str(year) for year in range(2010, 2021)]
        self.hist_deaths_one = self.hist_deaths_one[columns]
        self.hist_deaths_one['COFIPS'] = self.hist_deaths_one['STATE'].astype(str).str.zfill(2) + self.hist_deaths_one['COUNTY'].astype(str).str.zfill(3)
        self.hist_deaths_one = self.hist_deaths_one.query('COFIPS == @COFIPS')
        self.hist_deaths_one.drop(columns=['STATE', 'COUNTY'], inplace=True)

        self.hist_deaths_one = self.hist_deaths_one.melt(id_vars='COFIPS', var_name='YEAR', value_name='DEATHS')
        self.hist_deaths_one = self.hist_deaths_one.drop(columns='COFIPS')
        self.hist_deaths_one['YEAR'] = self.hist_deaths_one['YEAR'].str.replace('DEATHS', '').astype(int)
        # self.hist_deaths_one['DEATHS'] /= 1000000

        # historical deaths, 2020-2024
        csv = os.path.join(CENSUS_CSV_PATH, '2024\\co-est2024-alldata.csv')
        self.hist_deaths_two = pd.read_csv(csv, encoding='latin-1')
        columns = ['STATE', 'COUNTY'] + ['DEATHS' + str(year) for year in range(2020, 2025)]
        self.hist_deaths_two = self.hist_deaths_two[columns]
        self.hist_deaths_two['COFIPS'] = self.hist_deaths_two['STATE'].astype(str).str.zfill(2) + self.hist_deaths_two['COUNTY'].astype(str).str.zfill(3)
        self.hist_deaths_two = self.hist_deaths_two.query('COFIPS == @COFIPS')
        self.hist_deaths_two.drop(columns=['STATE', 'COUNTY'], inplace=True)

        self.hist_deaths_two = self.hist_deaths_two.melt(id_vars='COFIPS', var_name='YEAR', value_name='DEATHS')
        self.hist_deaths_two = self.hist_deaths_two.drop(columns='COFIPS')
        self.hist_deaths_two['YEAR'] = self.hist_deaths_two['YEAR'].str.replace('DEATHS', '').astype(int)
        # self.hist_deaths_two['DEATHS'] /= 1000000

    def get_iclus_v3_projected_deaths(self):
        query = f'SELECT * \
                  FROM deaths_by_race_sex_age_{SCENARIO} \
                  WHERE GEOID == "{COFIPS}"'
        con = sqlite3.connect(PROJECTIONS_DB)
        self.iclusv3_deaths = pd.read_sql_query(sql=query, con=con)
        con.close()

        self.iclusv3_deaths = self.iclusv3_deaths.melt(id_vars=['GEOID', 'AGE_GROUP', 'RACE', 'SEX'],
                                                       var_name='YEAR',
                                                       value_name='DEATHS')
        self.iclusv3_deaths = self.iclusv3_deaths[['YEAR', 'DEATHS']].groupby(by='YEAR', as_index=False).sum()
        self.iclusv3_deaths['YEAR'] = self.iclusv3_deaths['YEAR'].astype(int64)
        self.iclusv3_deaths['DEATHS'] = self.iclusv3_deaths['DEATHS'].astype(int64)
        # self.iclusv3_deaths['DEATHS'] /= 1000000


    def plot_deaths(self):
        self.ax_deaths = self.fig.add_subplot(self.gs[2, :1])
        self.hist_deaths_one.plot(x='YEAR', y='DEATHS', color='gray', label='Historical', ax=self.ax_deaths)
        self.hist_deaths_two.plot(x='YEAR', y='DEATHS', color='black', label='Historical', ax=self.ax_deaths)
        self.iclusv3_deaths.plot(x='YEAR', y='DEATHS', color='orange', label='ICLUS v3', ax=self.ax_deaths)

        self.ax_deaths.get_legend().remove()

        plt.title('DEATHS')
        plt.gca().set_xlabel("")
        plt.gca().set_ylabel("")
        plt.gca().set_xlim(xmin=2010, xmax=2100)

    def get_historical_migration(self):
        # historical migration, 2010-2020
        csv = os.path.join(CENSUS_CSV_PATH, '2020\\co-est2020-alldata.csv')
        self.hist_migration_one = pd.read_csv(csv, encoding='latin-1')
        columns = ['STATE', 'COUNTY'] + ['NETMIG' + str(year) for year in range(2010, 2021)]
        self.hist_migration_one = self.hist_migration_one[columns]
        self.hist_migration_one['COFIPS'] = self.hist_migration_one['STATE'].astype(str).str.zfill(2) + self.hist_migration_one['COUNTY'].astype(str).str.zfill(3)
        self.hist_migration_one = self.hist_migration_one.query('COFIPS == @COFIPS')
        self.hist_migration_one.drop(columns=['STATE', 'COUNTY'], inplace=True)

        self.hist_migration_one = self.hist_migration_one.melt(id_vars='COFIPS', var_name='YEAR', value_name='MIGRATION')
        self.hist_migration_one = self.hist_migration_one.drop(columns='COFIPS')
        self.hist_migration_one['YEAR'] = self.hist_migration_one['YEAR'].str.replace('NETMIG', '').astype(int)
        # self.hist_migration_one['MIGRATION'] /= 1000000

        # historical migration, 2020-2024
        csv = os.path.join(CENSUS_CSV_PATH, '2024\\co-est2024-alldata.csv')
        self.hist_migration_two = pd.read_csv(csv, encoding='latin-1')
        columns = ['STATE', 'COUNTY'] + ['NETMIG' + str(year) for year in range(2020, 2025)]
        self.hist_migration_two = self.hist_migration_two[columns]
        self.hist_migration_two['COFIPS'] = self.hist_migration_two['STATE'].astype(str).str.zfill(2) + self.hist_migration_two['COUNTY'].astype(str).str.zfill(3)
        self.hist_migration_two = self.hist_migration_two.query('COFIPS == @COFIPS')
        self.hist_migration_two.drop(columns=['STATE', 'COUNTY'], inplace=True)

        self.hist_migration_two = self.hist_migration_two.melt(id_vars='COFIPS', var_name='YEAR', value_name='MIGRATION')
        self.hist_migration_two = self.hist_migration_two.drop(columns='COFIPS')
        self.hist_migration_two['YEAR'] = self.hist_migration_two['YEAR'].str.replace('NETMIG', '').astype(int)
        # self.hist_migration_two['MIGRATION'] /= 1000000

    def get_iclus_v3_projected_migration(self):
        query = f'SELECT * FROM migration_by_race_sex_age_{SCENARIO}'
        con = sqlite3.connect(PROJECTIONS_DB)
        self.iclusv3_migration = pd.read_sql_query(sql=query, con=con)
        con.close()

        columns = ['GEOID', 'AGE_GROUP', 'RACE', 'SEX'] + ['INMIG' + str(year) for year in range(2021, 2100)]
        self.iclusv3_migration = self.iclusv3_migration[columns]

        self.iclusv3_migration = self.iclusv3_migration.melt(id_vars=['GEOID', 'AGE_GROUP', 'RACE', 'SEX'],
                                                             var_name='YEAR',
                                                             value_name='MIGRATION')
        self.iclusv3_migration['YEAR'] = self.iclusv3_migration['YEAR'].str.replace('INMIG', '').astype(int)
        self.iclusv3_migration = self.iclusv3_migration[['YEAR', 'MIGRATION']].groupby(by='YEAR', as_index=False).sum()
        # self.iclusv3_migration['YEAR'] = self.iclusv3_migration['YEAR'].astype(int64)
        self.iclusv3_migration['MIGRATION'] = self.iclusv3_migration['MIGRATION'].astype(int64)
        # self.iclusv3_migration['MIGRATION'] /= 1000000

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

    def get_historical_immigration(self):
        # historical immigration, 2010-2020
        csv = os.path.join(CENSUS_CSV_PATH, '2020\\co-est2020-alldata.csv')
        self.hist_immigration_one = pd.read_csv(csv, encoding='latin-1')
        columns = ['STATE', 'COUNTY'] + ['INTERNATIONALMIG' + str(year) for year in range(2010, 2021)]
        self.hist_immigration_one = self.hist_immigration_one[columns]
        self.hist_immigration_one['COFIPS'] = self.hist_immigration_one['STATE'].astype(str).str.zfill(2) + self.hist_immigration_one['COUNTY'].astype(str).str.zfill(3)
        self.hist_immigration_one = self.hist_immigration_one.query('COFIPS == @COFIPS')
        self.hist_immigration_one.drop(columns=['STATE', 'COUNTY'], inplace=True)

        self.hist_immigration_one = self.hist_immigration_one.melt(id_vars='COFIPS', var_name='YEAR', value_name='IMMIGRATION')
        self.hist_immigration_one = self.hist_immigration_one.drop(columns='COFIPS')
        self.hist_immigration_one['YEAR'] = self.hist_immigration_one['YEAR'].str.replace('INTERNATIONALMIG', '').astype(int)
        # self.hist_immigration_one['IMMIGRATION'] /= 1000000

        # historical immigration, 2020-2024
        csv = os.path.join(CENSUS_CSV_PATH, '2024\\co-est2024-alldata.csv')
        self.hist_immigration_two = pd.read_csv(csv, encoding='latin-1')
        columns = ['STATE', 'COUNTY'] + ['INTERNATIONALMIG' + str(year) for year in range(2020, 2025)]
        self.hist_immigration_two = self.hist_immigration_two[columns]
        self.hist_immigration_two['COFIPS'] = self.hist_immigration_two['STATE'].astype(str).str.zfill(2) + self.hist_immigration_two['COUNTY'].astype(str).str.zfill(3)
        self.hist_immigration_two = self.hist_immigration_two.query('COFIPS == @COFIPS')
        self.hist_immigration_two.drop(columns=['STATE', 'COUNTY'], inplace=True)

        self.hist_immigration_two = self.hist_immigration_two.melt(id_vars='COFIPS', var_name='YEAR', value_name='IMMIGRATION')
        self.hist_immigration_two = self.hist_immigration_two.drop(columns='COFIPS')
        self.hist_immigration_two['YEAR'] = self.hist_immigration_two['YEAR'].str.replace('INTERNATIONALMIG', '').astype(int)
        # self.hist_immigration_two['IMMIGRATION'] /= 1000000

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
        # self.iclusv3_immigration['IMMIGRATION'] /= 1000000


    def plot_immigration(self):
        self.ax_immigration = self.fig.add_subplot(self.gs[2, 1:])
        self.hist_immigration_one.plot(x='YEAR', y='IMMIGRATION', color='gray', label='Historical', ax=self.ax_immigration)
        self.hist_immigration_two.plot(x='YEAR', y='IMMIGRATION', color='black', label='Historical', ax=self.ax_immigration)
        self.iclusv3_immigration.plot(x='YEAR', y='IMMIGRATION', color='orange', linestyle='--', label='ICLUS v3', zorder=10, ax=self.ax_immigration)

        self.ax_immigration.get_legend().remove()

        plt.title('IMMIGRATION')
        plt.gca().set_xlabel("")
        plt.gca().set_ylabel("")
        plt.gca().set_xlim(xmin=2010, xmax=2100)


def main():
    FigureMaker()


if __name__ == '__main__':
    main()
