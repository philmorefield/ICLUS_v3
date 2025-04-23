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

    for race in ('WHITE', 'BLACK', 'ASIAN', 'AIAN', 'NHPI', 'TWO_OR_MORE'):
        fig = plt.figure(constrained_layout=True)
        gs = fig.add_gridspec(3, 2)

        ######################
        ## TOTAL POPULATION ##
        ######################

        ax_pop = fig.add_subplot(gs[0, :])

        # historical population
        con = sqlite3.connect(POPULATION_DB)
        sql = f'SELECT YEAR, POPULATION \
                FROM county_population_ageracegender_2010_to_2020 \
                WHERE RACE == "{race}"'
        hist_pop = pd.read_sql_query(sql=sql, con=con)
        con.close()
        hist_pop = hist_pop.groupby(by='YEAR', as_index=False).sum()

        # future population
        query = f'SELECT * \
                  FROM population_by_race_sex_age_{SCENARIO} \
                  WHERE RACE == "{race}"'
        con = sqlite3.connect(PROJECTIONS_DB)
        proj_pop = pd.read_sql_query(sql=query, con=con)
        con.close()

        proj_pop = proj_pop.drop(columns=['GEOID', 'AGE_GROUP', 'RACE', 'SEX'])
        proj_pop = proj_pop.sum().reset_index()
        proj_pop.columns = ['YEAR', 'POPULATION']

        df = pd.concat([hist_pop, proj_pop], axis=0, ignore_index=True)
        df['YEAR'] = df['YEAR'].astype(int)
        df.plot(x='YEAR', legend=False, ax=ax_pop)

        plt.title(f'TOTAL POPULATION: {race}')
        plt.gca().set_xlabel("")
        plt.gca().set_ylabel('')
        xmin = df.YEAR.astype(int).min()
        xmax = df.YEAR.astype(int).max()
        plt.gca().set_xlim(xmin=xmin, xmax=xmax)


        ############
        ## BIRTHS ##
        ############

        ax_births = fig.add_subplot(gs[1, :1])

        # future births
        query = f'SELECT * \
                  FROM births_by_race_sex_age_{SCENARIO} \
                  WHERE RACE == "{race}"'
        con = sqlite3.connect(PROJECTIONS_DB)
        proj_births = pd.read_sql(sql=query, con=con)
        con.close()

        proj_births = proj_births.drop(columns=['GEOID', 'RACE', 'SEX', 'AGE_GROUP'])
        proj_births = proj_births.sum().reset_index()
        proj_births.columns = ['YEAR', 'BIRTHS']
        proj_births['YEAR'] = proj_births['YEAR'].astype(int)

        sns.lineplot(x='YEAR', y='BIRTHS', data=proj_births, color='orange', legend=False, ax=ax_births)

        plt.title('BIRTHS')
        ax_births.set_xticklabels([])
        ax_births.set_xlabel('')
        ax_births.set_ylabel('')

        ############################
        ## NET DOMESTIC MIGRATION ##
        ############################

        # historical migration
        ax_migration = fig.add_subplot(gs[1, 1:])

        # future migration
        query = f'SELECT * \
                  FROM migration_by_race_sex_age_{SCENARIO} \
                  WHERE RACE == "{race}"'
        con = sqlite3.connect(PROJECTIONS_DB)
        proj_migration = pd.read_sql(sql=query, con=con)
        con.close()

        proj_migration = proj_migration.drop(columns=['GEOID', 'RACE', 'SEX', 'AGE_GROUP'])
        proj_migration = proj_migration.clip(lower=0).sum().reset_index()
        proj_migration.columns = ['YEAR', 'MIGRATION']
        proj_migration['YEAR'] = proj_migration['YEAR'].astype(int)

        sns.lineplot(x='YEAR', y='MIGRATION', data=proj_migration, color='orange', legend=False, ax=ax_migration)

        plt.title('MIGRATION')
        ax_migration.set_xticklabels([])
        ax_migration.set_xlabel('')
        ax_migration.set_ylabel('')

        ############
        ## DEATHS ##
        ############

        ax_deaths = fig.add_subplot(gs[2, :1])

        # future deaths
        query = f'SELECT * \
                  FROM deaths_by_race_sex_age_{SCENARIO} \
                  WHERE RACE == "{race}"'
        con = sqlite3.connect(PROJECTIONS_DB)
        proj_deaths = pd.read_sql(sql=query, con=con)
        con.close()

        proj_deaths = proj_deaths.drop(columns=['GEOID', 'RACE', 'SEX', 'AGE_GROUP'])
        proj_deaths = proj_deaths.sum().reset_index()
        proj_deaths.columns = ['YEAR', 'DEATHS']
        proj_deaths['YEAR'] = proj_deaths['YEAR'].astype(int)

        sns.lineplot(x='YEAR', y='DEATHS', data=proj_deaths, color='orange', legend=False, ax=ax_deaths)

        plt.title('DEATHS')
        ax_deaths.set_xlabel('')
        ax_deaths.set_ylabel('')

        #####################
        ## NET IMMIGRATION ##
        #####################

        ax_immig = fig.add_subplot(gs[2, 1:])

        # future immigration
        query = f'SELECT * \
                  FROM immigration_by_race_sex_age_{SCENARIO} \
                  WHERE RACE == "{race}"'
        con = sqlite3.connect(PROJECTIONS_DB)
        proj_immig = pd.read_sql(sql=query, con=con)
        con.close()

        proj_immig = proj_immig.drop(columns=['GEOID', 'RACE', 'SEX', 'AGE_GROUP'])
        proj_immig = proj_immig.sum().reset_index()
        proj_immig.columns = ['YEAR', 'IMMIGRATION']
        proj_immig['YEAR'] = proj_immig['YEAR'].astype(int)

        sns.lineplot(x='YEAR', y='IMMIGRATION', data=proj_immig, color='orange', legend=False, ax=ax_immig)

        plt.title('IMMIGRATION')
        ax_immig.set_xlabel('')
        ax_immig.set_ylabel('')

        plt.tight_layout()
        plt.show()


if __name__ == '__main__':
    main()
