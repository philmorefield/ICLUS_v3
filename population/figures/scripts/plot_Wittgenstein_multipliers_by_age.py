import os
import sqlite3

import pandas as pd
import seaborn as sns

from matplotlib import pyplot as plt

BASE_FOLDER = 'D:\\projects\\ICLUS_v3\\population'
if os.path.isdir('D:\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'


WITTGENSTEIN_DB = os.path.join(BASE_FOLDER, 'inputs', 'databases', 'wittgenstein.sqlite')
POPULATION_DB = os.path.join(BASE_FOLDER, 'inputs', 'databases', 'population.sqlite')
PROJECTIONS_DB = os.path.join(BASE_FOLDER, 'outputs', 'wittgenstein_v3_202549211314.sqlite')

AGE_GROUPS = ('15_TO_19', '20_TO_24', '25_TO_29', '30_TO_34', '35_TO_39',
              '40_TO_44')

SCENARIO = 'SSP3'

def main():
    for age_group in AGE_GROUPS:
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
                WHERE AGE_GROUP == "{age_group}"'
        hist_pop = pd.read_sql_query(sql=sql, con=con)
        con.close()

        hist_pop = hist_pop.groupby(by='YEAR', as_index=False).sum()

        # future population
        query = f'SELECT * \
                  FROM population_by_race_sex_age_{SCENARIO} \
                  WHERE AGE_GROUP == "{age_group.replace("_TO_", "-")}"'
        con = sqlite3.connect(PROJECTIONS_DB)
        proj_pop = pd.read_sql_query(sql=query, con=con)
        con.close()

        proj_pop = proj_pop.drop(columns=['GEOID', 'AGE_GROUP', 'RACE', 'SEX'])
        proj_pop = proj_pop.sum().reset_index()
        proj_pop.columns = ['YEAR', 'POPULATION']

        df = pd.concat([hist_pop, proj_pop], axis=0, ignore_index=True)
        df['YEAR'] = df['YEAR'].astype(int)
        df.plot(x='YEAR', legend=False, ax=ax_pop)

        plt.title(f'TOTAL POPULATION: {age_group}')
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
        query = f'SELECT YEAR, SCENARIO, FERT_CHANGE_MULT \
                  FROM age_specific_fertility_v3 \
                  WHERE AGE_GROUP == "{age_group.replace('_TO_', '-')}"'
        con = sqlite3.connect(WITTGENSTEIN_DB)
        proj_births = pd.read_sql(sql=query, con=con)
        con.close()

        sns.lineplot(x='YEAR', y='FERT_CHANGE_MULT', hue='SCENARIO', data=proj_births, legend=False, ax=ax_births)

        plt.title('FERT_CHANGE_MULT')
        ax_births.set_xticklabels([])
        ax_births.set_xlabel('')
        ax_births.set_ylabel('')


        ############
        ## DEATHS ##
        ############

        ax_deaths = fig.add_subplot(gs[2, :1])

        # future deaths
        query = f'SELECT YEAR, SCENARIO, MORT_CHANGE_MULT \
                  FROM age_specific_mortality_v3 \
                  WHERE AGE_GROUP == "{age_group.replace('_TO_', '-')}" \
                  AND SEX == "MALE"'
        con = sqlite3.connect(WITTGENSTEIN_DB)
        proj_deaths = pd.read_sql(sql=query, con=con)
        con.close()

        proj_deaths = proj_deaths.groupby(by=['YEAR', 'SCENARIO'], as_index=False).mean()

        sns.lineplot(x='YEAR', y='MORT_CHANGE_MULT', hue='SCENARIO', data=proj_deaths, legend=False, ax=ax_deaths)

        plt.title('MORT_CHANGE_MULT')
        ax_deaths.set_xlabel('')
        ax_deaths.set_ylabel('')

        #####################
        ## NET IMMIGRATION ##
        #####################

        ax_immig = fig.add_subplot(gs[2, 1:])

        # future immigration
        query = f'SELECT YEAR, SCENARIO, NETMIG_INTERP_COHORT \
                  FROM age_specific_net_migration_v3 \
                  WHERE AGE_GROUP == "{age_group.replace('_TO_', '-')}"'
        con = sqlite3.connect(WITTGENSTEIN_DB)
        proj_immig = pd.read_sql(sql=query, con=con)
        con.close()

        sns.lineplot(x='YEAR', y='NETMIG_INTERP_COHORT', hue='SCENARIO', data=proj_immig, legend=False, ax=ax_immig)

        plt.title('NETMIG_INTERP_COHORT')
        ax_immig.set_xlabel('')
        ax_immig.set_ylabel('')

        ax_immig.legend(bbox_to_anchor=(1.05, 1))
        plt.tight_layout()
        plt.show()


if __name__ == '__main__':
    main()
