import csv
import requests
import sqlite3

import pandas as pd
import seaborn as sns

from matplotlib import pyplot as plt


PART_3_a_INPUTS = 'D:\\projects\\ICLUS_v3\\population\\inputs\\part_3_a\\part_3_a_inputs.sqlite'
con = sqlite3.connect(PART_3_a_INPUTS)
EST_2020 = pd.read_sql(sql='SELECT * FROM Census_county_estimates_2010_2020', con=con)
con.close()

PROJECTIONS_DB = 'D:\\projects\\ICLUS_v3\\population\\outputs\\wittgenstein_v2_2023211154336.sqlite'

COFIPS = '51710'
SCENARIO = 'SSP3'


def getCounties():
    "Function to return a dict of FIPS codes (keys) of U.S. counties (values)"
    d = {}
    r = requests.get("http://www2.census.gov/geo/docs/reference/codes/files/national_county.txt")
    reader = csv.reader(r.text.splitlines(), delimiter=',')
    for line in reader:
        d[line[1] + line[2]] = f'{line[3]}, {line[0]}'

    return d



def main():

    county_dict = getCounties()
    fig = plt.figure(constrained_layout=True)
    gs = fig.add_gridspec(3, 2)

    ######################
    ## TOTAL POPULATION ##
    ######################

    ax_pop = fig.add_subplot(gs[0, :])

    # historical population
    hist_pop = EST_2020.query(f'COFIPS == "{COFIPS}"')[[f'POPESTIMATE{year}' for year in range(2011, 2021)]].T.reset_index()
    hist_pop.columns = ['YEAR', 'TOTAL_POPULATION']
    hist_pop['YEAR'] = hist_pop['YEAR'].str[-4:].astype(int)

    # future population
    query = f'SELECT * FROM population_by_race_gender_age_{SCENARIO} \
              WHERE GEOID == "{COFIPS}"'
    con = sqlite3.connect(PROJECTIONS_DB)
    proj_pop = pd.read_sql(sql=query, con=con)
    con.close()
    proj_pop.drop(columns=['GEOID', 'RACE', 'GENDER', 'AGE_GROUP'], inplace=True)
    proj_pop = proj_pop.sum().reset_index()
    proj_pop.columns = ['YEAR', 'TOTAL_POPULATION']
    proj_pop['YEAR'] = proj_pop['YEAR'].astype(int)

    sns.lineplot(x='YEAR', y='TOTAL_POPULATION', data=hist_pop, color='black', legend=True)
    sns.lineplot(x='YEAR', y='TOTAL_POPULATION', data=proj_pop, color='orange', legend=True, ax=ax_pop)
    plt.title(f'{county_dict[COFIPS]} (FIPS: {COFIPS})')
    plt.xticks(range(2010, 2043, 5))
    plt.gca().set_xlabel('TOTAL\nPOPULATION')
    plt.gca().set_ylabel('')

    ############
    ## BIRTHS ##
    ############

    ax_births = fig.add_subplot(gs[1, :1])

    # historical births
    hist_births = EST_2020.query(f'COFIPS == "{COFIPS}"')[[f'BIRTHS{year}' for year in range(2011, 2021)]].T.reset_index()
    hist_births.columns = ['YEAR', 'BIRTHS']
    hist_births['YEAR'] = hist_births['YEAR'].str[-4:].astype(int)

    # future births
    query = f'SELECT * FROM births_by_race_age_{SCENARIO} \
              WHERE GEOID == "{COFIPS}"'
    con = sqlite3.connect(PROJECTIONS_DB)
    proj_births = pd.read_sql(sql=query, con=con)
    con.close()
    proj_births.drop(columns=['GEOID', 'RACE', 'GENDER', 'AGE_GROUP'], inplace=True)
    proj_births = proj_births.sum().reset_index()
    proj_births.columns = ['YEAR', 'BIRTHS']
    proj_births['YEAR'] = proj_births['YEAR'].astype(int)

    sns.lineplot(x='YEAR', y='BIRTHS', data=hist_births, color='black', legend=True)
    sns.lineplot(x='YEAR', y='BIRTHS', data=proj_births, color='orange', legend=True, ax=ax_births)
    # plt.title('BIRTHS')
    plt.xticks(range(2010, 2043, 10))
    ax_births.set_xticklabels([])
    ax_births.set_xlabel('')

    ############################
    ## NET DOMESTIC MIGRATION ##
    ############################

    ax_netmig = fig.add_subplot(gs[1, 1:])

    # historical migration
    hist_netmig = EST_2020.query(f'COFIPS == "{COFIPS}"')[[f'DOMESTICMIG{year}' for year in range(2010, 2021)]].T.reset_index()
    hist_netmig.columns = ['YEAR', 'DOMESTICMIG']
    hist_netmig['YEAR'] = hist_netmig['YEAR'].str[-4:].astype(int)

    # future netmigration
    query = f'SELECT * FROM migration_by_race_gender_age_{SCENARIO} \
             WHERE GEOID == "{COFIPS}"'
    con = sqlite3.connect(PROJECTIONS_DB)
    proj_netmig = pd.read_sql(sql=query, con=con)
    con.close()
    proj_netmig.drop(columns=['GEOID', 'RACE', 'GENDER', 'AGE_GROUP'], inplace=True)
    proj_netmig = proj_netmig.sum().reset_index()
    proj_netmig.columns = ['YEAR', 'DOMESTICMIG']
    proj_netmig['YEAR'] = proj_netmig['YEAR'].astype(int)

    sns.lineplot(x='YEAR', y='DOMESTICMIG', data=hist_netmig, color='black', legend=True)
    sns.lineplot(x='YEAR', y='DOMESTICMIG', data=proj_netmig, color='orange', legend=True, ax=ax_netmig)
    # plt.title('NET MIGRATION')
    plt.xticks(range(2010, 2043, 10))
    ax_netmig.set_xticklabels([])
    ax_netmig.set_xlabel('')

    ############
    ## DEATHS ##
    ############

    ax_deaths = fig.add_subplot(gs[2, :1])

    # historical deaths
    hist_deaths = EST_2020.query(f'COFIPS == "{COFIPS}"')[[f'DEATHS{year}' for year in range(2010, 2021)]].T.reset_index()
    hist_deaths.columns = ['YEAR', 'DEATHS']
    hist_deaths['YEAR'] = hist_deaths['YEAR'].str[-4:].astype(int)

    # future deaths
    query = f'SELECT * FROM deaths_by_race_gender_age_{SCENARIO} \
             WHERE GEOID == "{COFIPS}"'
    con = sqlite3.connect(PROJECTIONS_DB)
    proj_deaths = pd.read_sql(sql=query, con=con)
    con.close()
    proj_deaths.drop(columns=['GEOID', 'RACE', 'GENDER', 'AGE_GROUP'], inplace=True)
    proj_deaths = proj_deaths.sum().reset_index()
    proj_deaths.columns = ['YEAR', 'DEATHS']
    proj_deaths['YEAR'] = proj_deaths['YEAR'].astype(int)

    sns.lineplot(x='YEAR', y='DEATHS', data=hist_deaths, color='black', legend=True)
    sns.lineplot(x='YEAR', y='DEATHS', data=proj_deaths, color='orange', legend=True, ax=ax_deaths)
    # plt.title('DEATHS')
    plt.xticks(range(2010, 2043, 10))

    #####################
    ## NET IMMIGRATION ##
    #####################

    ax_imm = fig.add_subplot(gs[2, 1:])

    # historical immig
    hist_immig = EST_2020.query(f'COFIPS == "{COFIPS}"')[[f'INTERNATIONALMIG{year}' for year in range(2010, 2021)]].T.reset_index()
    hist_immig.columns = ['YEAR', 'IMMIGRATION']
    hist_immig['YEAR'] = hist_immig['YEAR'].str[-4:].astype(int)

    # future immig
    query = f'SELECT * FROM immigration_by_race_gender_age_{SCENARIO} \
             WHERE GEOID == "{COFIPS}"'
    con = sqlite3.connect(PROJECTIONS_DB)
    proj_immig = pd.read_sql(sql=query, con=con)
    con.close()
    proj_immig.drop(columns=['GEOID', 'RACE', 'GENDER', 'AGE_GROUP'], inplace=True)
    proj_immig = proj_immig.sum().reset_index()
    proj_immig.columns = ['YEAR', 'IMMIGRATION']
    proj_immig['YEAR'] = proj_immig['YEAR'].astype(int)

    sns.lineplot(x='YEAR', y='IMMIGRATION', data=hist_immig, color='black', legend=True)
    sns.lineplot(x='YEAR', y='IMMIGRATION', data=proj_immig, color='orange', legend=True, ax=ax_imm)
    # plt.title('IMMIGRATION')
    plt.xticks(range(2010, 2043, 10))

    plt.tight_layout()
    plt.show()

    return


if __name__ == '__main__':
    main()
