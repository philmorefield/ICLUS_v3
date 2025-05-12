import os
import sqlite3

import matplotlib.pyplot as plt
import pandas as pd

import seaborn as sns


BASE_FOLDER = 'D:\\projects\\ICLUS_v3\\population'
if os.path.isdir('D:\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'

CENSUS_CSV_PATH = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\Census')
POPULATION_DB = os.path.join(BASE_FOLDER, 'inputs', 'databases', 'population.sqlite')
PROJECTIONS_DB = os.path.join(BASE_FOLDER, 'outputs', 'iclus_v3_census_202551072849.sqlite')


sort_dict = {'0-4': 1,
             '5-9': 2,
             '10-14': 3,
             '15-19': 4,
             '20-24': 5,
             '25-29': 6,
             '30-34': 7,
             '35-39': 8,
             '40-44': 9,
             '45-49': 10,
             '50-54': 11,
             '55-59': 12,
             '60-64': 13,
             '65-69': 14,
             '70-74': 15,
             '75-79': 16,
             '80-84': 17,
             '85+': 18}


# def make_movie():
#     f = matplotlib.figure.Figure(figsize=[x/dpi for x in video_dim], dpi=dpi)
#     canvas = FigureCanvas(f)
#     for idx in range(0, men.shape[1]):  # len(wt)):
#         f.clf()
#         f = make_plot(f, t_idx=idx, group_size=group_size)
#         f.savefig('movie%.4d.png' % idx)


def get_iclusv3_projection(scenario, year, cofips):

    con = sqlite3.connect(PROJECTIONS_DB)

    query = f'SELECT AGE_GROUP, SEX, "{year}" AS "Population" \
              FROM population_by_race_sex_age_{scenario} \
              WHERE GEOID == "{cofips}"'
    df = pd.read_sql(sql=query, con=con)
    con.close()

    df = df.groupby(by=['AGE_GROUP', 'SEX'], as_index=False).sum()
    df.rename(columns={'AGE_GROUP': 'Age group',
                       'SEX': 'Sex'},
              inplace=True)

    df.loc[df.Sex == 'MALE', 'Population'] *= -1
    df['SORT_INDEX'] = df['Age group'].map(sort_dict)
    df.sort_values(by=['SORT_INDEX', 'Sex'], ascending=False, inplace=True)

    return df


def get_census_historical(cofips):
    con = sqlite3.connect(POPULATION_DB)

    query = f'SELECT AGE_GROUP, SEX, POPULATION \
              FROM county_population_ageracesex_2020 \
              WHERE GEOID == "{cofips}"'
    df = pd.read_sql(sql=query, con=con)
    con.close()

    df = df.groupby(by=['AGE_GROUP', 'SEX'], as_index=False).sum()
    df.rename(columns={'POPULATION': 'Population',
                       'AGE_GROUP': 'SORT_INDEX',
                       'SEX': 'Sex'},
              inplace=True)

    df.loc[df.Sex == 'MALE', 'Population'] *= -1
    df['Age group'] = df['SORT_INDEX'].map({value:key for key, value in sort_dict.items()})
    df.sort_values(by=['SORT_INDEX', 'Sex'], ascending=False, inplace=True)

    male = df.loc[df.Sex == 'MALE', 'Population'].values
    female = df.loc[df.Sex == 'FEMALE', 'Population'].values

    return male, female


def main():
    scenario = 'low'
    year = '2099'
    cofips = '06037'  # Montgomery County, Alabama

    df = get_iclusv3_projection(scenario=scenario, year=year, cofips=cofips)
    plot_seaborn(df, cofips)


def plot_seaborn(df, cofips):

    # draw the population pyramid
    g = sns.barplot(data=df,
                    x='Population',
                    y='Age group',
                    hue='Sex',
                    orient='horizontal',
                    dodge=False)

    for p in g.patches:
        p.set_height(1.0)
        p.set_linewidth(0.25)
        p.set_edgecolor('white')

    g.tick_params(left=False)

    male_min = df.loc[df.Sex == 'MALE', 'Population'].min()
    female_max = df.loc[df.Sex == 'FEMALE', 'Population'].max()
    abs_max = max(abs(male_min), female_max)
    g.set_xlim(-abs_max * 1.2, abs_max * 1.2)

    g.set_xlabel('Population')

    plt.gcf().set_figheight(6.0)
    plt.gcf().set_figwidth(8.0)
    g.get_legend().remove()

    plt.figtext(x=0.25, y=0.93, s='Male', fontsize='large')
    plt.figtext(x=0.75, y=0.93, s='Female', fontsize='large')

    plt.tight_layout()
    sns.despine(fig=plt.gcf(), top=True, left=True, right=True)

    labels = g.get_xticklabels()
    for label in labels:
        old_text = label.get_text()
        label.set_text(old_text.replace("\N{MINUS SIGN}", ''))
    g.set_xticklabels(labels)

    # draw the Census 2020 estimate
    census_male, census_female = get_census_historical(cofips=cofips)

    census_vert = []
    census_horiz = []

    for i in range(census_male.shape[0] - 1, -1, -1):
        census_vert.append(i + 0.6)
        census_vert.append(i - 0.4)
        census_horiz.append(census_male[i])
        census_horiz.append(census_male[i])

    for i in range(0, census_male.shape[0]):
        census_vert.append(i - 0.4)
        census_vert.append(i + 0.6)
        census_horiz.append(census_female[i])
        census_horiz.append(census_female[i])

    plt.gca().plot(census_horiz, census_vert, 'black')

    plt.gca().set_ylim(17.5, -0.5)
    plt.subplots_adjust(top=0.925)
    plt.suptitle(t=f'County FIPS: {cofips}')
    plt.show()
    plt.close()

    return


if __name__ == '__main__':
    main()
