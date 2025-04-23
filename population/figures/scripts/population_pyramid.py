import os

import matplotlib.pyplot as plt
import pandas as pd

import seaborn as sns

from sqlalchemy import create_engine

'''
Hauer race groups:
1 = WHITE NH
2 = BLACK NH
3 = HISPANIC
4 = OTHER
'''

SSP = 'SSP3'
HAUER_SSP = 'SSP3'
YEAR = 2020
COFIPS = '38105'


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


def get_Morefield_projection():
    p = 'D:\\OneDrive\\Dissertation\\analysis\\part_5\\outputs'
    f = 'wittgenstein_v2.sqlite'

    engine = create_engine(name_or_url='sqlite:///' + os.path.join(p, f))
    con = engine.connect()

    query = f'SELECT AGE_GROUP, GENDER, "{YEAR}" AS "Population" \
              FROM population_by_race_gender_age_{SSP} \
              WHERE GEOID == "{COFIPS}"'
    df = pd.read_sql(sql=query, con=con)
    con.close()
    engine.dispose()

    df = df.groupby(by=['AGE_GROUP', 'GENDER'], as_index=False).sum()
    df.rename(columns={'AGE_GROUP': 'Age group',
                       'GENDER': 'Sex'},
              inplace=True)

    df.loc[df.Sex == 'MALE', 'Population'] *= -1
    df['SORT_INDEX'] = df['Age group'].map(sort_dict)
    df.sort_values(by=['SORT_INDEX', 'Sex'], ascending=False, inplace=True)

    return df


def get_Hauer_projection():
    p = 'D:\\OneDrive\\Dissertation\\analysis\\part_3\\inputs\\hauer'
    f = 'hauer.sqlite'
    engine = create_engine(name_or_url='sqlite:///' + os.path.join(p, f))
    con = engine.connect()

    query = f'SELECT SEX, AGE, {HAUER_SSP} FROM hauer_ssp \
              WHERE YEAR == {YEAR} \
              AND GEOID == "{COFIPS}"'
    df = pd.read_sql(sql=query, con=con)
    con.close()
    engine.dispose()

    df = df.groupby(by=['SEX', 'AGE'], as_index=False).sum()
    df.rename(columns={HAUER_SSP: 'Population',
                       'AGE': 'SORT_INDEX',
                       'SEX': 'Sex'},
              inplace=True)
    df['Sex'] = df['Sex'].map({1: 'MALE', 2: 'FEMALE'})

    df.loc[df.Sex == 'MALE', 'Population'] *= -1
    df['Age group'] = df['SORT_INDEX'].map({value:key for key, value in sort_dict.items()})
    df.sort_values(by=['SORT_INDEX', 'Sex'], ascending=False, inplace=True)

    return df


def main():

    df1 = get_Morefield_projection()
    plot_seaborn(df1)

    df2 = get_Hauer_projection()
    plot_seaborn(df2)


def get_Census_estimate():
    p = 'D:\\OneDrive\\Dissertation\\databases'
    f = 'population.sqlite'
    t = 'county_population_ageracegender_2010_to_2020'
    engine = create_engine(name_or_url='sqlite:///' + os.path.join(p, f))
    con = engine.connect()

    query = f'SELECT SEX, AGE_GROUP, POPULATION FROM {t} \
              WHERE YEAR == {YEAR} \
              AND COFIPS == "{COFIPS}"'
    df = pd.read_sql(sql=query, con=con)
    con.close()
    engine.dispose()

    df = df.groupby(by=['AGE_GROUP', 'SEX'], as_index=False).sum()
    df.rename(columns={'POPULATION': 'Population',
                       'AGE_GROUP': 'Age group',
                       'SEX': 'Sex'},
              inplace=True)

    df.loc[df.Sex == 'MALE', 'Population'] *= -1
    df.sort_values(by='Age group', ascending=False, inplace=True)

    male = df.loc[df.Sex == 'MALE', 'Population'].values
    female = df.loc[df.Sex == 'FEMALE', 'Population'].values

    return male, female


def plot_seaborn(df):

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
    Census_male, Census_female = get_Census_estimate()

    Census_vert = []
    Census_horiz = []

    for i in range(Census_male.shape[0] - 1, -1, -1):
        Census_vert.append(i + 0.6)
        Census_vert.append(i - 0.4)
        Census_horiz.append(Census_male[i])
        Census_horiz.append(Census_male[i])

    for i in range(0, Census_male.shape[0]):
        Census_vert.append(i - 0.4)
        Census_vert.append(i + 0.6)
        Census_horiz.append(Census_female[i])
        Census_horiz.append(Census_female[i])

    plt.gca().plot(Census_horiz, Census_vert, 'black')

    plt.gca().set_ylim(17.5, -0.5)
    plt.subplots_adjust(top=0.925)
    plt.suptitle(t=f'County FIPS: {COFIPS}')
    plt.show()
    plt.close()

    return


if __name__ == '__main__':
    main()
