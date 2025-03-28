import sqlite3

import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

sns.set_style('ticks')

conn = sqlite3.connect(
    'D:\\OneDrive\\Dissertation\\databases\\migration.sqlite')
query = 'SELECT ORIGIN_FIPS, DESTINATION_FIPS, DESTINATION_NAME, DESTINATION_STATE, EXEMPTIONS, ORIGIN_YEAR FROM irs_clean'
df = pd.read_sql_query(sql=query, con=conn)
df_acs_2015 = pd.read_sql_query(
    'SELECT D_FIPS, O_FIPS, TOTAL_FLOW, TOTAL_FLOW_MOE from acs_2011_2015', con=conn)
df_acs_2014 = pd.read_sql_query(
    'SELECT D_FIPS, O_FIPS, TOTAL_FLOW, TOTAL_FLOW_MOE from acs_2010_2014', con=conn)
df_acs_2013 = pd.read_sql_query(
    'SELECT D_FIPS, O_FIPS, TOTAL_FLOW, TOTAL_FLOW_MOE from acs_2009_2013', con=conn)
df_acs_2012 = pd.read_sql_query(
    'SELECT D_FIPS, O_FIPS, TOTAL_FLOW, TOTAL_FLOW_MOE from acs_2008_2012', con=conn)
df_acs_2011 = pd.read_sql_query(
    'SELECT D_FIPS, O_FIPS, TOTAL_FLOW, TOTAL_FLOW_MOE from acs_2007_2011', con=conn)
df_acs_2010 = pd.read_sql_query(
    'SELECT D_FIPS, O_FIPS, TOTAL_FLOW, TOTAL_FLOW_MOE from acs_2006_2010', con=conn)
df_acs_2009 = pd.read_sql_query(
    'SELECT D_FIPS, O_FIPS, TOTAL_FLOW, TOTAL_FLOW_MOE from acs_2005_2009', con=conn)


def main():
    irs = get_irs_time_series(fips='51710')


def get_irs_time_series(fips):
    '''build a time series of IRS migration data'''

    in_mig = df.loc[df['DESTINATION_FIPS'] == fips]
    place_name = in_mig.DESTINATION_NAME[0]
    state_name = in_mig.DESTINATION_STATE[0]
    in_mig = in_mig.groupby(
        by=['DESTINATION_FIPS', 'ORIGIN_YEAR'], as_index=False).sum()

    out_mig = df.loc[df['ORIGIN_FIPS'] == fips]

    on = row.iloc[0]['ORIGIN_NAME']
    os = row.iloc[0]['ORIGIN_STATE']
    of = origin_fips
    dn = row.iloc[0]['DESTINATION_NAME']
    ds = row.iloc[0]['DESTINATION_STATE']
    dest_f = destination_fips

    data = sub[['DESTINATION_YEAR', 'EXEMPTIONS']]
    title = u"Origin: {}, {}  ({})\nDestination: {}, {}  ({})".format(
        on, os, of, dn, ds, dest_f)

    return data, title


def get_acs_time_series_averaged(self):
    """
    Build multiple time series of ACS county-to-county migration, returning
    a single time series with migration values averaged over overlapping
    years. Assumes that origin and destination FIPS have already been set.
    """
    temp_list_flows = []
    df_flows = None

    acs_dfs = (df_acs_2015, df_acs_2014, df_acs_2013, df_acs_2012,
               df_acs_2011, df_acs_2010, df_acs_2009)

    acs_y1 = 2011
    for acs_df in acs_dfs:
        row = acs_df[(acs_df['O_FIPS'] == origin_fips) &
                     (acs_df['D_FIPS'] == destination_fips)]
        if row.shape[0] > 0:
            assert len(row) == 1
            cols = [str(i) for i in range(acs_y1, acs_y1 + 5)]
            for col in cols:
                temp_list_flows.append(int(row.iloc[0]['TOTAL_FLOW']))
            if df_flows is None:
                df_flows = pd.DataFrame.from_records(
                    data=[tuple(temp_list_flows)], columns=cols)
            else:
                df_flows_temp = pd.DataFrame.from_records(
                    data=[tuple(temp_list_flows)], columns=cols)
                df_flows = pd.concat(
                    objs=[df_flows, df_flows_temp], ignore_index=True)
                del df_flows_temp
            temp_list_flows = []
        acs_y1 -= 1

    if df_flows is None:
        return None

    zipped = zip(df_flows.columns.values.astype(
        'int'), df_flows.mean().values)
    acs_df = pd.DataFrame.from_records(zipped, columns=['YEAR', 'FLOW'])

    return acs_df


def plot_net_migration(self):

    ofe = origin_fips_entry.text()
    dfe = destination_fips_entry.text()
    if len(ofe) == 5 and len(dfe) == 5:
        data_irs, title = get_irs_time_series(
            origin_fips=ofe, destination_fips=dfe)
    else:
        data_irs, title = get_irs_time_series()
    acs_df = get_acs_time_series_averaged()
    # moe_df = get_acs_moe_time_series()

    # radiation_estimates = get_radiation_projections()

    figure.clear()

    sns.regplot(x='DESTINATION_YEAR', y='EXEMPTIONS', data=data_irs,
                lowess=True, color='#1f77b4', scatter_kws={'zorder': 10})
    if acs_df is not None:
        sns.regplot(x='YEAR', y='FLOW', data=acs_df, lowess=True,
                    color='#ff7f0e', scatter_kws={'zorder': 1}, ax=plt.gca())

        # for i in range(len(moe_df.columns)-1):
        # to_plot = moe_df[['Year', i]][~moe_df[i].isnull()]
    # if acs_df is not None:
        # plt.errorbar(x=acs_df['Year'], y=acs_df['TOTAL_FLOW'], yerr=acs_df['TOTAL_FLOW_MOE'], marker='x', ms=12, ls='None', zorder=1)
    plt.title(label=title, fontsize=25)
    plt.xlim((1980, 2017))
    plt.xlabel(xlabel="")
    plt.ylabel(ylabel="Exemptions", fontsize=25)
    plt.tick_params(axis='both', which='major', labelsize=25)
    plt.tight_layout()

    # refresh canvas
    canvas.draw()

    origin_fips = None
    destination_fips = None

    return


if __name__ == '__main__':
    main()
