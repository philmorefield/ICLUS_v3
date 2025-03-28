import sqlite3
import seaborn
import sys
from PyQt5.QtWidgets import QDialog, QApplication, QPushButton, QGridLayout, QLineEdit
from PyQt5.QtCore import QCoreApplication

from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

sns.set_style('ticks')

conn = sqlite3.connect(
    'D:\\OneDrive\\Dissertation\\databases\\migration.sqlite')
df = pd.read_sql_query('SELECT * FROM irs_clean', con=conn)
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


class Window(QDialog):
    def __init__(self, parent=None):
        super(Window, self).__init__(parent)

        self.origin_fips = None
        self.destination_fips = None

        # a figure instance to plot on
        self.figure = plt.figure(figsize=(10, 8))

        # this is the Canvas Widget that displays the `figure`
        # it takes the `figure` instance as a parameter to __init__
        self.canvas = FigureCanvas(self.figure)

        # this is the Navigation widget
        # it takes the Canvas widget and a parent
        self.toolbar = NavigationToolbar(self.canvas, self)

        # Just some button connected to `plot` method
        self.random_button = QPushButton('Plot a random flow')
        self.random_button.clicked.connect(self.clear_then_plot)

        self.origin_fips_entry = QLineEdit()
        self.destination_fips_entry = QLineEdit()
        self.new_button = QPushButton("Plot this flow")
        self.new_button.clicked.connect(self.plot)
        self.quit_button = QPushButton("Quit")
        self.quit_button.clicked.connect(QCoreApplication.instance().quit)

        # set the layout
        layout = QGridLayout()
        layout.addWidget(self.toolbar, 1, 1, 1, 5)
        layout.addWidget(self.canvas, 2, 1, 10, 5)
        layout.addWidget(self.random_button, 2, 6, 1, 2)
        layout.addWidget(self.origin_fips_entry, 5, 6, 1, 2)
        layout.addWidget(self.destination_fips_entry, 6, 6, 1, 2)
        layout.addWidget(self.new_button, 7, 6, 1, 2)
        layout.addWidget(self.quit_button, 11, 6, 1, 2)

        self.setLayout(layout)

    def clear_then_plot(self):
        self.origin_fips_entry.setText('')
        self.destination_fips_entry.setText('')

        self.plot()

        return

    def get_irs_time_series(self, fips='51710'):
        '''build a time series of IRS migration data'''

        self.fips = fips
        self.in_mig = df.loc[df['DESTINATION_FIPS'] ==
                             self.fips].groupby(by='DESTINATION_FIPS').sum()
        self.out_mig = df.loc[df['ORIGIN_FIPS'] ==
                              self.fips].groupby(by='ORIGIN_FIPS').sum()

        on = row.iloc[0]['ORIGIN_NAME']
        os = row.iloc[0]['ORIGIN_STATE']
        of = self.origin_fips
        dn = row.iloc[0]['DESTINATION_NAME']
        ds = row.iloc[0]['DESTINATION_STATE']
        dest_f = self.destination_fips

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
            row = acs_df[(acs_df['O_FIPS'] == self.origin_fips) &
                         (acs_df['D_FIPS'] == self.destination_fips)]
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

    def get_acs_moe_time_series(self):
        """
        Build multiple time series of ACS county-to-county migration, returning
        a single time series with migration values averaged over overlapping
        years. Assumes that origin and destination FIPS have already been set.
        """
        temp_list_moe = []
        df_moe = None

        acs_dfs = (df_acs_2015, df_acs_2014, df_acs_2013, df_acs_2012,
                   df_acs_2011, df_acs_2010, df_acs_2009)

        acs_y1 = 2011
        for acs_df in acs_dfs:
            row = acs_df[(acs_df['O_FIPS'] == self.origin_fips) &
                         (acs_df['D_FIPS'] == self.destination_fips)]
            if row.shape[0] > 0:
                assert len(row) == 1
                cols = [str(i) for i in range(acs_y1, acs_y1 + 5)]
                for col in cols:
                    temp_list_moe.append(int(row.iloc[0]['TOTAL_FLOW_MOE']))
                if df_moe is None:
                    df_moe = pd.DataFrame.from_records(
                        data=[tuple(temp_list_moe)], columns=cols)
                else:
                    df_moe_temp = pd.DataFrame.from_records(
                        data=[tuple(temp_list_moe)], columns=cols)
                    df_moe = pd.concat(
                        objs=[df_moe, df_moe_temp], ignore_index=True)
                    del df_moe_temp
                temp_list_moe = []
            acs_y1 -= 1

        if df_moe is None:
            return None

        df_moe = df_moe.T.reset_index()
        df_moe.rename({'index': 'Year'}, axis=1, inplace=True)
        return df_moe

    # def get_radiation_projections(self, start_year=1980, end_year=2015):

    #     conn = sqlite3.connect(
    #         'C:\\Users\\Phil\\OneDrive\\Dissertation\\analysis\\analysis_db.sqlite')
    #     query = 'SELECT Tij FROM {} WHERE ORIGIN_FIPS == "{}" AND DESTINATION_FIPS == "{}"'
    #     list_of_tuples = []
    #     for year in range(start_year, end_year):
    #         if year in (1981, 1982):
    #             continue
    #         table = 'radiation_model_{}_predictions_part_1'.format(year)
    #         flow = pd.read_sql_query(query.format(
    #             table, self.origin_fips, self.destination_fips), con=conn).squeeze()
    #         if isinstance(flow, np.int64):
    #             list_of_tuples.append((year, flow))

    #     conn.close()
    #     print(f"\n{list_of_tuples}\n")
    #     radiation_estimates = pd.DataFrame(
    #         list_of_tuples, columns=('YEAR', 'FLOW'))

    #     return radiation_estimates

    def plot(self):

        ofe = self.origin_fips_entry.text()
        dfe = self.destination_fips_entry.text()
        if len(ofe) == 5 and len(dfe) == 5:
            data_irs, title = self.get_irs_time_series(
                origin_fips=ofe, destination_fips=dfe)
        else:
            data_irs, title = self.get_irs_time_series()
        acs_df = self.get_acs_time_series_averaged()
        # moe_df = self.get_acs_moe_time_series()

        # radiation_estimates = self.get_radiation_projections()

        self.figure.clear()

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
        self.canvas.draw()

        self.origin_fips = None
        self.destination_fips = None

        return


if __name__ == '__main__':
    app = QApplication(sys.argv)

    main = Window()
    main.show()

    sys.exit(app.exec_())
