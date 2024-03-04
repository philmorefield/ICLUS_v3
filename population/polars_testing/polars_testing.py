import os
import sqlite3
import timeit

import pandas as pd
import polars as pl


db = 'D:/projects/ICLUS_v3/population/polars_testing/polars_test.sqlite'



def main():

    setup = 'from __main__ import join_polars'
    timer = timeit.timeit(stmt='join_polars()', setup=setup, number=100)
    print("\n\tPolars join:", timer, "\n")

    setup = 'from __main__ import join_pandas'
    timer = timeit.timeit(stmt='join_pandas()', setup=setup, number=100)
    print("\tPandas join:", timer, "\n")


def join_pandas():
    # join baseline_migration to net_migration_total
    con = sqlite3.connect(database=db)
    baseline = pd.read_sql(sql='SELECT * FROM baseline_migration',
                           con=con)
    net = pd.read_sql(sql='SELECT * FROM net_migration',
                      con=con)
    con.close()

    net.merge(right=baseline, on='GEOID')


def join_polars():

    con = sqlite3.connect(database=db)
    baseline_pd = pd.read_sql(sql='SELECT * FROM baseline_migration',
                           con=con)
    net_pd = pd.read_sql(sql='SELECT * FROM net_migration',
                      con=con)
    con.close()

    baseline = pl.from_pandas(df=baseline_pd)
    net = pl.from_pandas(df=net_pd)

    net.join(other=baseline, on='GEOID', how='left')


if __name__ == '__main__':
    main()