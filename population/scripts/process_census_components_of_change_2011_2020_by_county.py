from base64 import encode
import os
import sqlite3

import pandas as pd

if os.path.isdir('D:\\OneDrive\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\OneDrive\\ICLUS_v3\\population'
elif os.path.isdir('D:\\projects\\ICLUS_v3\\population'):
    BASE_FOLDER = 'D:\\projects\\ICLUS_v3\\population'
else:
    raise Exception


POPULATION_DB = os.path.join(BASE_FOLDER, 'inputs', 'databases', 'population.sqlite')
CENSUS_CSV_FILES = os.path.join(BASE_FOLDER, 'inputs\\raw_files\\Census')


def main():
    df = None
    for year in range(2011, 2021):
        csv = os.path.join(CENSUS_CSV_FILES, str(year), f'co-est{year}-alldata.csv')
        result = pd.read_csv(filepath_or_buffer=csv, encoding='latin1')

        # DEATHS: create annual time series by county
        temp = result.query('SUMLEV == 50')
        columns = ['STATE', 'COUNTY',

        ...




if __name__ == '__main__':
    main()