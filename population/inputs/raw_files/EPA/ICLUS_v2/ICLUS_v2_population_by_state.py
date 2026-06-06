import os

import pandas as pd

weights_csv = 'D:\\projects\\GLIMPSE\\cy2cbsa_proportions.csv'
weights_df = pd.read_csv(filepath_or_buffer=weights_csv)
weights_df.rename(columns={'CBSAFP10': 'ICLUSGEOID', 'GEOID10': 'COFIPS'}, inplace=True)
weights_df['ICLUSGEOID'] = weights_df['ICLUSGEOID'].astype(str).str.zfill(5)
weights_df['COFIPS'] = weights_df['COFIPS'].astype(str).str.zfill(5)
weights_df = weights_df[['COFIPS', 'ICLUSGEOID', 'CBSA_FRACTION']]


def main():

    # disaggreate ICLUS v2.1 population projections to county level using
    # CBSA-to-county wieghts from 2010
    xls = 'D:\\projects\\GLIMPSE\\ICLUS_v2.1_ssp2_ssp5_conus_nocc.xls'
    df = pd.read_excel(io=xls)
    df.drop(columns=['FID', 'STATE', 'Shape_Leng', 'Shape_Area', 'NAME', 'NCAREG'], inplace=True)
    df['ICLUSGEOID'] = df['ICLUSGEOID'].astype(str).str.zfill(5)

    df = weights_df.merge(right=df, how='left', on='ICLUSGEOID', copy=False)
    df['STFIPS'] = df['COFIPS'].str[:2]
    df.set_index(keys=['COFIPS', 'ICLUSGEOID', 'STFIPS'], inplace=True)

    # this excludes Alaska, Hawaii, Puerto Rico and the independent cities in
    # Virginia, which means that the Virginia populations are slightly
    # underreported
    df = df.loc[~df.isnull().any(axis=1)]

    props = df[['CBSA_FRACTION']]
    df.drop(columns='CBSA_FRACTION', inplace=True)
    props = props.iloc[:, 0]

    df = df.mul(other=props, axis='index')
    df = df.groupby(by='STFIPS').sum()

    df.to_csv(path_or_buf='D:\\projects\\GLIMPSE\\ICLUS_v2.1_SSP2_SSP5_STATE_CONUS_NOCC.csv')

    # disaggreate ICLUS v2.1.1 population projections to county level using
    # CBSA-to-county wieghts from 2010
    csv = 'D:\\projects\\GLIMPSE\\ICLUS_v2_1_1_population_conus_all.csv'
    df = pd.read_csv(filepath_or_buffer=csv)
    df.drop(columns=['OBJECTID', 'ICLUS_NAME', 'STATE', 'NCAREG', 'Shape_Length', 'Shape_Area'], inplace=True)
    df['ICLUSGEOID'] = df['ICLUSGEOID'].astype(str).str.zfill(5)

    df = weights_df.merge(right=df, how='left', on='ICLUSGEOID', copy=False)
    df['STFIPS'] = df['COFIPS'].str[:2]
    df.set_index(keys=['COFIPS', 'ICLUSGEOID', 'STFIPS'], inplace=True)

    # this excludes Alaska, Hawaii, Puerto Rico and the independent cities in
    # Virginia, which means that the Virginia populations are slightly
    # underreported
    df = df.loc[~df.isnull().any(axis=1)]

    props = df[['CBSA_FRACTION']]
    df.drop(columns='CBSA_FRACTION', inplace=True)
    props = props.iloc[:, 0]

    df = df.mul(other=props, axis='index')
    df = df.groupby(by='STFIPS').sum()

    df.to_csv(path_or_buf='D:\\projects\\GLIMPSE\\ICLUS_v2_1_1_population_conus_all.csv')

    return

if __name__ == '__main__':
    main()
