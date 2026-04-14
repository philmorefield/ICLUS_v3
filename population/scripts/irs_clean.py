'''
Tables used:    irs_county_to_county
                fips_or_name_changes
                valid_cyfips

Tables created: irs_clean
                irs_ignore_cyfips

The entire IRS county-to-county dataset for 1990-2014 is used. Origin and
destination FIPS and names are changed to reflect actual changes over time, AND
the treatment of OCONUS locations. For example, all of Alaska is treated as a
single origin/destination.

This script also filters records using a table that holds all valid FIPS codes.

20170311  Within-county migrations are excluded. Not sure if we can reliably
extract those values, at least for some years.
'''
import sqlite3

import pandas as pd

pd.set_option('chained_assignment', 'raise')


def find_fips(df, valid_df):
    '''
    Populate the ORIGIN_FIPS and DESTINATION_FIPS columns of a DataFrame. Do
    this by performing a query using:
        - ORIGIN/DESTINATION_NAME and
        - ORIGIN/DESTINATION_STATE

    For various reasons, some FIPS codes can't be looked up in this manner. In
    those cases, FIPS codes are populated using a special "look up" DataFrame
    that only exists in the scope of this function.

    Many records with 'FR' as the destination state have a clearly erroneaous
    destination name, usually the name of the county listed in the preceding
    record. This systematic error is handled in this function.

    This functions is only needed for records through the 1991/92 tax year.

    '''
    look_up_fips_list = [('ADA', 'ID', '16001'),
                         ('ALEUTIAN ISLANDS', 'AK', '02999'),
                         ('ANCHORAGE', 'AK', '02999'),
                         ('BALTIMORE', 'MD', '24005'),
                         ('BARTON', 'GA', '13015'),
                         ('BEDFORD CITY', 'VA', '51019'),
                         ('BILES', 'TN', '47055'),
                         ('CARSON CITY CITY', 'NV', '32510'),
                         ('CHARLOTTESVILLE CI', 'VA', '51540'),
                         ('CHESTER', 'SC', '45023'),
                         ('Clackmas', 'Or', '41005'),
                         ('CLIFTON FORGE CITY', 'VA', '51005'),
                         ('COFFEE', 'KS', '20031'),
                         ('COLLIN', 'TX', '48085'),
                         ('COLONIAL HEIGHTS C', 'VA', '51570'),
                         ('DADE', 'FL', '12086'),
                         ('DE KALB', 'AL', '01049'),
                         ('DE KALB', 'GA', '13089'),
                         ('DE KALB', 'IL', '17037'),
                         ('DE KALB', 'IN', '18033'),
                         ('DE KALB', 'MO', '29063'),
                         ('DE KALB', 'TN', '47041'),
                         ('DE SOTO', 'FL', '12027'),
                         ('DE SOTO', 'MS', '28033'),
                         ('DE WITT', 'TX', '48123'),
                         ('DIFFERENT REGION', None, '59000'),
                         ('DONA ANA', 'NM', '35013'),
                         ('DU PAGE', 'IL', '17043'),
                         ('FAIRFAX', 'VA', '51059'),
                         ('Flager', 'FL', '12035'),
                         ('FOREIGN', 'XX', '57009'),
                         ('FOREIGN', None, '57009'),
                         ('FRANKLIN', 'VA', '51067'),
                         ('FREDERICKSBURG CIT', 'VA', '51630'),
                         ('GARRISON', 'MO', '29081'),
                         ('GRAY', 'TX', '48179'),
                         ('HARRIS', 'TX', '48201'),
                         ('JEFFERSON', 'LA', '22051'),
                         ('KNOW', 'KY', '21121'),
                         ('LACROSSE', 'WI', '55063'),
                         ('LA MOURE', 'ND', '38045'),
                         ('LA PORTE', 'IN', '18091'),
                         ('LA SALLE', 'IL', '17099'),
                         ('La Salle', 'La', '22059'),
                         ('MUSCOGEE CITY', 'GA', '13215'),
                         ('O BRIEN', 'IA', '19141'),
                         ('O@Brien', 'IA', '19141'),
                         ('PRINCE GEORGES', 'MD', '24033'),
                         ('Prince George@s', 'Md', '24033'),
                         ('Pushataha', 'Ok', '40127'),
                         ('Queen Anne@s', 'Md', '24035'),
                         ('QUEEN ANNES', 'MD', '24035'),
                         ('ROANOKE', 'VA', '51161'),
                         ("REGION 1'   NORTH EAST", None, '59001'),
                         ("REGION 1:   NORTHEAST", None, '59001'),
                         ("Region 1'   Northeast", None, '59001'),
                         ("REGION 2'   NORTH CENTRAL", None, '59003'),
                         ("REGION 2:   MIDWEST", None, '59003'),
                         ("REGION 3'   SOUTH", None, '59005'),
                         ("REGION 3:   SOUTH", None, '59005'),
                         ("REGION 4'   WEST", None, '59007'),
                         ("REGION 4:   WEST", None, '59007'),
                         ('SAME REGION, DIFF. STATE', None, '59000'),
                         ('SAME REGION,DIF. STATE', None, '59000'),
                         ('SAME STATE', None, '58000'),
                         ('SHANNON', 'SD', '46102'),
                         ('SOUTH BOSTON CITY', 'VA', '51083'),
                         ('ST. JOHN THE BAPTI', 'LA', '22095'),
                         ('ST. LOUIS', 'MO', '29510'),
                         ('ST. MARYS', 'MD', '24037'),
                         ('St. Mary@s', 'Md', '24037'),
                         ('WASH.,D.C.', 'DC', '11001'),
                         ('VIRGINIA BEACH CIT', 'VA', '51810'),
                         ('WILL', 'IL', '17197')]

    columns = ['NAME', 'STATE', 'NEW_FIPS']

    new_fips_df = pd.DataFrame(look_up_fips_list, columns=columns)

    df.loc[df['ORIGIN_STATE'].isin(['AK', 'Ak', 'ak']), 'ORIGIN_FIPS'] = '02999'
    df.loc[df['ORIGIN_STATE'].isin(['AK', 'Ak', 'ak']), 'ORIGIN_NAME'] = 'ALASKA'
    df.loc[df['DESTINATION_STATE'].isin(['AK', 'Ak', 'ak']), 'DESTINATION_FIPS'] = '02999'
    df.loc[df['DESTINATION_STATE'].isin(['AK', 'Ak', 'ak']), 'DESTINATION_NAME'] = 'ALASKA'

    df.loc[df['ORIGIN_STATE'].isin(['HI', 'Hi', 'hi']), 'ORIGIN_FIPS'] = '15999'
    df.loc[df['ORIGIN_STATE'].isin(['HI', 'Hi', 'hi']), 'ORIGIN_NAME'] = 'HAWAII'
    df.loc[df['DESTINATION_STATE'].isin(['HI', 'Hi', 'hi']), 'DESTINATION_FIPS'] = '15999'
    df.loc[df['DESTINATION_STATE'].isin(['HI', 'Hi', 'hi']), 'DESTINATION_NAME'] = 'HAWAII'

    # populate origin fips using 'look_up'
    df['origin_name_upper'] = df['ORIGIN_NAME'].str.upper()
    df['origin_state_upper'] = df['ORIGIN_STATE'].str.upper()
    new_fips_df['name_upper'] = new_fips_df['NAME'].str.upper()
    new_fips_df['state_upper'] = new_fips_df['STATE'].str.upper()
    df = df.merge(right=new_fips_df,
                  how='left',
                  left_on=['origin_name_upper', 'origin_state_upper'],
                  right_on=['name_upper', 'state_upper'],
                  copy=False)

    df.loc[~df['NEW_FIPS'].isnull(), 'ORIGIN_FIPS'] = df['NEW_FIPS']
    df.drop(labels=['origin_name_upper', 'origin_state_upper', 'NAME', 'STATE', 'NEW_FIPS', 'name_upper', 'state_upper'], axis=1, inplace=True)

    # populate destination fips using 'look_up'
    df['destination_name_upper'] = df['DESTINATION_NAME'].str.upper()
    df['destination_state_upper'] = df['DESTINATION_STATE'].str.upper()
    df = df.merge(right=new_fips_df,
                  how='left',
                  left_on=['destination_name_upper', 'destination_state_upper'],
                  right_on=['name_upper', 'state_upper'],
                  copy=False)

    df.loc[~df['NEW_FIPS'].isnull(), 'DESTINATION_FIPS'] = df['NEW_FIPS']
    df.drop(labels=['destination_name_upper', 'destination_state_upper', 'NAME', 'STATE', 'NEW_FIPS', 'name_upper', 'state_upper'], axis=1, inplace=True)

    # populate origin fips using 'valid_df'
    df['origin_name_lower'] = df['ORIGIN_NAME'].str.lower()
    df['origin_state_lower'] = df['ORIGIN_STATE'].str.lower()
    valid_df['cyname_lower'] = valid_df['CYNAME'].str.lower().str.replace(' county', '').str.replace(' parish', '')
    valid_df['stusps_lower'] = valid_df['STUSPS'].str.lower()

    df = df.merge(right=valid_df,
                  how='left',
                  left_on=['origin_name_lower', 'origin_state_lower'],
                  right_on=['cyname_lower', 'stusps_lower'],
                  copy=False)

    df.loc[~df['CYFIPS'].isnull(), 'ORIGIN_FIPS'] = df['CYFIPS']
    df.drop(labels=['CYNAME', 'STUSPS', 'CYFIPS', 'origin_name_lower', 'origin_state_lower', 'cyname_lower', 'stusps_lower'], axis=1, inplace=True)

    # populate destination fips using 'valid_df'
    df['destination_name_lower'] = df['DESTINATION_NAME'].str.lower()
    df['destination_state_lower'] = df['DESTINATION_STATE'].str.lower()
    df = df.merge(right=valid_df,
                  how='left',
                  left_on=['destination_name_lower', 'destination_state_lower'],
                  right_on=['cyname_lower', 'stusps_lower'],
                  copy=False)

    df.loc[~df['CYFIPS'].isnull(), 'DESTINATION_FIPS'] = df['CYFIPS']
    df.drop(labels=['CYNAME', 'STUSPS', 'CYFIPS', 'destination_name_lower', 'destination_state_lower', 'cyname_lower', 'stusps_lower'], axis=1, inplace=True)

    # correct records that have 'FR' as the state
    df.loc[df['DESTINATION_STATE'] == 'FR', 'DESTINATION_FIPS'] = '57009'
    df.loc[df['DESTINATION_STATE'] == 'FR', 'DESTINATION_NAME'] = 'FOREIGN'

    df.loc[df['DESTINATION_NAME'].str.lower() == 'county non-migrants', 'DESTINATION_FIPS'] = df['ORIGIN_FIPS']
    df.loc[df['DESTINATION_NAME'].str.lower() == 'county non-migrants', 'DESTINATION_STATE'] = df['ORIGIN_STATE']
    df.loc[df['DESTINATION_NAME'].str.lower() == 'county non-migrants', 'DESTINATION_NAME'] = df['ORIGIN_NAME']

    df.loc[df['DESTINATION_NAME'] == 'All Flows', 'DESTINATION_FIPS'] = 59999
    df.loc[df['DESTINATION_NAME'] == 'All Flows', 'DESTINATION_NAME'] = 'ALL OTHER FLOWS'

    df.loc[df['DESTINATION_NAME'] == 'ALL MIGRATION FLOWS', 'DESTINATION_FIPS'] = 59999
    df.loc[df['DESTINATION_NAME'] == 'ALL MIGRATION FLOWS', 'DESTINATION_NAME'] = 'ALL OTHER FLOWS'

    df.loc[df['DESTINATION_NAME'] == 'All Other Flows', 'DESTINATION_FIPS'] = 59999
    df.loc[df['DESTINATION_NAME'] == 'All Other Flows', 'DESTINATION_NAME'] = 'ALL OTHER FLOWS'

    df.loc[df['DESTINATION_NAME'] == 'ALL OTHER FLOWS', 'DESTINATION_FIPS'] = 59999
    df.loc[df['DESTINATION_NAME'] == 'ALL OTHER FLOWS', 'DESTINATION_NAME'] = 'ALL OTHER FLOWS'

    '''
    1980/81 - 'ALL OTHER FLOWS' is the sum of suppressed flows, by origin
    1983/84 - ibid
    1984/85 - ibid
    1985/86 - 'All Flows' is the sum of suppressed flows, by origin
    1986/87 - ibid
    1987/88 - ibid
    1988/89 - ibid
    1989/90 - ibid
    1990/91 - 'ALL MIGRATION FLOWS' is the sum of suppressed flows, by origin
    1991/92 - ibid
    '''

    assert len(df.loc[df['ORIGIN_FIPS'].isnull(), 'ORIGIN_NAME'].unique()) == 0
    assert len(df.loc[df['DESTINATION_FIPS'].isnull(), 'DESTINATION_NAME'].unique()) == 0

    return df


def main():

    con = sqlite3.connect('D:\\OneDrive\\Dissertation\\databases\\migration.sqlite')

    df = pd.read_sql('SELECT * FROM irs_raw_import', con=con)
    df = df[df['EXEMPTIONS'] != -1]  # suppressed flows
    df.dropna(thresh=1, subset=['EXEMPTIONS', 'EXEMPTIONSY1', 'EXEMPTIONSY2'], inplace=True)

    # a table with updated names and fips codes
    change_df = pd.read_sql('SELECT * FROM fips_or_name_changes', con=con)
    # a table holding all of the acceptable fips codes
    valid_df = pd.read_sql('SELECT * FROM valid_cyfips', con=con)

    no_fips_df = df.loc[(df['ORIGIN_FIPS'].isnull()) | (df['DESTINATION_FIPS'].isnull()), :].copy()
    df = df.loc[~(df['ORIGIN_FIPS'].isnull()) & ~(df['DESTINATION_FIPS'].isnull()), :]

    corrected_df = find_fips(no_fips_df, valid_df)

    df = pd.concat(objs=(df, corrected_df), ignore_index=True, verify_integrity=True)

    # years 1992 thru 1994 use 63050 to denote non-migrants
    df.loc[df['DESTINATION_FIPS'] == '63050', 'DESTINATION_STATE'] = None
    df.loc[df['DESTINATION_FIPS'] == '63050', 'DESTINATION_NAME'] = None
    df.loc[df['DESTINATION_FIPS'] == '63050', 'DESTINATION_FIPS'] = df['ORIGIN_FIPS']

    # update all origin fips codes and names as needed
    df = df.merge(right=change_df,
                  how='outer',
                  left_on='ORIGIN_FIPS',
                  right_on='OLD_FIPS',
                  copy=False)
    df.loc[~df['NEW_FIPS'].isnull(), 'ORIGIN_FIPS'] = df['NEW_FIPS']
    df.loc[~df['NEW_NAME'].isnull(), 'ORIGIN_NAME'] = df['NEW_NAME']
    df.loc[~df['NEW_STUSPS'].isnull(), 'ORIGIN_STATE'] = df['NEW_STUSPS']
    df.drop(labels=['OLD_FIPS', 'NEW_FIPS', 'NEW_NAME', 'NEW_STUSPS'], axis=1, inplace=True)

    # update all destination fips codes and names as needed
    df = df.merge(right=change_df,
                  how='outer',
                  left_on='DESTINATION_FIPS',
                  right_on='OLD_FIPS',
                  copy=False)
    df.loc[~df['NEW_FIPS'].isnull(), 'DESTINATION_FIPS'] = df['NEW_FIPS']
    df.loc[~df['NEW_NAME'].isnull(), 'DESTINATION_NAME'] = df['NEW_NAME']
    df.loc[~df['NEW_STUSPS'].isnull(), 'DESTINATION_STATE'] = df['NEW_STUSPS']
    df.drop(labels=['OLD_FIPS', 'NEW_FIPS', 'NEW_NAME', 'NEW_STUSPS'], axis=1, inplace=True)

    # standardize fips and names for Alaska, Hawaii, Puerto Rico, etc., which we
    # treat as if their respective counties make up a single origin/destination
    df = df.merge(right=valid_df,
                  how='outer',
                  left_on='ORIGIN_FIPS',
                  right_on='CYFIPS',
                  copy=False)

    df.loc[~df['CYFIPS'].isnull(), 'ORIGIN_NAME'] = df['CYNAME']
    df.loc[~df['CYFIPS'].isnull(), 'ORIGIN_STATE'] = df['STUSPS']
    df.drop(labels=['CYFIPS', 'CYNAME', 'STUSPS'], axis=1, inplace=True)

    df = df.merge(right=valid_df,
                  how='outer',
                  left_on='DESTINATION_FIPS',
                  right_on='CYFIPS',
                  copy=False)

    df.loc[~df['CYFIPS'].isnull(), 'DESTINATION_NAME'] = df['CYNAME']
    df.loc[~df['CYFIPS'].isnull(), 'DESTINATION_STATE'] = df['STUSPS']
    df.drop(labels=['CYFIPS', 'CYNAME', 'STUSPS'], axis=1, inplace=True)

    # collapse counties that have been merged over time; this does NOT include the
    # independent cities of Virginia, or MSAs
    sum_columns = ['ORIGIN_FIPS', 'ORIGIN_NAME', 'ORIGIN_STATE', 'DESTINATION_FIPS', 'DESTINATION_NAME', 'DESTINATION_STATE', 'ORIGIN_YEAR', 'DESTINATION_YEAR']
    df = df.groupby(by=sum_columns, as_index=False).sum()
    # df = df[df['ORIGIN_FIPS'] != df['DESTINATION_FIPS']]

    # make a sqlite table of all the 'bad' origin and destination records
    bad_origins = df[~df['ORIGIN_FIPS'].isin(valid_df['CYFIPS'])][['ORIGIN_FIPS']].drop_duplicates()
    bad_origins.columns = ['FIPS']
    bad_destinations = df[~df['DESTINATION_FIPS'].isin(valid_df['CYFIPS'])][['DESTINATION_FIPS']].drop_duplicates()
    bad_destinations.columns = ['FIPS']
    bad_all = pd.concat(objs=[bad_origins, bad_destinations], ignore_index=True).drop_duplicates().sort_values(by='FIPS')

    # this is basically for QA; this table holds all of the FIPS codes I'm
    # ignoring
    bad_all.to_sql(name='irs_ignore_cyfips',
                   con=con,
                   if_exists='replace',
                   index=False)

    print("Loaded 'irs_ignore_cyfips' into sqlite database...")

    # remove all the bad origins and destinations and load the query into sqlite
    df = df[~df['ORIGIN_FIPS'].isin(bad_all['FIPS'])]
    df = df[~df['DESTINATION_FIPS'].isin(bad_all['FIPS'])]

    df.sort_values(by=['ORIGIN_YEAR', 'ORIGIN_FIPS', 'DESTINATION_FIPS'], inplace=True)

    df.to_sql(name='irs_clean',
              con=con,
              if_exists='replace',
              index=False)

    print("Loaded 'irs_clean' into sqlite database...")


if __name__ == '__main__':
    main()
