# -*- coding: utf-8 -*-
"""
This is a script for displaying state-level population growths according to
different migration scenarios.

@author: Hamidreza.Zoraghein
"""
####################################################
#Import modules
import os
import glob
import pandas as pd
import plotly.io as pio

####################################################
#Variables
base_folder      = r"C:\Users\hzoraghein\Google Drive\Sensitivity_Analysis\Bilateral"
scenario         = "SSP5"
period           = "10100"
cur_proj_results = os.path.join(base_folder, scenario)


####################################################
#Main program

#Construct the addresses, csv files and indices for input population tables
state_adds = [add[0] for add in os.walk(cur_proj_results)][1:]
full_csvs  = [glob.glob(os.path.join(add, "*_pop.csv"))[0] for add in state_adds]
indexes    = [int(csv_add.split("\\")[-2][:-3]) for csv_add in full_csvs]

#Create a blank population dataframe holding states and their aggregate populations in 2010, 2050 and 2100 
pop_df = pd.DataFrame(index = indexes, columns = ["state", "Pop_2010", "Pop_2050", "Pop_2100"])
pop_df = pop_df.fillna(0)

#Loop through csv tables and fill the population table with corresponding values
for state_csv in full_csvs:
    index = int(state_csv.split("\\")[-2][:-3])
    pop_df.loc[index, "state"]    = state_csv.split("\\")[-1].split("_")[0][-2:]
    pop_df.loc[index, "Pop_2010"] = pd.read_csv(state_csv).loc[:, "2010"].sum()
    pop_df.loc[index, "Pop_2050"] = pd.read_csv(state_csv).loc[:, "2050"].sum()
    pop_df.loc[index, "Pop_2100"] = pd.read_csv(state_csv).loc[:, "2100"].sum()

#Calculate percentage of population change for the 2010-2050 abd 2010-2100 periods    
pop_df["change_1050"]  = ((pop_df["Pop_2050"] - pop_df["Pop_2010"]) / pop_df["Pop_2010"]) * 100
pop_df["change_10100"] = ((pop_df["Pop_2100"] - pop_df["Pop_2010"]) / pop_df["Pop_2010"]) * 100

#Categorize population change columns 
#From 2010 to 2050
pop_df['class_1050'] = pd.cut(pop_df.change_1050, 
                              bins = [-999, -35, -20, -5, 5, 20, 35, 50, 75, 100, 999], 
                              labels = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                              include_lowest = True)

#From 2010 to 2100
pop_df['class_10100'] = pd.cut(pop_df.change_10100, 
                              bins = [-999, -35, -20, -5, 5, 20, 35, 50, 75, 100, 999], 
                              labels = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                              include_lowest = True)

#Construct the colorscale for plotting
scl = [[0, 'brown'], [0.1, 'brown'], [0.1, 'rgb(255,70,0)'], [0.2, 'rgb(255,70,0)'],
        [0.2, 'rgb(251,154,153)'], [0.3, 'rgb(251,154,153)'], [0.3, 'rgb(170, 170, 170)'], 
        [0.4, 'rgb(170, 170, 170)'], [0.4, 'rgb(255,220,0)'], [0.5, 'rgb(255,220,0)'], [0.5, 'palegreen'],
        [0.6, 'palegreen'], [0.6, 'rgb(73,252,28)'], [0.7, 'rgb(73,252,28)'],
        [0.7, 'rgb(40,202,0)'], [0.8, 'rgb(40,202,0)'], [0.8, 'rgb(31,137,5)'], [0.9, 'rgb(31,137,5)'],
        [0.9, 'darkgreen'], [1, 'darkgreen']]

#Variables for the plot title
if   period == "1050": duration = "2010-2050"
elif period == "10100": duration = "2010-2100"

if   scenario == "No_Mig"        : scenario_title = "No  Migration"
elif scenario == "Zero_Int_Mig"  : scenario_title = "No International Migration"
elif scenario == "Zero_Dom_Mig"  : scenario_title = "No Domestic Migration"
elif scenario == "Reg_Scenario"  : scenario_title = "Baseline"
elif scenario == "Half_Scenario" : scenario_title = "Half Domestic Migration"
elif scenario == "Doub_Scenario" : scenario_title = "Double Domestic Migration"
elif scenario == "SSP2"          : scenario_title = "SSP2"
elif scenario == "SSP3"          : scenario_title = "SSP3"
elif scenario == "SSP5"          : scenario_title = "SSP5"

#Create the necessary elements for the plotly plotting
data = [ dict(
        type = 'choropleth',
        colorscale = scl,
        autocolorscale = False,
        locations = pop_df['state'],
        z = pop_df['class_{0}'.format(period)].append(pd.Series([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])),
        locationmode = 'USA-states',
        marker = dict(
            line = dict (
                color = 'rgb(120,120,120)',
                width = 1
            ) ),
        colorbar = dict(
            title = "% Population Change",
            titlefont = dict(size = 21),
            x = 0.93,
            xpad = 37,
            len = 0.75,
            yanchor = "middle",
            tickmode = "array",
            tickvals = [0.45, 1.3, 2.2, 3.1, 4.0, 4.9, 5.8, 6.7, 7.6, 8.5],
            ticktext = ["< -35", "-35 - -20", "-20 - -5", "-5 - 5", "5 - 20",
                        "20 -35", "35 - 50", "50 - 75", "75 - 100", "> 100"],
            tickfont = dict(
                    size  = 19,
                    color = 'black')
                )
        ) ]

layout = dict(
        title  = dict(text = 'Percentage of Population Change ({0}) According to <br> "{1}" Scenario'.format(duration, scenario_title),
                      x = 0.5, xanchor = "center"),
        font = dict(size = 23, family = "Arial"),
        width  = 1400,
        height = 780,
        margin = dict(l=0, r=0, b=10, t=100, pad=10),
        geo = dict(
            scope = "usa",
            showlakes = False,
            projection = dict(type = "albers usa"),
            center = dict(
                    lon = -94, lat = 38),
            lonaxis = dict(
                    range = [-120, -60]),
            lataxis = dict(
                    range = [21, 50])
            )
       )

#XOnstruct the figure and save it according to the scenario              
fig = dict(data = data, layout = layout)  
pio.write_image(fig, os.path.join(cur_proj_results, "{0}_{1}.jpg".format(scenario, period)))




