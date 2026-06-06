# -*- coding: utf-8 -*-
"""
This is a script for displaying state-level age averages according to
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
scenario         = "SSP2"
year             = 2010
cur_proj_results = os.path.join(base_folder, scenario)


####################################################
#Main program

#Construct the addresses, csv files and indices for input population tables
state_adds = [add[0] for add in os.walk(cur_proj_results)][1:]
full_csvs  = [glob.glob(os.path.join(add, "*_pop.csv"))[0] for add in state_adds]
indexes    = [int(csv_add.split("\\")[-2][:-3]) for csv_add in full_csvs]

#Create a blank population dataframe holding states and their aggregate populations in 2010, 2050 and 2100 
pop_df = pd.DataFrame(index = indexes)
pop_df = pop_df.fillna(0)


#Loop through csv tables and fill the population table with corresponding values
for state_csv in full_csvs:
    index = int(state_csv.split("\\")[-2][:-3])
    pop_df.loc[index, "state"] = state_csv.split("\\")[-1].split("_")[0][-2:]
    cur_proj_df                = pd.read_csv(state_csv)
    
    
    # Calculate mean ages in 2010, 2050 and 2100
    cur_age_2010  = cur_proj_df[["age", "2010"]].groupby(["age"]).sum()
    age_2010_mean = (cur_age_2010.index.values * cur_age_2010["2010"].values).sum() / cur_age_2010["2010"].values.sum()
    
    cur_age_2050  = cur_proj_df[["age", "2050"]].groupby(["age"]).sum()
    age_2050_mean = (cur_age_2050.index.values * cur_age_2050["2050"].values).sum() / cur_age_2050["2050"].values.sum()
    
    cur_age_2100  = cur_proj_df[["age", "2100"]].groupby(["age"]).sum()
    age_2100_mean = (cur_age_2100.index.values * cur_age_2100["2100"].values).sum() / cur_age_2100["2100"].values.sum()
    
    
    # Estimate age-related proportions
    pop_df.loc[index, "mean_age_2010"] = age_2010_mean 
    pop_df.loc[index, "mean_age_2050"] = age_2050_mean
    pop_df.loc[index, "mean_age_2100"] = age_2100_mean 


#Categorize population columns 
#2010
pop_df['class_2010'] = pd.cut(pop_df["mean_age_2010"], 
                              bins = [30, 35, 40, 45, 50, 55, 999], 
                              labels = [0, 1, 2, 3, 4, 5],
                              include_lowest = True)

#2050
pop_df['class_2050'] = pd.cut(pop_df["mean_age_2050"], 
                              bins = [30, 35, 40, 45, 50, 55, 999], 
                              labels = [0, 1, 2, 3, 4, 5],
                              include_lowest = True)

#2100
pop_df['class_2100'] = pd.cut(pop_df["mean_age_2100"], 
                              bins = [30, 35, 40, 45, 50, 55, 999], 
                              labels = [0, 1, 2, 3, 4, 5],
                              include_lowest = True)



#Construct the colorscale for plotting
scl = [[0, 'rgb(215,225,252)'], [0.17, 'rgb(215,225,252)'], [0.17, 'rgb(127,160,249)'], [0.33, 'rgb(127,160,249)'],
        [0.33, 'rgb(81,124,242)'], [0.50, 'rgb(81,124,242)'], [0.50, 'rgb(38, 82, 205)'], 
        [0.67, 'rgb(38, 82, 205)'], [0.67, 'rgb(26,59,149)'], [0.83, 'rgb(26,59,149)'],
        [0.83, 'rgb(2,14,48)'], [1, 'rgb(2,14,48)']]


#Variables for the plot title
if   scenario == "No_Mig"        : scenario_title = "No  Migration"
elif scenario == "Zero_Int_Mig"  : scenario_title = "No International Migration"
elif scenario == "Zero_Dom_Mig"  : scenario_title = "No Domestic Migration"
elif scenario == "Reg_Scenario"  : scenario_title = "Baseline"
elif scenario == "Half_Scenario" : scenario_title = "Half Domestic Migration"
elif scenario == "Doub_Scenario" : scenario_title = "Double Domestic Migration"
elif scenario == "SSP2"          : scenario_title = "SSP2"
elif scenario == "SSP3"          : scenario_title = "SSP3"
elif scenario == "SSP5"          : scenario_title = "SSP5"



# Write the resulting table to a csv file
pop_df.to_csv(os.path.join(cur_proj_results, "{}_{}_Mean_Age.csv".format(scenario, year)))


#Create the necessary elements for the plotly plotting
data = [ dict(
        type = 'choropleth',
        colorscale = scl,
        autocolorscale = False,
        locations = pop_df['state'],
        z = pop_df['class_{0}'.format(year)].append(pd.Series([0, 1, 2, 3, 4, 5])),
        locationmode = 'USA-states',
        marker = dict(
            line = dict (
                color = 'rgb(120,120,120)',
                width = 1.5
            ) ),
        colorbar = dict(
            title = "% of Population",
            titlefont = dict(size = 21),
            x = 0.93,
            xpad = 37,
            len = 0.75,
            yanchor = "middle",
            tickmode = "array",
            tickvals = [0.4, 1.2, 2.1, 2.9, 3.8, 4.6],
            ticktext = ["30 - 35", "35 - 40", "40 - 45", "45 - 50", "50 - 55", "> 55"],
            tickfont = dict(
                    size  = 19,
                    color = 'black')
                )
        ) ]

layout = dict(
        title  = dict(text = 'Mean Population Age in {} According to <br> "{}" Scenario'.format(year, scenario_title),
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
pio.write_image(fig, os.path.join(cur_proj_results, "{}_{}_Mean_Age.jpg".format(scenario, year)))




