# -*- coding: utf-8 -*-
"""
Created on Wed Feb 27 14:30 2018
This is a script for displaying U.S. national projected population grids under 
different SSPs upscaled to state boundaries. 

@author: Hamidreza.Zoraghein
"""
####################################################
#Import modules
import os
import pandas as pd
import plotly.tools as tls
import plotly.io as pio

####################################################
#Variables
base_folder      = r"C:\Users\hzoraghein\Google Drive\sensitivity_analysis\Bilateral"
upscaled_pop_csv = os.path.join(base_folder, "AllStatesProjection.csv")
scenario         = "SSP2"
period           = "10100"
cur_proj_results = os.path.join(base_folder, scenario)
states           = ["9-Connecticut", "23-Maine", "25-Massachusetts", "33-New_Hampshire", "44-Rhode_Island",
                    "50-Vermont", "34-New_Jersey", "36-New_York", "42-Pennsylvania", "17-Illinois", "18-Indiana",
                    "26-Michigan", "39-Ohio", "55-Wisconsin", "19-Iowa", "20-Kansas", "27-Minnesota", "29-Missouri",
                    "31-Nebraska", "38-North_Dakota", "46-South_Dakota", "10-Delaware", "11-DC", "12-Florida",
                    "13-Georgia", "24-Maryland", "37-North_Carolina", "45-South_Carolina", "51-Virginia", "54-West_Virginia",
                    "1-Alabama", "21-Kentucky", "28-Mississippi", "47-Tennessee", "5-Arkansas", "22-Louisiana",
                    "40-Oklahoma", "48-Texas", "4-Arizona", "8-Colorado", "16-Idaho", "30-Montana", "32-Nevada",
                    "35-New_Mexico", "49-Utah", "56-Wyoming", "2-Alaska", "6-California", "15-Hawaii", "41-Oregon",
                    "53-Washington"]

state_abbs      = ["9-CT", "23-ME", "25-MA", "33-NH", "44-RI", "50-VT", "34-NJ", "36-NY", "42-PA", "17-IL", "18-IN", "26-MI", "39-OH", 
                   "55-WI", "19-IA", "20-KS", "27-MN", "29-MO", "31-NE", "38-ND", "46-SD", "10-DE", "11-DC", "12-FL", "13-GA", "24-MD", 
                   "37-NC", "45-SC", "51-VA", "54-WV", "1-AL", "21-KY", "28-MS", "47-TN", "5-AR", "22-LA", "40-OK", "48-TX", "4-AZ", 
                   "8-CO", "16-ID", "30-MT", "32-NV", "35-NM", "49-UT", "56-WY", "2-AK", "6-CA", "15-HI", "41-OR", "53-WA"] 

####################################################
#Main program

#Create a blank population dataframe holding states and their aggregate populations in 2010, 2050 and 2100 
indexes = [int(state.split("-")[0]) for state in states]
pop_df  = pd.DataFrame(index = indexes,columns = ["state", "Pop_2010", "Pop_2050", "Pop_2100"])
pop_df  = pop_df.fillna(0)

#Read the csv containing upscaled population of states
upscaled_pop = pd.read_csv(upscaled_pop_csv, usecols = ["STATE_NAME", "Population", "Year",
                                                        "Scenario", "Pop_Type"])

#Loop through csv tables and fill the population table with corresponding values
i = 0
for state in states:
    index      = int(state.split("-")[0])
    state_name = state.split("-")[1]
    pop_df.loc[index, "state"]     = state_name
    pop_df.loc[index, "state_abb"] = state_abbs[i][-2:]
    pop_df.loc[index, "Pop_2010"] = upscaled_pop.loc[(upscaled_pop["STATE_NAME"] == state_name) & (upscaled_pop["Year"] == 2010) &
                                                     (upscaled_pop["Scenario"] == scenario.lower()) & (upscaled_pop["Pop_Type"] == "total"),
                                                     "Population"].values
    pop_df.loc[index, "Pop_2050"] = upscaled_pop.loc[(upscaled_pop["STATE_NAME"] == state_name) & (upscaled_pop["Year"] == 2050) &
                                                     (upscaled_pop["Scenario"] == scenario.lower()) & (upscaled_pop["Pop_Type"] == "total"),
                                                     "Population"].values
    pop_df.loc[index, "Pop_2100"] = upscaled_pop.loc[(upscaled_pop["STATE_NAME"] == state_name) & (upscaled_pop["Year"] == 2100) &
                                                     (upscaled_pop["Scenario"] == scenario.lower()) & (upscaled_pop["Pop_Type"] == "total"),
                                                     "Population"].values
    i+=1

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
        locations = pop_df['state_abb'],
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
        title  = dict(text = 'Percentage of Population Change ({0}) after Upscaling National Population Grids <br> According to "{1}" Scenario'.format(duration, scenario_title),
                      x = 0.5, xanchor = "center"),
        font   = dict(size = 23, family = "Arial"),
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
pio.write_image(fig, os.path.join(cur_proj_results, "{0}_{1}_upscaled.jpg".format(scenario, period)))




