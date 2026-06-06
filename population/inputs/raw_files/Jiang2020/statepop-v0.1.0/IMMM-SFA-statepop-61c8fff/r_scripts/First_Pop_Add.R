# Author:     Hamidreza Zoraghein
# Date:       2019-02-25
# Purpose:    Adds the base-year population to the projected population table that starts with 2011. After running this script, the resulting
#             table starts with 2010.

#############################
#*** Set general options ***#
#############################
options("scipen"=100, "digits"=4) # to force R not to use scientific notations


###############
#*** Paths ***#
###############
# Workspace
path <- "C:/Users/Hamidreza.Zoraghein/Google Drive/Sensitivity_Analysis/Bilateral"

scenario <- "No_Mig"

# Path to results folder
results.path <- file.path(path, scenario)  


###################################
#*** Declare general variables ***#
###################################
#* Regions
regUAll <- c("9-CT", "23-ME", "25-MA", "33-NH", "44-RI", "50-VT", "34-NJ", "36-NY", "42-PA", "17-IL", "18-IN", "26-MI", "39-OH", 
             "55-WI", "19-IA", "20-KS", "27-MN", "29-MO", "31-NE", "38-ND", "46-SD", "10-DE", "11-DC", "12-FL", "13-GA", "24-MD", 
             "37-NC", "45-SC", "51-VA", "54-WV", "1-AL", "21-KY", "28-MS", "47-TN", "5-AR", "22-LA", "40-OK", "48-TX", "4-AZ", 
             "8-CO", "16-ID", "30-MT", "32-NV", "35-NM", "49-UT", "56-WY", "2-AK", "6-CA", "15-HI", "41-OR", "53-WA")

num.ages <- 101 #From 0 to 100


#############################################
#***              Main Body              ***#
#############################################
pop.csv <- file.path(results.path, "state_pop_projections.csv")
pop.df  <- read.csv(pop.csv, stringsAsFactors = F, check.names = F)

states.fst.pop.df <- pop.df[1:(num.ages*4),]

for (region in regUAll){
  
  cur.state.csv  <- file.path(path, scenario, region, paste0(region, "_proj_pop.csv"))
  cur.fst.pop.df <- read.csv(cur.state.csv, stringsAsFactors = F, check.names = F)["2010"]
  
  states.fst.pop.df[,region] <- cur.fst.pop.df
}


pop.df <- rbind(states.fst.pop.df, pop.df)

write.csv(pop.df, pop.csv, row.names = F)













