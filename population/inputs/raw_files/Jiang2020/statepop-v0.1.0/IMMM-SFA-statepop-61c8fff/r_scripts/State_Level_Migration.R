# Author:     Hamidreza Zoraghein
# Date:       2019-02-11
# Purpose:    Disaggregates total state-level migrations to values per contributing state. This is for the bilateral model.


#############################
#*** Set general options ***#
#############################
options("scipen"=100, "digits"=4) # to force R not to use scientific notations

############################
#*** The required paths ***#
############################
# Workspace
path <- "C:/Users/Hamidreza.Zoraghein/Google Drive/Sensitivity_Analysis/Bilateral"

# Path to results folder 
resultsPath <- file.path(path, "Doub_Scenario")  

# Path to state-level inputs folder
inputsPath  <- file.path(path, "State_Inputs")

# Path to the pachage
mspackage   <- file.path(path, "Scripts", "multistate_0.1.0.tar.gz") 

# csv file containing population projection with no domestic migration applied
pop.no.dom.proj <- file.path(resultsPath, "state_pop_projections_no_dom.csv")


#######################
#*** Load packages ***#
#######################
if ("readxl" %in% installed.packages())
{
  library(readxl)
} else {
  install.packages("readxl")
  library(readxl)
}

if ("multistate" %in% installed.packages())
{
  library(multistate)
} else {
  install.packages("multistate")
  library(multistate)
}


###################################
#*** Declare general variables ***#
###################################
#* Specify regions
regUAll <- c("9-CT", "23-ME", "25-MA", "33-NH", "44-RI", "50-VT", "34-NJ", "36-NY", "42-PA", "17-IL", "18-IN", "26-MI", "39-OH", 
             "55-WI", "19-IA", "20-KS", "27-MN", "29-MO", "31-NE", "38-ND", "46-SD", "10-DE", "11-DC", "12-FL", "13-GA", "24-MD", 
             "37-NC", "45-SC", "51-VA", "54-WV", "1-AL", "21-KY", "28-MS", "47-TN", "5-AR", "22-LA", "40-OK", "48-TX", "4-AZ", 
             "8-CO", "16-ID", "30-MT", "32-NV", "35-NM", "49-UT", "56-WY", "2-AK", "6-CA", "15-HI", "41-OR", "53-WA")

#* Specify scenario
scenUAll     <- c("Constant_rate", "SSP2", "SSP3", "SSP5")
cur.scenario <- "Constant_rate"

# Specify the domestic migration factor
# If scenario is not "Constant_rate" (for fertility, mortality and international migration), this factor will become dynamic later
scen.factor <- 2 # 1 for regular, 0 for no domestic migration, 0.5 for half scenario and 2 for double scenario

# Sepecify if international migration is applied
int.mig <- 1 # 1 applied 0 not applied

# Other parameters
yearStart <- 2010                # Base year
yearEnd   <- 2100                # Last year for which population is projected
steps     <- yearEnd - yearStart # each projection step can be thought of as the resulting year (e.g., projection step 1 projects values for year 1 using input for year 0)
num.ages  <- 101 #From 0 to 100


###################
#*** Main body ***#
###################
# Scenario table generation based on modifications to the "Constant_rate" scenario for SSP2, SSP3 and SSP5
# cur.scenario could be SSP2, SSP3 and SSP5
if (cur.scenario != "Constant_rate"){
  
  scenario.csv   <- file.path(resultsPath, paste0(cur.scenario, "_scenario.csv"))
  scenario.table <- read.csv(scenario.csv, stringsAsFactors = F, check.names = F)
  dom.mig.factor <- scenario.table$Dom_Mig_Factor
  
}


# initialize a dataframe that will hold population values without applying deomestic migration 
pop.upd.df           <- data.frame(matrix(0, nrow = num.ages * 4, ncol = length(regUAll)))
colnames(pop.upd.df) <- regUAll

tot.upd.pop <- read.csv(pop.no.dom.proj, stringsAsFactors = F, check.names = F)


#Save two dataframes per state: one for in-migration from other states and two for out-migration to other states
for (state in regUAll){
  
  cat(paste("The current state is", state, "\n"))
  
  # Initialize two dataframes for holding state-level in and out migration for the current state
  in.mig.path  <- file.path(resultsPath, state, paste0(state, "_total_in_mig.csv"))
  out.mig.path <- file.path(resultsPath, state, paste0(state, "_total_out_mig.csv"))
  
  tot.in.migration  <- NULL
  tot.out.migration <- NULL
  
  # In each year, read the population of all other states (from the previous step) and calulate state-level in and out migration
  for (t in 0:90){
    pop.dataframe     <- tot.upd.pop[seq(num.ages*4*t+1, num.ages*4*(t+1)),]
    
    if (cur.scenario != scenUAll[1]){
      
      # Assign the relevant migration factor
      scen.factor <- dom.mig.factor[t+1]
      
    } 
      
    cur.in.state.mig  <- f.in.state.dom.mig.calc(inputsPath, state, pop.dataframe, scen.factor)
    cur.out.state.mig <- f.out.state.dom.mig.calc(inputsPath, state, pop.dataframe, scen.factor)
    
    # Add the current migration values to the total dataframes
    tot.in.migration  <- rbind(tot.in.migration, cur.in.state.mig)
    tot.out.migration <- rbind(tot.out.migration, cur.out.state.mig)
  }
  
  # Save the total dataframes
  write.csv(tot.in.migration, in.mig.path, row.names = F)
  write.csv(tot.out.migration, out.mig.path, row.names = F)
}



