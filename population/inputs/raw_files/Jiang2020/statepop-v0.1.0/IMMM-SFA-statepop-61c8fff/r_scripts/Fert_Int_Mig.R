# Author:     Hamidreza Zoraghein
# Date:       2019-03-05
# Purpose:    Calculates the national-level international migration and TFR estimates for the U.S. over all projection years 
#             under a specified scenario by utilizing projected state-level TFR and international migration values.

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

#* Specify regions
regUAll <- c("9-CT", "23-ME", "25-MA", "33-NH", "44-RI", "50-VT", "34-NJ", "36-NY", "42-PA", "17-IL", "18-IN", "26-MI", "39-OH", 
             "55-WI", "19-IA", "20-KS", "27-MN", "29-MO", "31-NE", "38-ND", "46-SD", "10-DE", "11-DC", "12-FL", "13-GA", "24-MD", 
             "37-NC", "45-SC", "51-VA", "54-WV", "1-AL", "21-KY", "28-MS", "47-TN", "5-AR", "22-LA", "40-OK", "48-TX", "4-AZ", 
             "8-CO", "16-ID", "30-MT", "32-NV", "35-NM", "49-UT", "56-WY", "2-AK", "6-CA", "15-HI", "41-OR", "53-WA")

num.ages <- 101 #From 0 to 100

#* Define starting year and end year
yearStart <- 2010                # Base year
yearEnd   <- 2100                # Last year for which population is projected
steps     <- yearEnd - yearStart # each projection step can be thought of as the resulting year (e.g., projection step 1 projects values for year 1 using input for year 0)


#############################################
#***              Main Body              ***#
#############################################

#***    TFR of the U.S.    ***#

# Read the csv file containing fertility rates for all states according to the scenario
fert.csv <- file.path(results.path, "tot.fert.csv")
fert.df  <- read.csv(fert.csv, stringsAsFactors = F, check.names = F)

# Keep only required columns
fert.df  <- fert.df[,2:(steps + 2)]

# create a dataframe that will hold TFR for all states across all years
tfr.df <- data.frame(matrix(NA, nrow = steps+1, ncol = length(regUAll)))
colnames(tfr.df) <- regUAll

# Populate the dataframe
for (t in 0:steps){
  for (reg in 1:length(regUAll)){
    tfr.df[t+1, regUAll[reg]] <- sum(fert.df[((reg-1)*(num.ages)+1): ((reg)*(num.ages)), (t+1)])
  }
}


# Read the csv file containing population projections
tot.pop.csv <- file.path(results.path, "state_pop_projections.csv")
tot.pop.df  <- read.csv(tot.pop.csv, stringsAsFactors = F, check.names = F)

# Get the total projected population of each state in each year
pop.df           <- data.frame(matrix(NA, nrow = steps+1, ncol = length(regUAll)))
colnames(pop.df) <- regUAll

# Populate the dataframe
for (t in 0:steps){
  for (reg in 1:length(regUAll)){
    pop.df[t+1, regUAll[reg]] <- sum(tot.pop.df[((t*num.ages*4)+1): ((t+1)*num.ages*4), regUAll[reg]])
  }
}

# Multiply total fertility rate and population of each state for weighted averaging
tfr.pop.df <- tfr.df * pop.df

# Create a dataframe for holding the TFR of the U.S. in each year
tfr.national           <- data.frame(matrix(NA, nrow = steps+1, ncol = 2))
colnames(tfr.national) <- c("Year", "TFR")
tfr.national[,"Year"] <- 2010 + 0:steps

# Calculate the TFR of the U.S. in each year
tfr.national[,"TFR"] <- apply(tfr.pop.df, 1, sum) / apply(pop.df, 1, sum)


#***    International migration of the U.S.    ***#

# Read the csv file that contains international migration per state and for all years
tot.int.mig.csv <- file.path(results.path, "tot.int.mig.csv")
tot.int.mig.df  <- read.csv(tot.int.mig.csv, stringsAsFactors = F, check.names = F)

# Create a dataframe for holding the international migration of the U.S. in each year
int.mig.national           <- data.frame(matrix(NA, nrow = steps+1, ncol = 2))
colnames(int.mig.national) <- c("Year", "Int_Mig")
int.mig.national[,"Year"] <- 2010 + 0:steps

# Populate the dataframe
for (t in 0:steps){
  int.mig.national[t+1, "Int_Mig"] <- sum(tot.int.mig.df[((t*num.ages*4)+1): ((t+1)*num.ages*4),])
}


# Write the national level TFR and international migration to csv files
# TFR
tfr.national.csv <- file.path(results.path, "National_TFR.csv")
write.csv(tfr.national, tfr.national.csv, row.names = F)

# International migration
int.mig.national.csv <- file.path(results.path, "International_Mig.csv")
write.csv(int.mig.national, int.mig.national.csv, row.names = F)
