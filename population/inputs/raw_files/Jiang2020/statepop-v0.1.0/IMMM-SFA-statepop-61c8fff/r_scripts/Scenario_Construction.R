# Author:     Hamidreza Zoraghein
# Date:       2019-03-06
# Purpose:    Creates different SSP scenarios for tfr, life expectancy at age 0, international migration and domestic migration 
#             by incorporating historical records and IIASA's equivalent projections.


###############
#*** Paths ***#
###############
#* Specify scenario
cur.scenario <- "SSP5"

path <- "C:/Users/Phil/OneDrive/Dissertation/data/Jiang/statepop-v0.1.0/IMMM-SFA-statepop-61c8fff"

# Path to results folder 
results.path <- file.path(path, cur.scenario)

# Path to the package
mspackage    <- file.path(path, "r-scripts", "multistate_0.1.0.tar.gz")

# File that contains fertility and international migration historical series
history.file  <- file.path(path, "inputs", "USA_historical.xlsx")

# File that contains fertility, mortality and international migration scenarios for SSPs from IIASA
iiasa.file  <- file.path(path, "inputs", "USA population projection scenarios_SSPs.xlsx")


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


#############################
#*** Set general options ***#
#############################
options("scipen"=100, "digits"=4) # to force R not to use scientific notations
#options(warn=2)  # treat warnings as errors


###################################
#*** Declare general variables ***#
###################################
#* Define starting year and end year
year.start <- 2010                  # Base year
year.end   <- 2100                  # Last year for which population is projected
steps      <- year.end - year.start # each projection step can be thought of as the resulting year (e.g., projection step 1 projects values for year 1 using input for year 0)


##################################
#*** Main body of the program ***#
##################################

if (cur.scenario != "Constant_rate"){
  
  # Create a scenario dataframe
  scenario.table           <- data.frame(matrix(NA, nrow = steps+1, ncol = 10))
  colnames(scenario.table) <- c("Year", "TFR", "TFR_change", "Int_Mig", "Int_Mig_change", "F_Mor", "M_Mor", "F_Mor_change", "M_Mor_change",
                                "Dom_Mig_Factor")
  scenario.table$Year      <- year.start:year.end
  
  iiasa.table <- as.data.frame(read_excel(iiasa.file, sheet = cur.scenario))
  iiasa.table <- iiasa.table[iiasa.table$year >= year.start,]
  
  
  # ********** Fertility ********** #
  
  his.tfr.table        <- as.data.frame(read_excel(history.file, sheet = "TFR"))
  iiasa.fertility      <- f.linIntE(iiasa.table[,"f_A"], "TFR", si = T)
  iiasa.fertility$year <- year.start:year.end
  
  # TFR of our scenario is the same as historical data for 2010 to 2016 and as IIASA projections for 2045 to 2100
  scenario.table[scenario.table$Year %in% seq(year.start, 2016), "TFR"] <- his.tfr.table[his.tfr.table$Year %in% seq(year.start, 2016), "TFR"]
  scenario.table[scenario.table$Year %in% seq(2045, year.end), "TFR"] <- iiasa.fertility[iiasa.fertility$year %in% seq(2045, year.end), "TFR"]
  
  # For years in between (2017 2044) we linearly remove the difference in 2016 from IIASA projections until we reach 0 in 2045
  # linear equation for the fertility scenario
  y2    <- 0
  y1    <- iiasa.fertility[iiasa.fertility$year == 2016, "TFR"] - his.tfr.table[his.tfr.table$Year == 2016, "TFR"]
  x2    <- 2045
  x1    <- 2016
  slope <- (y2-y1) / (x2-x1)
  
  for (i in seq(x1+1, x2-1)){
    diff <- slope*(i-x1) + y1
    scenario.table[scenario.table$Year == i, "TFR"] <- iiasa.fertility[iiasa.fertility$year == i, "TFR"] - diff
  }
  
  # Calculate the change between each year and the next to later apply these change to state-level TFRs
  scenario.table[1:(nrow(scenario.table) - 1), "TFR_change"] <- ((scenario.table[2:nrow(scenario.table), "TFR"] - scenario.table[1:(nrow(scenario.table) - 1), "TFR"]) /
                                                                   scenario.table[1:(nrow(scenario.table) - 1), "TFR"]) + 1
  
  
  # ********** International Migration ********** #
  
  his.int.mig.table  <- as.data.frame(read_excel(history.file, sheet = "international mig"))
  his.int.mig        <- f.linIntE(his.int.mig.table[his.int.mig.table$Year %in% c(year.start, 2015),"Int_Mig"], "Int_Mig", si = T)[1:6, "Int_Mig"]
  iiasa.f.int.mig    <- f.linIntE(iiasa.table[,"nim_F"]*1000, "Int_Mig", si = T)
  iiasa.m.int.mig    <- f.linIntE(iiasa.table[,"nim_M"]*1000, "Int_Mig", si = T)
  iiasa.int.mig      <- iiasa.f.int.mig + iiasa.m.int.mig
  iiasa.int.mig$year <- year.start:year.end   
  
  # International migration of our scenario is the same as historical data for 2010 to 2015
  scenario.table[scenario.table$Year %in% seq(year.start, 2015), "Int_Mig"] <- his.int.mig
  
  # For 2015 to 2035, we linearly remove the difference in 2015 from IIASA projections until we reach 0 in 2035
  y2    <- 0
  y1    <- iiasa.int.mig[iiasa.int.mig$year == 2015, "Int_Mig"] - his.int.mig[6]
  x2    <- 2035
  x1    <- 2015
  slope <- (y2-y1) / (x2-x1)
  
  for (i in seq(x1+1, x2-1)){
    diff <- slope*(i-x1) + y1
    scenario.table[scenario.table$Year == i, "Int_Mig"] <- iiasa.int.mig[iiasa.int.mig$year == i, "Int_Mig"] - diff
  }
  
  # For 2036 to 2055, the international migration for our scenario is equal to the equivalent IIASA SSP scenario
  scenario.table[scenario.table$Year %in% seq(2035, 2055), "Int_Mig"] <- iiasa.int.mig[iiasa.int.mig$year %in% seq(2035, 2055), "Int_Mig"] 

  # If the scenario is either SSP2 or SSP5, the value for after 2055 remains the same
  if (cur.scenario %in% c("SSP2", "SSP5")){
    scenario.table[scenario.table$Year %in% seq(2056, year.end), "Int_Mig"] <- iiasa.int.mig[iiasa.int.mig$year == 2055, "Int_Mig"] 
    
  } else{
    # For SSP3 the value linearly diminishes to 0 from 2056 to 2100
    
    y2    <- 0
    y1    <- scenario.table[scenario.table$Year == 2055, "Int_Mig"] 
    x2    <- year.end
    x1    <- 2055
    slope <- (y2-y1) / (x2-x1)
    
    for (i in seq(x1+1, x2)){
      int.mig.value <- slope*(i-x1) + y1
      scenario.table[scenario.table$Year == i, "Int_Mig"] <- int.mig.value
    }
  }
  
  # Calculate the change between each year and the next to later apply these change to state-level international migration
  scenario.table[1:(nrow(scenario.table) - 1), "Int_Mig_change"]   <- ((scenario.table[2:nrow(scenario.table), "Int_Mig"] - scenario.table[1:(nrow(scenario.table) - 1), "Int_Mig"]) /
                                                                        scenario.table[1:(nrow(scenario.table) - 1), "Int_Mig"]) + 1

  
  
  # ********** Mortality ********** #
  iiasa.f.mor      <- f.linIntE(iiasa.table[, "m_AF"], "F_Mor", si = T)
  iiasa.m.mor      <- f.linIntE(iiasa.table[, "m_AM"], "M_Mor", si = T)
  iiasa.f.mor$year <- year.start:year.end
  iiasa.m.mor$year <- year.start:year.end
  
  # Our scenario for life expectancy at age 0 uses the same values as in IIASA's SSPs
  scenario.table[, "F_Mor"] <- iiasa.f.mor[, "F_Mor"]
  scenario.table[, "M_Mor"] <- iiasa.m.mor[, "M_Mor"]
  
  # Calculate the change between each year and the next to later apply these change to state-level life expectancies at age 0
  scenario.table[1:(nrow(scenario.table) - 1), "F_Mor_change"]   <- ((scenario.table[2:nrow(scenario.table), "F_Mor"] - scenario.table[1:(nrow(scenario.table) - 1), "F_Mor"]) /
                                                                       scenario.table[1:(nrow(scenario.table) - 1), "F_Mor"]) + 1
  scenario.table[1:(nrow(scenario.table) - 1), "M_Mor_change"]   <- ((scenario.table[2:nrow(scenario.table), "M_Mor"] - scenario.table[1:(nrow(scenario.table) - 1), "M_Mor"]) /
                                                                       scenario.table[1:(nrow(scenario.table) - 1), "M_Mor"]) + 1
  
  
  # Domestic migration
  # First assign 1 as the initial domestic migration factor, this is also the case for SSP2
  scenario.table[, "Dom_Mig_Factor"] <- 1
  
  if (cur.scenario == "SSP5"){
    # In this case, the domestic migration factor increases from 1 (2010) to 2 (2035) gradually
    
    # Retrieve the indices of the initial year and the year with the highest international migration
    index.35  <- which(scenario.table$Year == 2035)
    
    # Increase the factor gradually between the two years
    for (i in seq(1, index.35-1)){
      scenario.table[i+1, "Dom_Mig_Factor"] <- 1 + i / (index.35-1)
    }
    
    # The factor remains constant afterwards
    scenario.table[(index.35+1): nrow(scenario.table), "Dom_Mig_Factor"] <- 2
    
  } else if (cur.scenario == "SSP3") {
    # In this case, the domestic migration factor reduces from 1 (2010) to 0.5 (2100) gradually
    
    # Increase the factor gradually between the two years
    for (i in seq(1, nrow(scenario.table)-1)){
      scenario.table[i+1, "Dom_Mig_Factor"] <- 1 - (i/2) / (nrow(scenario.table)-1)
    }
    
  } 
  
  # Replace NA values with 0
  scenario.table[is.na(scenario.table)] <- 0
}


# Write the scenario table to the relevant folder to use it later in the main program
scenario.csv <- file.path(results.path, paste0(cur.scenario, "_scenario.csv"))
write.csv(scenario.table, scenario.csv, row.names = F)
