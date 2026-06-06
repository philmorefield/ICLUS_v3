# Author:     Hamidreza Zoraghein
# Date:       2019-01-03
# Purpose:    Project populations by states for the U.S with sensitivity analysis added. Sensitivity analysis
#             currently takes into account national-level SSP projectsions of fertility, mortality and international
#             migration. The U.S. Domestic migration is bilateral. By sensitivty analysis, we mean projection based on
#             different scenarios.


###############
#*** Paths ***#
###############
path <- "C:/Users/Phil/OneDrive/Dissertation/data/Jiang/statepop-v0.1.0/IMMM-SFA-statepop-61c8fff"

# Path to results folder 
resultsPath <- file.path(path, "SSP3")

# Path to state-level inputs folder
inputsPath  <- file.path(path, 'inputs', "State_Inputs") 

# Path to the package
mspackage   <- file.path(path, "r_scripts", "multistate_0.1.0.tar.gz") 

# UN standard life table e0=30; used for linear interpolation of lx values
datMortS30S  <- file.path(path, 'inputs', "AllRegions_mortality_UNe030.csv")  

# UN standard life table e0=100; used for linear interpolation of lx values
datMortS100S <- file.path(path, 'inputs', "AllRegions_mortality_UNe0100.csv") 


#######################
#*** Load packages ***#
#######################
# Various packages
if ("foreign" %in% installed.packages())
{
  library(foreign)
} else {
  install.packages("foreign")
  library(foreign)
}

if ("Matrix" %in% installed.packages())
{
  library(Matrix)
} else {
  install.packages("Matrix")
  library(Matrix)
}

if ("ggplot2" %in% installed.packages())
{
  library(ggplot2)
} else {
  install.packages("ggplot2")
  library(ggplot2)
}

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

#* Specify regions
regUAll <- c("9-CT", "23-ME", "25-MA", "33-NH", "44-RI", "50-VT", "34-NJ", "36-NY", "42-PA", "17-IL", "18-IN", "26-MI", "39-OH", 
             "55-WI", "19-IA", "20-KS", "27-MN", "29-MO", "31-NE", "38-ND", "46-SD", "10-DE", "11-DC", "12-FL", "13-GA", "24-MD", 
             "37-NC", "45-SC", "51-VA", "54-WV", "1-AL", "21-KY", "28-MS", "47-TN", "5-AR", "22-LA", "40-OK", "48-TX", "4-AZ", 
             "8-CO", "16-ID", "30-MT", "32-NV", "35-NM", "49-UT", "56-WY", "2-AK", "6-CA", "15-HI", "41-OR", "53-WA")

#* Specify scenario
scenUAll     <- c("Constant_rate", "SSP2", "SSP3", "SSP5")
cur.scenario <- "SSP3"

# Specify the domestic migration factor
# If scenario is not "Constant_rate" (for fertility, mortality and international migration), this factor will become dynamic later
scen.factor <- 1 # 1 for regular, 0 for no domestic migration, 0.5 for half scenario and 2 for double scenario

# Sepecify if international migration is applied
int.mig <- 1 # 1 applied 0 not applied

#* Should details for projection model adjustment be printed?
vis <- T # TRUE (print details); FALSE (don't print details)

#* Should the Brass Relational Model be used or a simple scaling approach to compute future fertility schedules
useBrassf <- T # TRUE (use Brass); FALSE (use scaling)

#* Generate Directories
if(!file.exists(resultsPath)) {dir.create(resultsPath)} # output directory

datMortS100  <- read.csv(datMortS100S, check.names = F, stringsAsFactors = F) 
datMortS30   <- read.csv(datMortS30S, check.names = F, stringsAsFactors = F) 


num.ages         <- 101 #From 0 to 100
ini.all.base.pop <- as.data.frame(matrix(0, nrow = 4 * num.ages, ncol = length(regUAll)))
upd.all.base.pop <- as.data.frame(matrix(0, nrow = 4 * num.ages, ncol = length(regUAll)))
colnames(ini.all.base.pop) <- regUAll
colnames(upd.all.base.pop) <- regUAll


#* Define starting year and end year
yearStart <- 2010                # Base year
yearEnd   <- 2100                # Last year for which population is projected
steps     <- yearEnd - yearStart # each projection step can be thought of as the resulting year (e.g., projection step 1 projects values for year 1 using input for year 0)


# These dataframes will hold population projections and net/in/out state-level migrations for all states and years
tot.projection    <- NULL
tot.state.net.mig <- NULL
tot.state.in.mig  <- NULL
tot.state.out.mig <- NULL

# This dataframe holds population values before applying domestic migration. It is necessary to disaggregate
# domestic migration to/from each state across all other states 
tot.pop.no.dom    <- NULL 

# Initialize a dataframe for holding international migration for all years and states
tot.int.mig           <- data.frame(matrix(NA, nrow = 4*num.ages*(steps+1), ncol = length(regUAll)))
colnames(tot.int.mig) <- regUAll

#####################################################
#*** Derive national-level changes based on SSPs ***#
#####################################################

# Read the scenario table if it is not constant_rate
# cur.scenario could be SSP2, SSP3 and SSP5
if (cur.scenario != "Constant_rate"){
  
  scenario.csv   <- file.path(resultsPath, paste0(cur.scenario, "_scenario.csv"))
  scenario.table <- read.csv(scenario.csv, stringsAsFactors = F, check.names = F)
}

#############################################
#*** Generate baseline population matrix ***#
#############################################

# Loop over regions to update their base year population with international and state-level migrations
for (regU in 1:length(regUAll)){
  
  #* Generate paths
  pathIn    <- file.path(inputsPath, regUAll[regU])  # Input data directory
  
  #* Scenario data (The Constant_rate scenario. )
  scenarioS <- paste0(scenUAll[1], ".csv")
  scenario  <- read.csv(file.path(pathIn, scenarioS), check.names = F, stringsAsFactors = F)  # Input data directory
  
  #* Base Population data
  datBPS <- "basePop.csv"          # file containing baseline population
  datBP  <- read.csv(file.path(pathIn, datBPS), check.names = F, stringsAsFactors = F)
  
  # Variables
  bpAge <- "age"                      # Age categories (numeric, starting with 0)
  bpFR  <- "rural_female"             # Rural female population
  bpFU  <- "urban_female"             # Urban female population
  bpMR  <- "rural_male"               # Rural male population
  bpMU  <- "urban_male"               # Urban male population
  
  #* International migration data
  # Note: The international migration rates input data needs to be in relative rates (summing up to 1)
  datNetMigS  <- "intMig.csv"           # file containing international migration rates
  datNetMig   <- read.csv(file.path(pathIn, datNetMigS), check.names = F, stringsAsFactors = F)
  
  # Variables
  netMigAge  <- "age"                   # Age variable
  netMigF    <- "net_female"            # Proportion of net migration for each age group for females 
  netMigM    <- "net_male"              # Proportion of net migration for each age group for males
  
  # Annual total net international migration counts for 5-year periods
  nMigFt     <- as.numeric(scenario[scenario$year%in%c(yearStart:yearEnd), "nim_F"]) # female net international migrants
  nMigMt     <- as.numeric(scenario[scenario$year%in%c(yearStart:yearEnd), "nim_M"]) # male net international migrants

  # Update the base year population with the international migration
  fBP   <- c(rbind(datBP[1:num.ages, bpFU], datBP[1:num.ages, bpFR])) # females -> urban0, rural0, urban1, rural1,... 
  mBP   <- c(rbind(datBP[1:num.ages, bpMU], datBP[1:num.ages, bpMR])) # males
  matBP <- as.matrix(c(fBP,mBP))                                      # combines female and male pieces
  
  
  # Spread migrant numbers according to profile
  if (int.mig == 1){
    datNetMig[, "nmUF"] <- datNetMig[, netMigF] * nMigFt[1]   # urban females
    datNetMig[, "nmRF"] <- datNetMig[, netMigF] * nMigFt[1]   # rural females
    datNetMig[, "nmUM"] <- datNetMig[, netMigM] * nMigMt[1]   # urban males
    datNetMig[, "nmRM"] <- datNetMig[, netMigM] * nMigMt[1]   # rural males
  } else {
    datNetMig[, "nmUF"] <- datNetMig[, netMigF] * 0   # urban females
    datNetMig[, "nmRF"] <- datNetMig[, netMigF] * 0   # rural females
    datNetMig[, "nmUM"] <- datNetMig[, netMigM] * 0   # urban males
    datNetMig[, "nmRM"] <- datNetMig[, netMigM] * 0   # rural males
  }
  
  
  # Update the base year population with the international migration rates
  matBP_upd <- matBP + c(rbind(datNetMig[1:num.ages, "nmUF"], datNetMig[1:num.ages, "nmRF"]),
                         rbind(datNetMig[1:num.ages, "nmUM"], datNetMig[1:num.ages, "nmRM"]))
  
  
  # Populate the data frames that store base-year population and its updated population by international migration
  ini.all.base.pop[, regUAll[regU]]         <- matBP
  upd.all.base.pop[, regUAll[regU]]         <- matBP_upd
  ini.all.base.pop[is.na(ini.all.base.pop)] <- 0
  upd.all.base.pop[is.na(upd.all.base.pop)] <- 0
  
  # Keep the international migration for the current state
  cur.int.mig                     <- matBP_upd - matBP
  cur.int.mig[is.na(cur.int.mig)] <- 0
  tot.int.mig[0:(4*num.ages), regUAll[regU]] <- cur.int.mig
}

# Calculate the total in out and net migration numbers for all states
in.migration  <- f.in.dom.mig.calc(inputsPath, upd.all.base.pop, scen.factor)
out.migration <- f.out.dom.mig.calc(inputsPath, upd.all.base.pop, scen.factor)
net.migration <- in.migration - out.migration # People entered a state minus those who left

# Store the population before applying the domestic migration
tot.pop.no.dom <- rbind(tot.pop.no.dom, upd.all.base.pop)

#Update the base year population with the state-level migration
upd.all.base.pop   <- upd.all.base.pop + net.migration

# Store net/in/out migration values in the base year
tot.state.in.mig  <- rbind(tot.state.in.mig, in.migration)
tot.state.out.mig <- rbind(tot.state.out.mig, out.migration)
tot.state.net.mig <- rbind(tot.state.net.mig, net.migration)


###################
#*** Mortality ***#
###################

# Initialize total matrices
tot.dfdx <- NULL
tot.dfLx <- NULL
tot.dfmx <- NULL
tot.dfTx <- NULL
tot.dfSx <- NULL
tot.dfex <- NULL

for (regU in 1:length(regUAll)){
  
  cat(paste("\nMortality for", regUAll[regU]))
  
  #* Generate paths
  pathIn      <- file.path(inputsPath, regUAll[regU])  # Input data directory
  
  #* Scenario data
  scenarioS   <- paste0(scenUAll[1], ".csv")
  scenario    <- read.csv(file.path(pathIn, scenarioS), check.names = F, stringsAsFactors = F)  # Input data directory
  
  #* Mortality data
  # Note: The variable names need to be the same in all three mortality data files
  datMortS    <- "mortality.csv"  # file containing mortality data for particular region for standard (preferable close to the base year)
  datMort     <- read.csv(file.path(pathIn, datMortS), check.names = F, stringsAsFactors = F)
  
  # Variables
  lxAge <- "age"                          # Age categories (numeric, starting with 0)
  lxFR  <- "lx_rural_female"              # Life expectancy rural females in 100000 people
  lxFU  <- "lx_urban_female"              # Life expectancy urban females in 100000 people
  lxMR  <- "lx_rural_male"                # Life expectancy rural male in 100000 people
  lxMU  <- "lx_urban_male"                # Life expectancy urban male in 100000 people
  
  #* Rename variables
  # Standard
  names(datMort)[names(datMort) == lxFR] <- "lxFRs" # rural females
  names(datMort)[names(datMort) == lxFU] <- "lxFUs" # urban females
  names(datMort)[names(datMort) == lxMR] <- "lxMRs" # rural males
  names(datMort)[names(datMort) == lxMU] <- "lxMUs" # urban males
  
  # UN e0=100 (life expectancy at age 0 is 100 years)
  names(datMortS100)[names(datMortS100) == lxFR] <- "lxFRs100" # rural females for e0=100
  names(datMortS100)[names(datMortS100) == lxFU] <- "lxFUs100" # urban females for e0=100
  names(datMortS100)[names(datMortS100) == lxMR] <- "lxMRs100" # rural males for e0=100
  names(datMortS100)[names(datMortS100) == lxMU] <- "lxMUs100" # urban males for e0=100
  
  # UN e0=30 (life expectancy at age 0 is 30 years)
  names(datMortS30)[names(datMortS30) == lxFR] <- "lxFRs30"    # rural females for e0=30
  names(datMortS30)[names(datMortS30) == lxFU] <- "lxFUs30"    # urban females for e0=30
  names(datMortS30)[names(datMortS30) == lxMR] <- "lxMRs30"    # rural males for e0=30
  names(datMortS30)[names(datMortS30) == lxMU] <- "lxMUs30"    # urban males for e0=30
  
  #* Prepare df
  datMort <- datMort[, c(lxAge, "lxFRs", "lxFUs", "lxMRs", "lxMUs")]            # Reduce df to relevant variables
  datMort <- merge(datMort, merge(datMortS30, datMortS100, by=lxAge), by=lxAge) # Merge UN e0=100 and UN e0=30 values into datMort
  
  
  #*** Compute lx values using linear interpolation
  #* Initialize data frames and variables
  dflx <- datMort                  # data frame to hold interpolated lx values
  e0n  <- "e0df1"                  # beginning of name (e.g., "e0df1") of tables of expected life expectancy values (e0) interpolated for each projection year
  gl   <- c("FU","FR","MU","MR")   # gender groups for which interpolation should be performed
  
  
  # Life expectancies at age zero (e0) for projection period according to the scenario
  # Note: Start with e0 of baseline year and provide expected e0 values for 5-year intervals; single year e0 values will be linearly interpolated 
  e0EFU <- as.numeric(scenario[scenario$year%in%c(yearStart:yearEnd),"m_UF"])      # urban females
  e0EFR <- as.numeric(scenario[scenario$year%in%c(yearStart:yearEnd),"m_RF"])      # rural females
  e0EMU <- as.numeric(scenario[scenario$year%in%c(yearStart:yearEnd),"m_UM"])      # urban males
  e0EMR <- as.numeric(scenario[scenario$year%in%c(yearStart:yearEnd),"m_RM"])      # rural males
  
  
  # Modify the constant rate scenario mortality assumptions if current scenario is something else
  if (cur.scenario != scenUAll[1]){
    
    # Retrive the national-level changes of female life expectancy according to the current scenario
    # Currently urban and rural are assumed equal
    urb.mor.f.rate <- cumprod(scenario.table[, "F_Mor_change"])
    rur.mor.f.rate <- cumprod(scenario.table[, "F_Mor_change"])
    
    # Retrive the national-level changes of male life expectancy according to the current scenario
    # Currently urban and rural are assumed equal
    urb.mor.m.rate <- cumprod(scenario.table[, "M_Mor_change"])
    rur.mor.m.rate <- cumprod(scenario.table[, "M_Mor_change"])
    
    # Apply the national-level rates to the state-level life expectancy assumptions
    # Female
    e0EFU   <- urb.mor.f.rate[1:(length(urb.mor.f.rate) - 1)] * e0EFU[1]
    e0df1FU <- data.frame(year = 0:90, e0 = c(e0EFU[1], e0EFU))
    e0EFR   <- rur.mor.f.rate[1:(length(rur.mor.f.rate) - 1)] * e0EFR[1]
    e0df1FR <- data.frame(year = 0:90, e0 = c(e0EFR[1], e0EFR))
    
    # Male
    e0EMU   <- urb.mor.m.rate[1:(length(urb.mor.m.rate) - 1)] * e0EMU[1]
    e0df1MU <- data.frame(year = 0:90, e0 = c(e0EMU[1], e0EMU))
    e0EMR   <- rur.mor.m.rate[1:(length(rur.mor.m.rate) - 1)] * e0EMR[1]
    e0df1MR <- data.frame(year = 0:90, e0 = c(e0EMR[1], e0EMR))
    
  } else {
    
    # Linearly interpolate 1-year life expectancies between 5-year intervals 
    e0df1FU <- f.linIntE(e0EFU, "e0", si = T) # Urban females
    e0df1FR <- f.linIntE(e0EFR, "e0", si = T) # Rural females 
    e0df1MU <- f.linIntE(e0EMU, "e0", si = T) # Urban males
    e0df1MR <- f.linIntE(e0EMR, "e0", si = T) # Rural males
  }
  
  
  #* Loop over time steps
  for(t in 0:steps){
    
    if(vis){cat("\n\nGenerating lx values for year:", t+yearStart)}
    
    #* Loop over gender groups
    for(g in 1:length(gl)){
      
      if(vis){cat("\nProcessing gender group:",gl[g],"; Iterations...")}
      
      #* Obtain e0 values
      e0df1 <- get(paste0(e0n, gl[g]))                                   # Obtain the table containing the e0 values at 1-year interval for the particular gender group according to the scenario
      e0t   <- e0df1[e0df1$year == t, "e0"]                              # e0 value for year t from scenario
      lxs   <- paste0("lx",gl[g], "s")                                   # gender and region specific lx value used as sandard
      e0s   <- f.e0(dflx, lxs, lxAge, gl[g], e0df1[e0df1$year==0, "e0"]) # e0 value from the mortality dataframe of the region
      
      #* Choose correct UN lx variable for interpolation
      if(e0t >= e0s)
      {                                             # when e0 for time t in scenario is larger than e0 from data for standard
        lxUN <- paste0("lx", gl[g], "s100")         # variable name of lx values to be used for interpolation
        e0UN <- f.e0(dflx, lxUN, lxAge, gl[g], 100) # e0=100 from UN data
        
      }else if(e0t < e0s){                          # when e0 for time t in scenario is smaller than e0 from data for standard; if a decline in e0 is assumed over time
        lxUN <- paste0("lx", gl[g], "s30")          # variable name of lx values to be used for interpolation
        e0UN <- f.e0(dflx, lxUN, lxAge, gl[g], 30)  # e0=30 from UN data
      }
      
      #* Linearily interpolate lx values based on e0 values
      dflx[, paste0("lx", gl[g], t)] <- f.lIntC(df = dflx, x1 = lxs, x2 = lxUN, y1 = e0s, y2 = e0UN, yi = e0t)
      
      # Compute e0tp from the newly modified observed lx values after standardizing based on the scenario
      e0tp  <- f.e0(dflx, paste0("lx", gl[g], t), lxAge, gl[g], e0t)
      ediff <- abs(e0tp - e0t)  # Compute difference between e0 (user specified) and e0 prime (from data)
      te    <- 0.01             # theshold of e0 (numeric) above which an iterative adjustment is performed
      
      #*** Perform iterative adjustment
      # Note: the scaler is adjusted incrementally until the e0 difference between the scenario e0 value and the e0 value from within the data is less than te
      if(ediff > te){
        count  <- 1       # Set counter
        e0tAdj <- e0t     # generate a copy of the e0t value that will be used/modified in the adjustment process
        
        # Perform while loop
        while(ediff > te){
          
          #* Specify the size of the increment by which e0tAdj is changed
          if(ediff >= 0.7){
            si <- 0.5
          }else if(ediff >= 0.2 & ediff < 0.7){
            si <- 0.1
          }else if(ediff >= 0.05 & ediff < 0.2){
            si <- 0.05
          }else if(ediff < 0.05){
            si <- 0.01
          }
          
          #* Adjust scaler value
          if(e0tp > e0t){         # When e0tp (generated data) is larger than e0t (scenario)
            e0tAdj <- e0tAdj - si
          }else if(e0tp < e0t){   # When e0tp (generated data) is smaller than e0t (scenario)
            e0tAdj <- e0tAdj + si
          }
          
          #* Linearily interpolate lx values based on updated e0 values
          dflx[, paste0("lx", gl[g], t)] <- f.lIntC(df = dflx, x1 = lxs, x2 = lxUN, y1 = e0s, y2 = e0UN, yi = e0tAdj)
          
          # Recompute e0tp from the newly generated lx values
          e0tp  <- f.e0(dflx, paste0("lx", gl[g], t), lxAge, gl[g], e0t)
          ediff <- abs(e0tp - e0t)  # Compute difference between e0 (user specified) and e0 prime (from data)
          
          #* Update counter
          if(vis){cat(count, " ")} # print counter
          count <- count+1
        }
      }
    }
  }
  
  
  # Returns a warning if negative values are produced in lx table
  if(any(dflx<0)) {warning("lx data frame contains negative values ", regUAll[regU]," ",scenUAll[scenU])} 
  
  
  #*** Compute life table variables
  # Note: input lx values need to be provided in 100000 of people; variable lx in form "lxFU0"
  
  #* Initialize variables
  e0n <- "e0df1"                # beginning of name (e.g., "e0df1") of tables of expected life expectancy values (e0)
  gl  <- c("FU","FR","MU","MR") # list of gender groups for which computation should be performed (e.g., "FU","FR","MU","MR")
  
  #* Initialize data frames to hold results
  dfdx <- data.frame(age = dflx[,lxAge], stringsAsFactors = F)
  dfLx <- data.frame(age = dflx[,lxAge], ax = c(0.33, rep(0.5, nrow(dflx)-1)), stringsAsFactors = F) # ax= Percentage of survivals
  dfmx <- data.frame(age = dflx[,lxAge], stringsAsFactors = F)
  dfTx <- data.frame(age = dflx[,lxAge], stringsAsFactors = F)
  dfSx <- data.frame(age = dflx[,lxAge], stringsAsFactors = F)
  dfex <- data.frame(age = dflx[,lxAge], stringsAsFactors = F)
  
  #* Loop over time steps
  for(t in 0:steps){
    
    for(g in 1:length(gl)){
      
      #* Deaths per age group (dx)
      # Note: dx=lx0-lx1;
      dfdx[, paste0("dx", gl[g], t)]                 <- NA
      dfdx[1:(nrow(dfdx)-1), paste0("dx", gl[g], t)] <- dflx[1:(nrow(dflx)-1), paste0("lx", gl[g], t)] - dflx[2:nrow(dflx), paste0("lx", gl[g], t)] 
      
      #* Exposure to death per person year (Lx)
      # Note: Lx=(lx1*1)+(dx0*0.5); similar to Lx=0.33*(lx0+lx1)
      dfLx[, paste0("Lx", gl[g], t)]                 <- NA
      dfLx[1:(nrow(dfLx)-1), paste0("Lx", gl[g], t)] <- (dflx[2:nrow(dflx), paste0("lx", gl[g], t)]*1) + (dfdx[1:(nrow(dfdx)-1), paste0("dx", gl[g], t)]*dfLx[1:(nrow(dfLx)-1), "ax"])
      
      #* Mortality rate (mx); the percentage of individuals in a certain age group that died in the last year
      # Note: mx=dx/Lx
      dfmx[, paste0("mx", gl[g], t)] <- dfdx[, paste0("dx", gl[g], t)] / dfLx[, paste0("Lx", gl[g], t)]
      
      #* Compute mx for last age category (e.g., 100+)
      # Note: based on user specified life expectancy at age 0 (for the base year??)
      e0df1                        <- get(paste0(e0n, gl[g]))                      # obtain the table containing the e0 values for the particular gender group
      mm                           <- f.mxMax(gl[g],e0df1[e0df1[,"year"]==t,"e0"]) # obtain mx max value
      maxy                         <- max(dfmx[,"age"])                            # obtain the max year
      dfmx[dfmx[, "age"] == maxy, 
           paste0("mx", gl[g], t)] <- mm                                           # assign the mx value
      
      #* Compute Lx value for oldest age category (Lxmax) 
      # Note: Lxmax=lxmax/mxmax
      dfLx[dfLx[, "age"] == maxy, paste0("Lx", gl[g], t)] <- dflx[dflx[, "age"]==maxy, paste0("lx", gl[g], t)]/mm
      
      #* Cumulative person years of survival
      # Note: Cumulative Lx from oldest to youngest age group
      dfTx[, paste0("Tx", gl[g], t)] <- rev(cumsum(rev(dfLx[, paste0("Lx", gl[g], t)])))
      
      #* Generate proportion of individuals that survived to the next age group (Sx)
      # Note: Sx=Lx1/Lx0; Smax-1=S99=T100+/T99; Smax=0
      dfSx[, paste0("Sx", gl[g], t)] <- NA
      dfSx[1:(nrow(dfSx)-1), 
           paste0("Sx", gl[g], t)]   <- dfLx[2:(nrow(dfLx)), paste0("Lx", gl[g], t)] / dfLx[1:(nrow(dfLx)-1), paste0("Lx", gl[g], t)]
      maxy1                          <- max(dfSx[, "age"])-1 # prior to last year
      dfSx[dfSx[, "age"]==maxy1,
           paste0("Sx", gl[g], t)]   <- dfTx[dfTx[, "age"] == maxy, paste0("Tx", gl[g], t)] / dfTx[dfTx[, "age"] == maxy1, paste0("Tx", gl[g], t)]
      dfSx[dfSx[, "age"]==maxy, 
           paste0("Sx", gl[g], t)]   <- 0  # assign 0 value for Sx value of last age group because no one survived
      
      #* Compute life expectancy by age (ex)
      # Note: ex=Tx/lx
      dfex[, paste0("ex", gl[g], t)] <- dfTx[, paste0("Tx", gl[g], t)] / dflx[, paste0("lx", gl[g], t)]
    }
  }
  
  # Populate the total matrix with the current region
  tot.dfdx <- rbind(tot.dfdx, dfdx)
  tot.dfLx <- rbind(tot.dfLx, dfLx)
  tot.dfmx <- rbind(tot.dfmx, dfmx)
  tot.dfTx <- rbind(tot.dfTx, dfTx)
  tot.dfSx <- rbind(tot.dfSx, dfSx)
  tot.dfex <- rbind(tot.dfex, dfex)
}



###################
#*** Fertility ***#
###################

#Initialize the overal fertility table
tot.fert <- NULL

for (regU in 1:length(regUAll)){

  cat(paste("\nFertility for", regUAll[regU]))
  
  #* Generate paths
  pathIn      <- file.path(inputsPath, regUAll[regU])  # Input data directory
  
  #* Scenario data
  scenarioS   <- paste0(scenUAll[1], ".csv")
  scenario    <- read.csv(file.path(pathIn, scenarioS), check.names = F, stringsAsFactors = F)  # Input data directory
  
  #* Fertility data in the base year
  datFertS    <- "fertility.csv"         # file containing fertility data
  datFert     <- read.csv(file.path(pathIn, datFertS), check.names = F, stringsAsFactors = F)
  
  # Variables
  fAge  <- "age"                      # Variable for ages (numeric, starting with 0)
  asfrR <- "rural"                    # Rural population ASFR (age specific fertility rate)
  asfrU <- "urban"                    # Urban population ASFR (age specific fertility rate)
  
  #* Rename age specific fertility rate variables to tpFx
  # Note: tpFx = proportional TFR; sums up to TFR; this measure feeds into Leslie matrix
  names(datFert)[names(datFert) == asfrR] <- "tpFxRs"   # rural
  names(datFert)[names(datFert) == asfrU] <- "tpFxUs"   # urban
  
  #Replicate urban fertility rates with rural
  datFert["tpFxRs"] <- datFert["tpFxUs"]
  
  # Expected total fertility rate according to the scenario
  # Note: TFR in 5-year intervals
  tfrER <- as.numeric(scenario[scenario$year%in%c(yearStart:yearEnd),"f_R"]) # rural total fertility rates
  tfrEU <- as.numeric(scenario[scenario$year%in%c(yearStart:yearEnd),"f_U"]) # urban total fertility rates
  
  # Modify the constant rate scenario fertility assumptions if current scenario is something else
  if (cur.scenario != scenUAll[1]){
    
    # Retrive the national-level changes of tfr according to the current scenario
    # Currently urban and rural are assumed equal
    net.fer.r.rate <- cumprod(scenario.table[, "TFR_change"])
    net.fer.u.rate <- cumprod(scenario.table[, "TFR_change"])
    
    # Apply the national-level rates to the state-level tfr assumptions
    # Currently urban and rural are assumed equal
    tfrDf1R <- net.fer.r.rate[1:(length(net.fer.r.rate) - 1)] * tfrER[1]
    tfrDf1R <- data.frame(year = 0:90, tfr = c(tfrER[1], tfrDf1R))
    tfrDf1U <- net.fer.u.rate[1:(length(net.fer.u.rate) - 1)] * tfrEU[1]
    tfrDf1U <- data.frame(year = 0:90, tfr = c(tfrEU[1], tfrDf1U))
    
    
  } else {
    
    #* Linearily interpolate expected TFR values between 5-year time steps for 1-year intervals and according to the scenario
    # Note: This generates a data frame with a "year" and "tfr" column
    tfrDf1R <- f.linIntE(tfrER, "tfr", si=T) # rural; si=T first value is starting value; si=F last value is starting value
    tfrDf1U <- f.linIntE(tfrEU, "tfr", si=T) # urban
  }
  
  
  #* Compute fertility variables for standard
  rt <- c("R","U") # "R" = rural; "U" = urban
  for(r in 1:length(rt)){
    
    #* Compute tfr for standard
    tfrS <- sum(datFert[, paste0("tpFx", rt[r], "s")], na.rm=T)
    
    #* Compute pFx from age specific fertility rate
    # Note: pFx = Proportion fertility for each age group relative to all age groups; summing up to 1
    datFert[, paste0("pFx", rt[r], "s")] <- datFert[, paste0("tpFx", rt[r], "s")] / tfrS
    
    #* Compute cpFx
    # Note: cpFx = cumulated proportion fertility up to exact age x -> sum of age-specific fertility rates up to age x
    datFert[, paste0("cpFx", rt[r], "s")] <- cumsum(datFert[, paste0("pFx", rt[r], "s")])
    
    #* Compute tcpFx
    # Note: tcpFx = cumulative proportion of TFR for Standard up to exact age x -> sum of tfr up to age x
    datFert[, paste0("tcpFx", rt[r], "s")] <- datFert[, paste0("cpFx", rt[r], "s")]*tfrS
  }
  
  #* Compute IQR (Inter Quartile Range) for standard
  # Note: Exact ages for the 75%ile and 25%ile are derived using linear interpolation
  iqrAgeRS <- f.lInt(datFert, "cpFxRs", fAge, 0.75) - f.lInt(datFert, "cpFxRs", fAge, 0.25) # rural
  iqrAgeUS <- f.lInt(datFert, "cpFxUs", fAge, 0.75) - f.lInt(datFert, "cpFxUs", fAge, 0.25) # urban
  
  #* Compute Median Age of standard
  medAgeRS <- f.lInt(datFert, "cpFxRs", fAge, 0.50) # rural
  medAgeUS <- f.lInt(datFert, "cpFxUs", fAge, 0.50) # urban
  
  #*** Compute future fertility schedules by standardizing them according to the scenario
  # Note: User defines whether to use the Brass Relational model or a simple scaling approach
  rt <- c("R","U")     # "R" = rural; "U" = urban
  
  if(useBrassf){ # uses the Brass Relational Gompertz model to compute future fertility schedules
    
    #* Loop over years for which fertility data is needed
    for(t in 0:steps){
      # t=0
      if(vis){cat(t + yearStart," ")}
      
      #* Loop over region types (rural/urban)
      for(r in 1:(length(rt))){
        # r=1
        
        #* Obtain df of 1-year TFR values from scenario
        tfrDf1 <- get(paste0("tfrDf1", rt[r]))
        
        #* Declare TFR values used in fertility schedule computation
        tfrS <- sum(datFert[, paste0("tpFx", rt[r], "s")], na.rm = T)      # standard
        tfrF <- tfrDf1[tfrDf1[, "year"]==t, "tfr"]                     # future year
        
        #* Compute IQR of Future year (t)
        # Note: Formula is based on empirically derived relationship between TFR and IQR 
        iqrAgeF <- 6.1 + 0.466*tfrF + 0.136*tfrF^2
        
        #* Compute beta value
        # Note: When beta=1 then there is no change; the smaller the beta the more dispersed the curve of the schedule; beta ranges from 0.65 to 1.5 (Zeng et al., 2000)
        iqrAgeS <- get(paste0("iqrAge", rt[r], "S")) # obtain prior computed IQR of standard for either rural or urban
        beta    <- iqrAgeS / iqrAgeF
        
        #* Compute Median Age of Future year (t)
        # Note: Formula is based on empirically derived relationship between TFR and Median Age
        if(tfrF <= 4.5){
          medAgeF <- 30.8-4.64*tfrF + 1.05*tfrF^2
        }else{
          medAgeF <- 31.18 # when tfr of future year is larger than 4.5 we fix the median age at 31.18 years 
        }
        
        #* Linearly interpolate tcpFx value of Standard for Median Age of Future year
        tcpFxMedAge <- f.lInt(datFert, fAge, paste0("tcpFx", rt[r], "s"), medAgeF)
        
        #* Compute alpha value
        # Note: the smaller the alpha the later the fertility process; normal alpha rage -0.5 to 0.5 (Zeng et al., 2000)
        alpha <- -log(-log(0.5)) - beta*(-log(-log(tcpFxMedAge / tfrS)))
        
        #* Compute YHxT of Future 
        # Note: YHxT is computed by using alpha and beta values together with the Standard (see Equation 4 in Zeng et al. (2000))
        datFert[, paste0("yHxT", rt[r], t)] <- alpha + beta*(-log(-log(round(datFert[, paste0("tcpFx", rt[r], "s")]/tfrS, 7)))) # Note: rounding to the 7th decimal point is necessary to avoid introduction of NA values because R introduces random numbers after the 15 decimal point which cause problems with the -log-log transformation
        
        #* Compute cpFx for Future
        # Note: cpFx = cumulative proportional fertility by age group
        datFert[, paste0("cpFx", rt[r], t)] <- exp(-exp(-datFert[, paste0("yHxT", rt[r], t)]))
        
        #* Compute tcpFx for Future
        # Note: tcpFx = cumulative proportion of TFR by age group
        datFert[, paste0("tcpFx", rt[r], t)] <- tfrF*datFert[, paste0("cpFx", rt[r], t)]
        
        #* Compute tpFx for Future (fx=H(x)-H(x-1))
        # Note: tpFx = proportion of TFR by age group; the tpFx values are used in the Leslie matrix
        datFert[, paste0("tpFx", rt[r], t)] <- c(0, datFert[2:nrow(datFert), paste0("tcpFx", rt[r], t)] - datFert[1:(nrow(datFert)-1), paste0("tcpFx", rt[r], t)])
        
        #*** Apply middle year value
        # Note: This step is necessary only if the underlying fertility data comes from 5-year age category data
        df5y <- datFert[datFert[, fAge] == 21, paste0("pFx", rt[r], "s")] == datFert[datFert[, fAge] == 22, paste0("pFx", rt[r], "s")] # tests similarity between age 21 and 22 of standard
        if(df5y){ 
          
          # Add variable that reflects middle age for each five-year group
          fmaS    <- seq(2, max(datFert[, fAge], na.rm = T), 5)                       # sequence of middle ages
          fmaSE   <- rep(fmaS, each = 5)                                              # sequence of middle age years
          fmaSmax <- rep(max(datFert[, fAge], na.rm = T), times = (nrow(datFert) - length(fmaSE))) # sequence of max age alues for rows that don't belong to a full 5-age group (e.g., age 100) 
          datFert[, "ageM"] <- c(fmaSE, fmaSmax) 
          
          # Obtain middle age fertility value
          fma <- datFert[datFert[, fAge]%in%unique(datFert[, "ageM"]), c(fAge, paste0("tpFx", rt[r], t))]
          
          # Apply middle age fertility value
          datFert <- datFert[, !names(datFert)%in%paste0("tpFx", rt[r], t)] # remove original variable
          datFert <- merge(datFert, fma, by.x = "ageM", by.y = fAge)        # merge in new variable
          datFert <- datFert[order(datFert[, fAge]),]                       # order datFert
          
          # Adjust estimates to meet target tfr
          # Note: this step is needed because the middle age assignment distorts the tfr value
          tfrDat <- sum(datFert[, paste0("tpFx", rt[r], t)])                # compute data specific tfr after conducting middle age assignment
          tfrDiffRate <- tfrF / tfrDat                                      # compute difference rate
          datFert[, paste0("tpFx", rt[r], t)] <- datFert[, paste0("tpFx", rt[r], t)]*tfrDiffRate # ajust fertility rates by difference rate
        }
      }  
    }
  } else { # Uses simple scaling of the standard fertility schedule
    
    #* Loop over years for which fertility data is needed
    for(t in 0:steps){
      # t=0
      if(vis){cat(t + yearStart," ")}
      
      #* Loop over region types (rural/urban)
      for(r in 1:(length(rt))){
        # r=1
        
        #* Obtain df of 1-year TFR values from scenario
        tfrDf1 <- get(paste0("tfrDf1", rt[r]))
        
        #* Declare TFR values used in fertility schedule computation
        tfrS <- sum(datFert[, paste0("tpFx", rt[r], "s")])  # standard
        tfrF <- tfrDf1[tfrDf1[, "year"] == t, "tfr"]        # future year
        
        # Adjust estimates to meet target tfr
        tfrDiffRate <- tfrF / tfrS                                                               # compute difference rate
        datFert[, paste0("tpFx", rt[r], t)] <- datFert[, paste0("tpFx", rt[r], "s")]*tfrDiffRate # ajust fertility rates by difference rate
      }
    }
  }
  
  # Reduce to needed variables
  datFert <- datFert[, c(fAge, paste0(rep(c("tpFxU", "tpFxR"), each = steps + 1), 0:steps))]
  
  tot.fert <- rbind(tot.fert, datFert)
}

# Write the total fertility table to a csv file
tot.fert.csv <- file.path(resultsPath, "tot.fert.csv")
write.csv(tot.fert, tot.fert.csv, row.names = FALSE)



###############################
#*** Population Projection ***#
##############################


#* The dataframe that contains updated population based on international and state-level migrations at each step
upd.pop <- as.data.frame(matrix(0, nrow = num.ages * 4, ncol = length(regUAll)))
colnames(upd.pop) <- regUAll

# The outer loop that goes through years
for (t in 1:steps){
  
  cat(paste("\nCurrent Year is", 2010 + t, "\n")) # print the projection year
  
  #* Initialize results matrix for the current year
  colN    <- c("age", "female", "urban") # generate vector of column names
  matProj <- as.data.frame(matrix(nrow = nrow(upd.all.base.pop), ncol = length(colN), dimnames = list(NULL, colN)))
  
  #* Add initial values to results matrix
  matProj[, "age"]         <- rep(c(rbind(datBP[, bpAge], datBP[,bpAge])), times=2) # age groups
  matProj[, "female"]      <- rep(c(1, 0), each = nrow(matProj) / 2) # gender
  matProj[, "urban"]       <- rep(c(1, 0), times = nrow(matProj) / 2) # urban vs. rural
  matProj[is.na(matProj)]  <- 0
  
  
  for (regU in 1:length(regUAll)){
    
    cat(" ", regUAll[regU])
  
    # Data preparation for Projection of the current state  
    
    #*** Declare required variables
    
    #* Generate paths
    pathIn      <- file.path(inputsPath, regUAll[regU])  # Input data directory
    
    #* Scenario data
    scenarioS   <- paste0(scenUAll[1], ".csv")
    scenario    <- read.csv(file.path(pathIn, scenarioS), check.names = F, stringsAsFactors = F)  # Input data directory
    
    #* International migration data
    # Note: The international migration rates input data needs to be in relative rates (summing up to 1)
    datNetMigS  <- "intMig.csv"           # file containing international migration rates
    datNetMig   <- read.csv(file.path(pathIn, datNetMigS), check.names = F, stringsAsFactors = F)
    
    # Variables
    netMigAge  <- "age"                   # Age variable
    netMigF    <- "net_female"            # Proportion of net migration for each age group for females 
    netMigM    <- "net_male"              # Proportion of net migration for each age group for males
    
    # Annual total net international migration counts for 5-year periods
    nMigFt     <- as.numeric(scenario[scenario$year%in%c(yearStart:yearEnd), "nim_F"]) # female net international migrants
    nMigMt     <- as.numeric(scenario[scenario$year%in%c(yearStart:yearEnd), "nim_M"]) # male net international migrants
    
    # Modify the constant rate scenario international migration assumptions if current scenario is something else
    if (cur.scenario != scenUAll[1]){
      
      # Retrive the national-level changes of net international migration according to the current scenario
      net.int.mig.f.rate <- cumprod(scenario.table[, "Int_Mig_change"])
      net.int.mig.m.rate <- cumprod(scenario.table[, "Int_Mig_change"])
      
      # Apply the national-level rates to the state-level net inernational migration assumptions
      nMigFt <- net.int.mig.f.rate[1:(length(net.int.mig.f.rate) - 1)] * nMigFt[1]
      nmdf1F <- data.frame(year = 0:90, nm = c(nMigFt[1], nMigFt))
      nMigMt <- net.int.mig.m.rate[1:(length(net.int.mig.m.rate) - 1)] * nMigMt[1]
      nmdf1M <- data.frame(year = 0:90, nm = c(nMigMt[1], nMigMt))
      
    } else {
      
      # Linearly interpolate 1-year total net international migration counts between 5-year intervals 
      nmdf1F <- f.linIntE(nMigFt, "nm", si = T)
      nmdf1M <- f.linIntE(nMigMt, "nm", si = T)
    }
    
    # Spread migrant numbers according to profile
    if (int.mig == 1){
      datNetMig[, "nmUF"] <- datNetMig[, netMigF] * nmdf1F[nmdf1F[, "year"] == t, "nm"]   # urban females
      datNetMig[, "nmRF"] <- datNetMig[, netMigF] * nmdf1F[nmdf1F[, "year"] == t, "nm"]   # rural females
      datNetMig[, "nmUM"] <- datNetMig[, netMigM] * nmdf1M[nmdf1M[, "year"] == t, "nm"]   # urban males
      datNetMig[, "nmRM"] <- datNetMig[, netMigM] * nmdf1M[nmdf1M[, "year"] == t, "nm"]   # rural males
    } else {
      datNetMig[, "nmUF"] <- datNetMig[, netMigF] * 0   # urban females
      datNetMig[, "nmRF"] <- datNetMig[, netMigF] * 0   # rural females
      datNetMig[, "nmUM"] <- datNetMig[, netMigM] * 0   # urban males
      datNetMig[, "nmRM"] <- datNetMig[, netMigM] * 0   # rural males
    }
    
    #* Domestic urban/rural migration data
    #* Urban/rural migration data in the base year (it's currently assumes that there is no urban/rural migration)
    datDomMigS  <- "domMig.csv" # file containing domestic migration rates
    datDomMig   <- read.csv(file.path(pathIn, datDomMigS), check.names=F, stringsAsFactors=F)
    
    # Variables
    domMigAge <- "age"                    # Age variable (numeric, starting with 0)
    ruMigF    <- "R-U_female"             # Female rural to urban migration 
    ruMigM    <- "R-U_male"               # Male rural to urban migration 
    urMigF    <- "U-R_female"             # Female urban to rural migration 
    urMigM    <- "U-R_male"               # Male urban to rural migration
    
    # Rename variables
    names(datDomMig)[names(datDomMig) == ruMigF] <- "dmFRU0" # females rural to urban 
    names(datDomMig)[names(datDomMig) == urMigF] <- "dmFUR0" # females urban to rural
    names(datDomMig)[names(datDomMig) == ruMigM] <- "dmMRU0" # males rural to urban 
    names(datDomMig)[names(datDomMig) == urMigM] <- "dmMUR0" # males urban to rural
    
    #Currently we assume no urban/rural migration
    datDomMig[is.na(datDomMig)] <- 0
    
    #* Sex ratio at birth according to the scenario
    # Note: Number of males born for every 100 females (e.g., 1.08 for 108 males born for 100 females) for every 5-year interval
    srR <- as.numeric(scenario[scenario$year%in%c(yearStart:yearEnd),"sr_R"]) # rural sex ratio
    srU <- as.numeric(scenario[scenario$year%in%c(yearStart:yearEnd),"sr_U"]) # urban sex ratio
    
    #* Linearly interpolate sex ratios between 5-year time steps at 1-year interval according to the scenario
    # Note: This generates a data frame with a "year" and "sr" column
    srDf1R <- f.linIntE(srR, "sr", si=T) # rural
    srDf1U <- f.linIntE(srU, "sr", si=T) # urban
    
    #* Compute Proportion male and females at birth according to the scenario
    # Rural
    srDf1R$propM <- (srDf1R$sr / 100) / (1 + (srDf1R$sr / 100))
    srDf1R$propF <- 1 - srDf1R$propM
    # Urban
    srDf1U$propM <- (srDf1U$sr / 100) / (1 + (srDf1U$sr / 100))
    srDf1U$propF <- 1 - srDf1U$propM
    
    #* Compute matrix representation of mortality variables for the current state
    # Note: function writes relevant variables to global environment
    dfmx <- tot.dfmx[seq(1 + (regU - 1) * num.ages, regU * num.ages), ]
    mlF  <- f.ltm(dfmx, datDomMig, t, "F") # Females
    mlM  <- f.ltm(dfmx, datDomMig, t, "M") # Males
    
    #* Compute matrix representation of Birth rates (Bx)
    # Note: the female Sx values are used for the Bx computation for both males and females
    datFert <- tot.fert[seq(1 + (regU - 1) * num.ages, regU * num.ages), ]
    mBxF    <- f.lBx(datFert, srDf1R, srDf1U, mlF$Lx, mlF$lx, mlF$Sx, t, "F")  # females
    mBxM    <- f.lBx(datFert, srDf1R, srDf1U, mlM$Lx, mlM$lx, mlF$Sx, t, "M")  # males
    
    #* Generate transition matrix
    # Diagonal matrix pieces of Survival
    # Note: for the matrix computation step of the projection it is necessary that the transition matrix is quadratic
    #       in this case it is necessary to add two "0" value columns to account for the merge of the Bx and Sx matrices
    #       use bdiag() function from Matrix package to construct a block diagonal matrix
    mSxFd <- cbind(as.matrix(bdiag(mlF$Sx)), 0, 0) # females
    mSxMd <- cbind(as.matrix(bdiag(mlM$Sx)), 0, 0) # males
    
    # Horizontal matrix pieces of Birth
    mBxFd <- cbind(do.call(cbind, mBxF), 0, 0)     # females
    mBxMd <- cbind(do.call(cbind, mBxM), 0, 0)     # males
    
    # Generate quadrant pieces of transition matrix
    mat11 <- rbind(mBxFd, mSxFd)                                                             # female births + female survival
    mat12 <- matrix(0, nrow = nrow(mat11), ncol = ncol(mat11))                               # no values
    mat21 <- rbind(mBxMd, matrix(0, nrow = (nrow(mat11) - nrow(mBxMd)), ncol = ncol(mat11))) # male births -> the new born boys are now computed based on the population of reproductive females
    mat22 <- rbind(matrix(0, nrow = (nrow(mat11) - nrow(mSxMd)), ncol = ncol(mat11)), mSxMd) # male survival
    
    # Combine quadrant pieces to complete matrix
    matTs                     <- rbind(cbind(mat11, mat12), cbind(mat21, mat22))
    matTs[is.na(matTs)]       <- 0       # recode all "NaN" values to 0
    matTs[is.infinite(matTs)] <- 0       # recodes all Inf and -Inf values to 0
    
    
    #* Project population
    # Note: For matrix multiplication A*B the number of columns in A must be identical to 
    #       the rows in B and the resulting matrix will have the number of raws of A 
    #       and the number of columns of B
    
    # For the first year, the transition matrix is applied to the base year population updated by international and state-level migration 
    if (t == 1){
      matProj[, paste0("pop_", regUAll[regU], "_", t + yearStart)] <- matTs%*%as.matrix(upd.all.base.pop[, regUAll[regU]])
    
    # For the other years the transition matrix is applied to the projected population of the previous year updated by international and state-level migration  
    } else {
      matProj[, paste0("pop_", regUAll[regU], "_", t + yearStart)] <- matTs%*%as.matrix(upd.pop[, regUAll[regU]])
      
    }
    
    # The projected population of the current year has already been saved in matProj
    
    # This step updates population based on international and state-level migrations for the next year
    
    # Update the population with the international migration
    matBP <- as.matrix(matProj[, paste0("pop_", regUAll[regU], "_", t + yearStart)])  # combines female and male pieces
    
    # Update the projected population with the international migration 
    int.mig.vec <- c(rbind(datNetMig[1:num.ages, "nmUF"], datNetMig[1:num.ages, "nmRF"]),
                     rbind(datNetMig[1:num.ages, "nmUM"], datNetMig[1:num.ages, "nmRM"]))
    
    matBP_upd   <- matBP + int.mig.vec
    matBP_upd[seq(2, nrow(matBP_upd), 2)] <- 0
    
    # Populate the data frames that store base-year population and its update population by international migration
    upd.pop[, regUAll[regU]] <- matBP_upd
    upd.pop[is.na(upd.pop)]  <- 0
    
    # Keep the international migration for the current state
    int.mig.vec[seq(2, length(int.mig.vec), 2)] <- 0
    tot.int.mig[((4*t*num.ages)+1):(4*(t+1)*num.ages), regUAll[regU]] <- int.mig.vec
    
  }
  
  # Now that one year projection for all states has finished, state-level migration can be applied 
  if (cur.scenario != scenUAll[1]){
    # State-level migration values are adjusted based on a factor that can be temporally variable
    # SSP2: The factor is 1 and constant
    # SSP3: The factor reduces from 1 to 0.5 gradually
    # SSP5: The factor increases fron 1 to 2 until the year with maximum international migration and them remains constant
    
    dom.mig.factor <- scenario.table[, "Dom_Mig_Factor"]
    
    # Calculate the total in out and net migration numbers for all states
    in.migration  <- f.in.dom.mig.calc(inputsPath, upd.pop, dom.mig.factor[t+1])
    out.migration <- f.out.dom.mig.calc(inputsPath, upd.pop, dom.mig.factor[t+1])
    net.migration <- in.migration - out.migration # People entered a state minus those who left
    
  } else {
    # The factor is constant based on the mechanical scenario (half, reg, double)
    # Calculate the total in out and net migration numbers for all states
    in.migration  <- f.in.dom.mig.calc(inputsPath, upd.pop, scen.factor)
    out.migration <- f.out.dom.mig.calc(inputsPath, upd.pop, scen.factor)
    net.migration <- in.migration - out.migration # People entered a state minus those who left
  }
  
  # Store the population before applying the domestic migration
  tot.pop.no.dom <- rbind(tot.pop.no.dom, upd.pop)
  
  # Update the projected population with the state-level migration, this will be used for the next year
  upd.pop <- upd.pop + net.migration
  
  # Add the results of the current year for all states to the dataframe that contains population for all years (tot.projection)
  colnames(matProj) <- c("age", "female", "urban", regUAll)
  tot.projection    <- rbind(tot.projection, matProj)
  
  # Add net/in/out migration values 
  tot.state.in.mig  <- rbind(tot.state.in.mig, in.migration)
  tot.state.out.mig <- rbind(tot.state.out.mig, out.migration)
  tot.state.net.mig <- rbind(tot.state.net.mig, net.migration)
  
}


###########################
#*** Output Generation ***#
###########################

# Create a csv file that contains population for all states and years
write.csv(tot.projection, file.path(resultsPath, "state_pop_projections.csv"), row.names = F)

# Create a csv file that contains population for all states and years before applying domestic migration
write.csv(tot.pop.no.dom, file.path(resultsPath, "state_pop_projections_no_dom.csv"), row.names = F)

# Create a csv file that contains state-level net migration values for all states and years
write.csv(tot.state.net.mig, file.path(resultsPath, "state_net_migrations.csv"), row.names = F)

# Create a csv file that contains state-level in migration values for all states and years
write.csv(tot.state.in.mig, file.path(resultsPath, "state_in_migrations.csv"), row.names = F)

# Create a csv file that contains state-level out migration values for all states and years
write.csv(tot.state.out.mig, file.path(resultsPath, "state_out_migrations.csv"), row.names = F)

# Create a csv file that contains international migration projections
tot.int.mig.csv <- file.path(resultsPath, "tot.int.mig.csv")
write.csv(tot.int.mig, tot.int.mig.csv, row.names = F)


# Create a separate csv file per state

# Population
# Initialize the dataframe to be saved as a csv file for the state
cur.results.table             <- as.data.frame(matrix(0, num.ages * 4, steps + 5))
colnames(cur.results.table)   <- c("state", "age", "female", "urban", "2010", as.character(yearStart + 1:steps))
cur.results.table[, "age"]    <- rep(c(rbind(datBP[, bpAge], datBP[,bpAge])), times=2) # age groups
cur.results.table[, "female"] <- rep(c(1, 0), each = nrow(matProj) / 2) # gender
cur.results.table[, "urban"]  <- rep(c(1, 0), times = nrow(matProj) / 2) # urban vs. rural

# Loop through times and states to extract the specific data per state 
for (col in regUAll){
  for (t in 0:steps){
    if (t == 0){
      cur.results.table[, as.character(yearStart + t)] <- ini.all.base.pop[, col]
    } else {
      cur.results.table[, as.character(yearStart + t)] <- tot.projection[(1 + (t - 1) * num.ages * 4): (t * num.ages * 4), col]
    }
  }
  cur.results.table[, "state"] <- substr(col, nchar(col) - 1, nchar(col))
  cur.res.path <- file.path(resultsPath, col)
  if(!file.exists(cur.res.path)) {dir.create(cur.res.path)}
  write.csv(cur.results.table, file.path(cur.res.path, paste0(col, "_proj_pop", ".csv")), row.names = F)
}


# Net migration
# Initialize the dataframe to be saved as a csv file for the state
cur.results.table             <- as.data.frame(matrix(0, num.ages * 4, steps + 5))
colnames(cur.results.table)   <- c("state", "age", "female", "urban", "2010", as.character(yearStart + 1:steps))
cur.results.table[, "age"]    <- rep(c(rbind(datBP[, bpAge], datBP[,bpAge])), times=2) # age groups
cur.results.table[, "female"] <- rep(c(1, 0), each = nrow(matProj) / 2) # gender
cur.results.table[, "urban"]  <- rep(c(1, 0), times = nrow(matProj) / 2) # urban vs. rural

# Loop through times and states to extract the specific data per state 
for (col in regUAll){
  for (t in 0:steps){
    cur.results.table[, as.character(yearStart + t)] <- tot.state.net.mig[(1 + t * num.ages * 4): ((t + 1) * num.ages * 4), col]
  }
  cur.results.table[, "state"] <- substr(col, nchar(col) - 1, nchar(col))
  cur.res.path <- file.path(resultsPath, col)
  if(!file.exists(cur.res.path)) {dir.create(cur.res.path)}
  write.csv(cur.results.table, file.path(cur.res.path, paste0(col, "_proj_net_mig", ".csv")), row.names = F)
}


# In migration
# Initialize the dataframe to be saved as a csv file for the state
cur.results.table             <- as.data.frame(matrix(0, num.ages * 4, steps + 5))
colnames(cur.results.table)   <- c("state", "age", "female", "urban", "2010", as.character(yearStart + 1:steps))
cur.results.table[, "age"]    <- rep(c(rbind(datBP[, bpAge], datBP[,bpAge])), times=2) # age groups
cur.results.table[, "female"] <- rep(c(1, 0), each = nrow(matProj) / 2) # gender
cur.results.table[, "urban"]  <- rep(c(1, 0), times = nrow(matProj) / 2) # urban vs. rural

# Loop through times and states to extract the specific data per state 
for (col in regUAll){
  for (t in 0:steps){
    cur.results.table[, as.character(yearStart + t)] <- tot.state.in.mig[(1 + t * num.ages * 4): ((t + 1) * num.ages * 4), col]
  }
  cur.results.table[, "state"] <- substr(col, nchar(col) - 1, nchar(col))
  cur.res.path <- file.path(resultsPath, col)
  if(!file.exists(cur.res.path)) {dir.create(cur.res.path)}
  write.csv(cur.results.table, file.path(cur.res.path, paste0(col, "_proj_in_mig", ".csv")), row.names = F)
}


# Out migration
# Initialize the dataframe to be saved as a csv file for the state
cur.results.table             <- as.data.frame(matrix(0, num.ages * 4, steps + 5))
colnames(cur.results.table)   <- c("state", "age", "female", "urban", "2010", as.character(yearStart + 1:steps))
cur.results.table[, "age"]    <- rep(c(rbind(datBP[, bpAge], datBP[,bpAge])), times=2) # age groups
cur.results.table[, "female"] <- rep(c(1, 0), each = nrow(matProj) / 2) # gender
cur.results.table[, "urban"]  <- rep(c(1, 0), times = nrow(matProj) / 2) # urban vs. rural

# Loop through times and states to extract the specific data per state 
for (col in regUAll){
  for (t in 0:steps){
    cur.results.table[, as.character(yearStart + t)] <- tot.state.out.mig[(1 + t * num.ages * 4): ((t + 1) * num.ages * 4), col]
  }
  cur.results.table[, "state"] <- substr(col, nchar(col) - 1, nchar(col))
  cur.res.path <- file.path(resultsPath, col)
  if(!file.exists(cur.res.path)) {dir.create(cur.res.path)}
  write.csv(cur.results.table, file.path(cur.res.path, paste0(col, "_proj_out_mig", ".csv")), row.names = F)
}



#######################
#*** Visualization ***#
#######################

# Projected population visualization
# Loop through times and states to extract the specific data per state 
options(scipen=10000)
for (col in regUAll){
  # Read the projection table for the current state 
  cur.res.path <- file.path(resultsPath, col)
  cur.res.csv  <- file.path(cur.res.path, paste0(col, "_proj_pop", ".csv"))
  proj.table   <- read.csv(cur.res.csv, check.names = F, stringsAsFactors = F)
  
  data           <- data.frame(apply(proj.table[,5:ncol(proj.table)], 2, sum))
  colnames(data) <- "Pop"
  
  cur.plot <- ggplot(data, aes(x = as.numeric(rownames(data)), y = Pop / 1000, group = 1)) + 
    geom_line(linetype = "solid", color = "red", size = 2) + 
    labs(title = paste("Population Change through Time for", substr(col, nchar(col) - 1, nchar(col))), x = "Year", y = "Population / 1000") +
    scale_x_continuous(breaks = seq(2010, 2100, by = 10)) +
    scale_y_continuous(breaks = scales::pretty_breaks(n = 10)) +
    theme_bw() +
    theme(plot.title   = element_text(size = 25, face = "bold", hjust = 0.5), 
          axis.text.x  = element_text(size = 18, angle = 45, margin = margin(15,0,0,0)), 
          axis.text.y  = element_text(size = 18),
          axis.title.x = element_text(size = 20),
          axis.title.y = element_text(size = 20, margin = margin(0,10,0,0)),
          plot.margin = margin(1, 0.8, 1, 0.8, "cm"),
          panel.grid.major = element_blank(),
          axis.line = element_line(colour = "black")) 
  
  
  ggsave(filename = file.path(cur.res.path, paste0(col, ".jpg")), plot = cur.plot, width = 12, height = 8)
}


# Projected net migration visualization
# Loop through times and states to extract the specific data per state 
for (col in regUAll){
  # Read the projection table for the current state 
  cur.res.path <- file.path(resultsPath, col)
  cur.res.csv  <- file.path(cur.res.path, paste0(col, "_proj_net_mig", ".csv"))
  proj.table   <- read.csv(cur.res.csv, check.names = F, stringsAsFactors = F)
  
  data           <- data.frame(apply(proj.table[,5:ncol(proj.table)], 2, sum))
  colnames(data) <- "Net_Migration"
  
  cur.plot <- ggplot(data, aes(x = as.numeric(rownames(data)), y = Net_Migration / 100, group = 1)) + 
    geom_line(linetype = "solid", color = "red", size = 2) + 
    labs(title = paste("Net Migration Change through Time for", substr(col, nchar(col) - 1, nchar(col))), x = "Year", y = "Net Domestic Migration / 100") +
    scale_x_continuous(breaks = seq(2010, 2100, by = 10)) +
    scale_y_continuous(breaks = scales::pretty_breaks(n = 10)) +
    theme_bw() +
    theme(plot.title   = element_text(size = 25, face = "bold", hjust = 0.5), 
          axis.text.x  = element_text(size = 18, angle = 45, margin = margin(15,0,0,0)), 
          axis.text.y  = element_text(size = 18),
          axis.title.x = element_text(size = 20),
          axis.title.y = element_text(size = 20, margin = margin(0,10,0,0)),
          plot.margin = margin(1, 0.8, 1, 0.8, "cm"),
          panel.grid.major = element_blank(),
          axis.line = element_line(colour = "black")) 
  
  
  ggsave(filename = file.path(cur.res.path, paste0(col, "_Net_Mig.jpg")), plot = cur.plot, width = 12, height = 8)
}


# Projected in migration visualization
# Loop through times and states to extract the specific data per state 
for (col in regUAll){
  # Read the projection table for the current state 
  cur.res.path <- file.path(resultsPath, col)
  cur.res.csv  <- file.path(cur.res.path, paste0(col, "_proj_in_mig", ".csv"))
  proj.table   <- read.csv(cur.res.csv, check.names = F, stringsAsFactors = F)
  
  data           <- data.frame(apply(proj.table[,5:ncol(proj.table)], 2, sum))
  colnames(data) <- "In_Migration"
  
  cur.plot <- ggplot(data, aes(x = as.numeric(rownames(data)), y = In_Migration / 100, group = 1)) + 
    geom_line(linetype = "solid", color = "red", size = 2) + 
    labs(title = paste("In-migration Change through Time for", substr(col, nchar(col) - 1, nchar(col))), x = "Year", y = "Domestic In-Migration / 100") +
    scale_x_continuous(breaks = seq(2010, 2100, by = 10)) +
    scale_y_continuous(breaks = scales::pretty_breaks(n = 10)) +
    theme_bw() +
    theme(plot.title   = element_text(size = 25, face = "bold", hjust = 0.5), 
          axis.text.x  = element_text(size = 18, angle = 45, margin = margin(15,0,0,0)), 
          axis.text.y  = element_text(size = 18),
          axis.title.x = element_text(size = 20),
          axis.title.y = element_text(size = 20, margin = margin(0,10,0,0)),
          plot.margin = margin(1, 0.8, 1, 0.8, "cm"),
          panel.grid.major = element_blank(),
          axis.line = element_line(colour = "black")) 
  
  
  ggsave(filename = file.path(cur.res.path, paste0(col, "_In_Mig.jpg")), plot = cur.plot, width = 12, height = 8)
}


# Projected out migration visualization
# Loop through times and states to extract the specific data per state 
for (col in regUAll){
  # Read the projection table for the current state 
  cur.res.path <- file.path(resultsPath, col)
  cur.res.csv  <- file.path(cur.res.path, paste0(col, "_proj_out_mig", ".csv"))
  proj.table   <- read.csv(cur.res.csv, check.names = F, stringsAsFactors = F)
  
  data           <- data.frame(apply(proj.table[,5:ncol(proj.table)], 2, sum))
  colnames(data) <- "Out_Migration"
  
  cur.plot <- ggplot(data, aes(x = as.numeric(rownames(data)), y = Out_Migration / 100, group = 1)) + 
    geom_line(linetype = "solid", color = "red", size = 2) + 
    labs(title = paste("Out-migration Change through Time for", substr(col, nchar(col) - 1, nchar(col))), x = "Year", y = "Domestic Out-Migration / 100") +
    scale_x_continuous(breaks = seq(2010, 2100, by = 10)) +
    scale_y_continuous(breaks = scales::pretty_breaks(n = 10)) +
    theme_bw() +
    theme(plot.title   = element_text(size = 25, face = "bold", hjust = 0.5), 
          axis.text.x  = element_text(size = 18, angle = 45, margin = margin(15,0,0,0)), 
          axis.text.y  = element_text(size = 18),
          axis.title.x = element_text(size = 20),
          axis.title.y = element_text(size = 20, margin = margin(0,10,0,0)),
          plot.margin = margin(1, 0.8, 1, 0.8, "cm"),
          panel.grid.major = element_blank(),
          axis.line = element_line(colour = "black")) 
  
  
  ggsave(filename = file.path(cur.res.path, paste0(col, "_Out_Mig.jpg")), plot = cur.plot, width = 12, height = 8)
}




