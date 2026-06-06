# Author:     Hamidreza Zoraghein
# Date:       2018-12-07
# Purpose:    Visualization of contributing factors to U.S. state-level population

#######################
#*** Load packages ***#
#######################
# Various packages
if ("ggplot2" %in% installed.packages())
{
  library(ggplot2)
} else {
  install.packages("ggplot2")
  library(ggplot2)
}

if ("reshape2" %in% installed.packages())
{
  library(reshape2)
} else {
  install.packages("reshape2")
  library(reshape2)
}


#############################
#*** Set general options ***#
#############################
options("scipen"=100, "digits"=4) # to force R not to use scientific notations
#options(warn=2)  # treat warnings as errors


###############
#*** Paths ***#
###############
path <- "C:/Users/Hamidreza.Zoraghein/Google Drive/Sensitivity_Analysis/Bilateral"


###################################
#*** Declare general variables ***#
###################################

#* Specify regions
regUAll <- c("9-CT", "23-ME", "25-MA", "33-NH", "44-RI", "50-VT", "34-NJ", "36-NY", "42-PA", "17-IL", "18-IN", "26-MI", "39-OH", 
             "55-WI", "19-IA", "20-KS", "27-MN", "29-MO", "31-NE", "38-ND", "46-SD", "10-DE", "11-DC", "12-FL", "13-GA", "24-MD", 
             "37-NC", "45-SC", "51-VA", "54-WV", "1-AL", "21-KY", "28-MS", "47-TN", "5-AR", "22-LA", "40-OK", "48-TX", "4-AZ", 
             "8-CO", "16-ID", "30-MT", "32-NV", "35-NM", "49-UT", "56-WY", "2-AK", "6-CA", "15-HI", "41-OR", "53-WA")

#* Specify scenario
# Note: currently Constant_rate
scenarios <- c("No_Mig", "Zero_Dom_Mig", "Reg_Scenario")


#############################################
#***            Main Program             ***#
#############################################
 
tot.pop.df            <- data.frame(matrix(nrow = length(regUAll), ncol = length(scenarios) + 1))
colnames(tot.pop.df) <- c("State", scenarios)
rownames(tot.pop.df) <- regUAll

for (scenario in scenarios){
  
  #The current scenario folder storing population tables
  results.path <- file.path(path, scenario)
  
  for (state.num in 1:length(regUAll)){
    
    #Read the current state's csv file to a dataframe 
    state.csv <- file.path(results.path, regUAll[state.num], paste0(regUAll[state.num], "_proj_pop.csv"))
    state.df  <- read.csv(state.csv, check.names = F, stringsAsFactors = F)
    
    #Populate the total dataframe with the 2050 to 2010 population change across all states and scenarios 
    tot.pop.df[state.num, "State"]  <- substr(regUAll[state.num], nchar(regUAll[state.num]) - 1, nchar(regUAll[state.num]))
    tot.pop.df[state.num, scenario] <- sum(state.df[, "2050"]) / sum(state.df[, "2010"])
  }
}

#Calculate the changes contributed by each scenario
tot.pop.df[, "IntVsNoInt"] <- tot.pop.df[, "Zero_Dom_Mig"] - tot.pop.df[, "No_Mig"] + 1
tot.pop.df[, "DomVsNoDom"] <- tot.pop.df[, "Reg_Scenario"] - tot.pop.df[, "Zero_Dom_Mig"] + 1

#Reformat the table for plot drawing
melt.tot.pop.df <- melt(tot.pop.df[, c("State", "No_Mig", "IntVsNoInt", "DomVsNoDom")], id.vars = "State")

ggplot(melt.tot.pop.df, aes(x = State, y = value - 1, fill = variable)) +
  geom_bar(stat='identity', position = "dodge") +
  scale_y_continuous(breaks = seq(-2, 3, 0.2), labels = seq(-2, 3, 0.2) + 1) +
  labs(title = "Population Change between 2010 and 2050 According to Different Migration Scenarios",
       x = "State",
       y = "Ratio of Change (2010-2050)") +
  scale_fill_discrete(name = "Scenario", 
                      breaks = c("No_Mig", "IntVsNoInt", "DomVsNoDom"),
                      labels = c("Fertility/Mortality/Age", "International Migration Added",
                                 "Domestic Migration Added")) +
theme_bw() +
  theme(plot.title   = element_text(size = 25, face = "bold", hjust = 0.5), 
        axis.text.x  = element_text(size = 15, angle = 90), 
        axis.text.y  = element_text(size = 15),
        axis.title.x = element_text(size = 18),
        axis.title.y = element_text(size = 18),
        legend.title = element_text(size=18),
        legend.text  = element_text(size=18),
        plot.margin  = margin(1, 0.8, 1, 0.8, "cm"),
        panel.grid.minor = element_blank(), 
        legend.justification = c(1, 1),
        legend.position = c(0.99, 0.99),
        axis.line = element_line(colour = "black")) 


# Save the plot
plot.path <- file.path(path, "Scenario_Comp.jpg")
ggsave(filename = plot.path, width = 22, height = 8)


