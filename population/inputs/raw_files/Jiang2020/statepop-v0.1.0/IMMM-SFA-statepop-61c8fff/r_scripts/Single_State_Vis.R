# Author:     Hamidreza Zoraghein
# Date:       2018-12-07
# Purpose:    For each state, it plots population change from 2010 to 2100 based on assuming no migration, only international migration and
#             both international and domestic migration.

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
path <- "C:/Users/Hamidreza.Zoraghein/Google Drive/Sensitivity_Analysis"


###################################
#*** Declare general variables ***#
###################################

#* Specify migration scenarios
scenarios <- c("No_Mig", "Zero_Dom_Mig", "Reg_Scenario")

#* Specify model
models <- c("Simple", "Bilateral")

#* Model folder according to the model specification
model.folder <- file.path(path, models[2])

#* Output folder for saving the plots
output.folder <- file.path(model.folder, "Individual_States")
if (!file.exists(output.folder)) dir.create(output.folder)

#* Regions
regUAll <- c("9-CT", "23-ME", "25-MA", "33-NH", "44-RI", "50-VT", "34-NJ", "36-NY", "42-PA", "17-IL", "18-IN", "26-MI", "39-OH", 
             "55-WI", "19-IA", "20-KS", "27-MN", "29-MO", "31-NE", "38-ND", "46-SD", "10-DE", "11-DC", "12-FL", "13-GA", "24-MD", 
             "37-NC", "45-SC", "51-VA", "54-WV", "1-AL", "21-KY", "28-MS", "47-TN", "5-AR", "22-LA", "40-OK", "48-TX", "4-AZ", 
             "8-CO", "16-ID", "30-MT", "32-NV", "35-NM", "49-UT", "56-WY", "2-AK", "6-CA", "15-HI", "41-OR", "53-WA")

#* Years
start.year <- 2010
end.year   <- 2100


#############################################
#***              Main Body              ***#
#############################################

# Create a dataframe for each state that will hold population values from different migration scenarios
tot.pop.df <- data.frame(matrix(0, nrow = length(seq(start.year, end.year)), ncol = length(scenarios) + 1))
colnames(tot.pop.df) <- c("Year", scenarios[1], scenarios[2], scenarios[3])
tot.pop.df[, 1]      <- seq(start.year, end.year)

# Populate the datafram per state with population values according to the different migration scenarios
for (region in regUAll){
  for (i in seq(1, length (scenarios))){
    # Read the population per current state per current region
    cur.scenario.folder <- file.path(model.folder, scenarios[i])
    cur.state.folder    <- file.path(cur.scenario.folder, region)
    cur.state.csv       <- file.path(cur.state.folder, paste0(region, "_proj_pop.csv"))
    cur.pop.df          <- read.csv(cur.state.csv, stringsAsFactors = F, check.names = F)
    
    # Add population values across age groups and save them in the dataframe that contains population values for all years and scenarios
    tot.pop.df[, i+1] <- apply(cur.pop.df[, seq(5, ncol(cur.pop.df))], 2, sum)
    
    # Modify the dataframe for plotting
    mod.pop.df <- melt(data = tot.pop.df, id.vars = "Year", variable.name = "Model", value.name = "Population")
    
    # Plot temporal changes of population for the current state according to different migration scenarios
    cur.plot <- ggplot(mod.pop.df, aes(x = Year, y = Population / 1000, col = Model)) + 
      geom_line(linetype = "solid", size = 2) + 
      labs(title = paste("Population Change through Time for", substr(region, nchar(region) - 1, nchar(region))), x = "Year", y = "Population / 1000") +
      scale_x_continuous(breaks = seq(2010, 2100, by = 10)) +
      scale_y_continuous(breaks = scales::pretty_breaks(n = 10)) +
      scale_color_manual(labels = c("No Migration", "+International Migration", "+International/Domestic Migration"), values = c("red", "green", "blue")) +
      theme_bw() +
      theme(plot.title   = element_text(size = 25, face = "bold", hjust = 0.5), 
            axis.text.x  = element_text(size = 18, angle = 45, margin = margin(15,0,0,0)), 
            axis.text.y  = element_text(size = 18),
            axis.title.x = element_text(size = 20),
            axis.title.y = element_text(size = 20, margin = margin(0,10,0,0)),
            legend.title =  element_blank(),
            legend.text  = element_text(size = 15),
            legend.position  = "bottom",
            legend.spacing.x = unit(1.0, 'cm'),
            plot.margin = margin(1, 0.8, 1, 0.8, "cm"),
            panel.grid.major = element_blank(),
            axis.line = element_line(colour = "black")) 
    
    ggsave(filename = file.path(output.folder, paste0(region, ".jpg")), plot = cur.plot, width = 12, height = 8)
  }
}
 




