# Author:     Hamidreza Zoraghein
# Date:       2019-02-26
# Purpose:    Compares and visualizes the national-level population projections from our bilateral model to the equivalent projection from IIASA.

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


###############
#*** Paths ***#
###############
# Workspace
path <- "C:/Users/Hamidreza.Zoraghein/Google Drive/Sensitivity_Analysis/Bilateral"

# IIASA's population projections for the U.S.
iiasa.us.csv <- file.path(path, "US_IIASA_Pop.csv")

scenario <- "SSP2"

# Path to results folder 
results.path <- file.path(path, scenario) 


###################################
#*** Declare general variables ***#
###################################
num.ages <- 101 #From 0 to 100

#* Specify regions
regUAll <- c("9-CT", "23-ME", "25-MA", "33-NH", "44-RI", "50-VT", "34-NJ", "36-NY", "42-PA", "17-IL", "18-IN", "26-MI", "39-OH", 
             "55-WI", "19-IA", "20-KS", "27-MN", "29-MO", "31-NE", "38-ND", "46-SD", "10-DE", "11-DC", "12-FL", "13-GA", "24-MD", 
             "37-NC", "45-SC", "51-VA", "54-WV", "1-AL", "21-KY", "28-MS", "47-TN", "5-AR", "22-LA", "40-OK", "48-TX", "4-AZ", 
             "8-CO", "16-ID", "30-MT", "32-NV", "35-NM", "49-UT", "56-WY", "2-AK", "6-CA", "15-HI", "41-OR", "53-WA")

#############################################
#***              Main Body              ***#
#############################################
# Read the csv containing states, regions and dividions
tot.pop.csv <- file.path(results.path, "state_pop_projections.csv")
tot.pop.df  <- read.csv(tot.pop.csv, stringsAsFactors = F, check.names = F)


# Initiate a dataframe that will hold national population in each year
pop.dataframe           <- data.frame(matrix(NA, nrow = 91, ncol = 2))
colnames(pop.dataframe) <- c("Year", "Pop")
pop.dataframe[, "Year"] <- 2010 + 0:90


# Populate the dataframe across all years
for (t in 0:90){
  pop.dataframe[pop.dataframe$Year == 2010+t, "Pop"] <- sum(tot.pop.df[seq(num.ages*4*t+1, num.ages*4*(t+1)), regUAll])
}
pop.dataframe$Pop  <- pop.dataframe$Pop/1000000


# Read and standardize the IIASA population for comparison plots
iiasa.us.df  <- read.csv(iiasa.us.csv, stringsAsFactors = F, check.names = F) 
iiasa.us.df  <- iiasa.us.df[, c("Scenario", as.character(seq(2010, 2100, 5)))]

iiasa.us.reshaped.df <- melt(iiasa.us.df, id = "Scenario", variable.name = "Year", value.name = "Pop")


# Extract population values for the current scenario
iiasa.us.reshaped.df      <- iiasa.us.reshaped.df[iiasa.us.reshaped.df$Scenario == scenario, ]
iiasa.us.reshaped.df$Type <- "IIASA National Model"
iiasa.us.reshaped.df      <- iiasa.us.reshaped.df[, c("Year", "Pop", "Type")]


# Bind the two population dataframes for comparison
# Extract only years for which SSP exists
pop.dataframe      <- pop.dataframe[pop.dataframe$Year %in% seq(2010, 2100, 5),]
pop.dataframe$Type <- "New State-Level Model"

both.pop.df      <- rbind(pop.dataframe, iiasa.us.reshaped.df)
both.pop.df$Year <- as.numeric(both.pop.df$Year)

# Visualize regional migration patterns per region
cur.plot <- ggplot(both.pop.df, aes(x = Year, y = Pop, col = Type)) + 
  geom_line(linetype = "solid", size = 2) + 
  labs(title = paste("Projected Population Values over Time under", scenario)) +
  ylab("Population/1000000") +
  scale_x_continuous(breaks = seq(2010, 2100, by = 5)) +
  scale_y_continuous(breaks = scales::pretty_breaks(n = 10)) +
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

ggsave(filename = file.path(results.path, "iiasa_comp.jpg"), plot = cur.plot, width = 12, height = 8)


