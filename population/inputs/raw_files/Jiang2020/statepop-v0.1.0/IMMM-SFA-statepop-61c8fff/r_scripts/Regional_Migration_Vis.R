# Author:     Hamidreza Zoraghein
# Date:       2019-02-08
# Purpose:    Visualizes projected net inter-regional migration (West, Southwest, Northeast and South) over all years.  

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


#############################
#*** Set general options ***#
#############################
options("scipen"=100, "digits"=4) # to force R not to use scientific notations


###############
#*** Paths ***#
###############
# Workspace
path <- "C:/Users/hzoraghein/Google Drive/Sensitivity_Analysis/Bilateral"

# The csv containing states, regions and dividions
states.csv <- file.path(path, "Regions_Divisions.csv")

scenario     <- "SSP2"
results.path <- file.path(path, scenario) # Path to results folder 


###################################
#*** Declare general variables ***#
###################################
num.ages <- 101 #From 0 to 100


#############################################
#***              Main Body              ***#
#############################################
# Read the csv containing states, regions and dividions
states.regions.df <- read.csv(states.csv, stringsAsFactors = F, check.names = F)

#Create a list of states and regions
#Unique regions
regions                           <- unique(states.regions.df$Region)
states.regions.list               <- vector(mode="list")
states.regions.list[[regions[1]]] <- states.regions.df$`State Code`[states.regions.df$Region == regions[1]]
states.regions.list[[regions[2]]] <- states.regions.df$`State Code`[states.regions.df$Region == regions[2]]
states.regions.list[[regions[3]]] <- states.regions.df$`State Code`[states.regions.df$Region == regions[3]]
states.regions.list[[regions[4]]] <- states.regions.df$`State Code`[states.regions.df$Region == regions[4]]


regions.mig.df                 <- data.frame(matrix(NA, nrow=length(regions) * (length(regions)-1) * 91, ncol=5))
colnames(regions.mig.df)       <- c("Year", "Focal_Region", "Other_Region", "In_Value", "Out_Value")
regions.mig.df["Year"]         <- rep(2010:2100, each=12)
regions.mig.df["Focal_Region"] <- rep(regions, each=3)
regions.mig.df["Other_Region"] <- c(regions[-1], regions[-2], regions[-3], regions[-4])


for (t in 0:90){
  
  cat(paste("The current year is", 2010+t, "\n"))
  
  #Calculate the mutual in-migration values for all regions
  for (region in regions){
    
    cat(paste("The current region is", region, "\n"))
    
    # Specify the states inside the current region
    inside.states <- states.regions.list[[region]]
    
    # All other regions
    other.regions <- regions.mig.df$Other_Region[regions.mig.df$Focal_Region == region][1:(length(regions)-1)]
    
    # Read the current state's csv file to a dataframe 
    for (other.region in other.regions){
      
      # Initialize the in-maigration value from the current other region to the current region
      in.migration  <- 0
      
      # All states inside the current other region
      other.states  <- states.regions.list[[other.region]]
      
      # Read and sum the out-migration from each other state to the inside states that form the current region for the current time
      for (other.state in other.states){
        state.csv <- file.path(results.path, other.state, paste0(other.state, "_total_out_mig.csv"))
        state.df  <- read.csv(state.csv, check.names = F, stringsAsFactors = F)[seq(num.ages*4*t+1, num.ages*4*(t+1))
                                                                                , inside.states]
        in.migration <- in.migration + sum(state.df)
      }
      
      regions.mig.df[(regions.mig.df$Year == 2010+t & regions.mig.df$Focal_Region == region & regions.mig.df$Other_Region == other.region),
                     "In_Value"] <- in.migration
    }
    
  }
}



for (t in 0:90){
  
  cat(paste("The current year is", 2010+t, "\n"))
  
  #Calculate the mutual in-migration values for all regions
  for (region in regions){
    
    cat(paste("The current region is", region, "\n"))
    
    # Specify the states inside the current region
    inside.states <- states.regions.list[[region]]
    
    # All other regions
    other.regions <- regions.mig.df$Other_Region[regions.mig.df$Focal_Region == region][1:(length(regions)-1)]
    
    # Read the current state's csv file to a dataframe 
    for (other.region in other.regions){
      
      # Initialize the in-maigration value from the current other region to the current region
      out.migration  <- 0
      
      # All states inside the current other region
      other.states  <- states.regions.list[[other.region]]
      
      # Read and sum the out-migration from each other state to the inside states that form the current region for the current time
      for (in.state in inside.states){
        state.csv <- file.path(results.path, in.state, paste0(in.state, "_total_out_mig.csv"))
        state.df  <- read.csv(state.csv, check.names = F, stringsAsFactors = F)[seq(num.ages*4*t+1, num.ages*4*(t+1))
                                                                                , other.states]
        out.migration <- out.migration + sum(state.df)
      }
      
      regions.mig.df[(regions.mig.df$Year == 2010+t & regions.mig.df$Focal_Region == region & regions.mig.df$Other_Region == other.region),
                     "Out_Value"] <- out.migration
    }
    
  }
}


# Add a column to store net migration values and save the final dataframe
regions.mig.df["Net_Value"] <- regions.mig.df["In_Value"] - regions.mig.df["Out_Value"]
regions.mig.csv             <- file.path(results.path, "Regional_Migration.csv")
write.csv(regions.mig.df, regions.mig.csv, row.names = F)


# Visualize regional migration patterns per region
colrs <- c("West" = "blue", "South" = "red", "Northeast" = "green", "Midwest" = "violet")
for (region in regions){
  
  cur.region.df <- regions.mig.df[regions.mig.df$Focal_Region == region,]
  
  
  cur.plot <- ggplot(cur.region.df, aes(x = Year, y = Net_Value, col = Other_Region)) + 
    geom_line(linetype = "solid", size = 2) + 
    labs(title = paste("Regional Net Migration Change for", region), x = "Year", y = "Migrants") +
    scale_x_continuous(breaks = seq(2010, 2100, by = 10)) +
    scale_y_continuous(breaks = scales::pretty_breaks(n = 10)) +
    scale_color_manual(values = colrs) +
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
  
  ggsave(filename = file.path(results.path, paste0(region, ".jpg")), plot = cur.plot, width = 12, height = 8)
}

