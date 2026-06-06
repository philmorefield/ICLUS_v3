# Author:     Hamidreza Zoraghein
# Date:       2019-01-04
# Purpose:    Standardize the bilateral domestic migration rates per state to be used in the bilateral model. It creates two csv filed for each
#             state, one including in-migration rates from all other states at 1-year age groups from 0 to 100 and the other out-migration rates
#             from each state to all other states.

###############
#*** Paths ***#
###############
workspace     <- "C:/Users/Phil/OneDrive/Dissertation/data/Jiang/statepop-v0.1.0/IMMM-SFA-statepop-61c8fff/inputs"
state.inputs  <- file.path(workspace, "State_Inputs")
bilateral.csv <- file.path(workspace, "bilateral.csv")


####################
#*** Functions ***#
####################
insert.row <- function(existing.df, new.row, r) {
  existing.df[seq(r+1,nrow(existing.df)+1),] <- existing.df[seq(r,nrow(existing.df)),]
  existing.df[r,] <- new.row
  existing.df
}


####################
#*** Parameters ***#
####################
states <- c("9-CT", "23-ME", "25-MA", "33-NH", "44-RI", "50-VT", "34-NJ", "36-NY", "42-PA", "17-IL", "18-IN", "26-MI", "39-OH", 
            "55-WI", "19-IA", "20-KS", "27-MN", "29-MO", "31-NE", "38-ND", "46-SD", "10-DE", "11-DC", "12-FL", "13-GA", "24-MD", 
            "37-NC", "45-SC", "51-VA", "54-WV", "1-AL", "21-KY", "28-MS", "47-TN", "5-AR", "22-LA", "40-OK", "48-TX", "4-AZ", 
            "8-CO", "16-ID", "30-MT", "32-NV", "35-NM", "49-UT", "56-WY", "2-AK", "6-CA", "15-HI", "41-OR", "53-WA")


####################
#*** Main Body  ***#
####################
# Read the csv file containing bilateral migration rates
bilateral.raw <- read.csv(bilateral.csv, stringsAsFactors = F, check.names = F)

# Refine the dataframe
# Remove within state migration rates
bilateral <- bilateral.raw[bilateral.raw$from != bilateral.raw$to, ]

# Remove migration to and from Puerto Rico 
bilateral <- bilateral[bilateral$from != "PR", ]
bilateral <- bilateral[bilateral$to   != "PR", ]

# Remove international migration
bilateral <- bilateral[bilateral$from != "INT", ]
bilateral <- bilateral[bilateral$to   != "INT", ]

# Change the structure from 5-year age groups to 1-year intervals
bilateral <- bilateral[rep(seq_len(nrow(bilateral)), each = 5), ]

# Update age identification groups
age.groups <- unique(bilateral$age)
for (group in seq_len(length(age.groups))){
  bilateral[bilateral$age == age.groups[group], "age"] <- rep(seq(5*(group - 1), 5*group - 1), 5100)
}

rownames(bilateral) <- seq_len(dim(bilateral)[1])

# Retain the relevant columns
bilateral <- bilateral[, colnames(bilateral)[colnames(bilateral) %in% c("from", "to", "gender", "age", "rate")]]

# Extract the index of the rows whose age group is 99
indexes.100 <- which(bilateral$age == "99")

# Add another row for the age 100 with the same values as 99
i <- 0
for (row in indexes.100){
  new.row <- row + i
  bilateral <- insert.row(bilateral, 
                          c(bilateral[new.row, 1], bilateral[new.row, 2], bilateral[new.row, 3], as.character(as.numeric(bilateral[new.row, 4]) + 1), bilateral[new.row, 5]),
                          new.row + 1)
  i <- i + 1
}
bilateral <- bilateral[-nrow(bilateral),]


# Create in- and out-migration csv file per state
for (state in states){
  cur.output.path <- file.path(state.inputs, state)
  cur.state       <- substr(state, nchar(state) - 1, nchar(state))
  
  cur.in.mig  <- bilateral[bilateral$to   == cur.state,]
  cur.out.mig <- bilateral[bilateral$from == cur.state,]
  
  cur.in.path  <- file.path(cur.output.path, paste0(state, "_in_mig.csv"))
  cur.out.path <- file.path(cur.output.path, paste0(state, "_out_mig.csv"))
  
  write.csv(cur.in.mig, cur.in.path, row.names = F)
  write.csv(cur.out.mig, cur.out.path, row.names = F)
}






