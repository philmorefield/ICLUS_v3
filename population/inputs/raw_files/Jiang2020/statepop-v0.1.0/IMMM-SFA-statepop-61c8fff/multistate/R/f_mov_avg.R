#' Moving average. Assumes that data frame is sorted correctly
#'
#' @param df data frame
#' @param var variable for which moving average should be computed
#' @param vart time variable that uniquely identifies a time step within each group (same length)
#' @param ws window size (default=3); must be odd number (integer)
#' @return smoothed data frame
#' @export
#'
f.movAv <- function(df, var, vart, ws=3){

  # Declare variable containing center cell
  df$cent <- df[,var]

  mi <- min(df[,vart]) # min of time variable
  mx <- max(df[,vart]) # max of time variable

  if(ws==3){

    # Lag and lead variables
    df$lead1 <- c(NA,df[1:(nrow(df)-1),var]) # lead by 1 cell
    df[df$agecat1==mi,"lead1"] <- NA         # assigns value that was lead from the prior case a NA value (avoids case overlap)

    df$lag1 <- c(df[2:nrow(df),var],NA)    # lag by 1 cell
    df[df$agecat1==mx,"lag1"] <- NA        # assigns value that was lagged from the following case a NA value (avoids case overlap)

    # Recompute input variable
    df[,var] <- rowMeans(df[,c("cent","lead1","lag1")],na.rm=T)

    # Remove temporary computing variables
    df <- df[,!names(df)%in%c("cent","lead1","lag1")]

  }else if(ws==5){

    # Lag and lead variables
    df$lead1 <- c(NA,df[1:(nrow(df)-1),var])       # lead by 1 cell
    df[df$agecat1==mi,"lead1"] <- NA               # assigns value that was lead from the prior case a NA value (avoids case overlap)
    df$lead2 <- c(NA,NA,df[1:(nrow(df)-2),var])    # lead by 2 cells
    df[df$agecat1%in%c(mi:(mi+1)),"lead2"] <- NA   # assigns value that was lead from the prior case a NA value (avoids case overlap)

    df$lag1 <- c(df[2:nrow(df),var],NA)            # lag by 1 cell
    df[df$agecat1==mx,"lag1"] <- NA                # assigns value that was lagged from the following case a NA value (avoids case overlap)
    df$lag2 <- c(df[3:nrow(df),var],NA,NA)         # lag by 2 cell
    df[df$agecat1%in%c((mx-1):mx),"lag2"] <- NA    # assigns value that was lagged from the following case a NA value (avoids case overlap)

    # Recompute input variable
    df[,var] <- rowMeans(df[,c("cent","lead1","lead2","lag1","lag2")],na.rm=T)

    # Remove temporary computing variables
    df <- df[,!names(df)%in%c("cent","lead1","lead2","lag1","lag2")]

  }

  return(df)
}
