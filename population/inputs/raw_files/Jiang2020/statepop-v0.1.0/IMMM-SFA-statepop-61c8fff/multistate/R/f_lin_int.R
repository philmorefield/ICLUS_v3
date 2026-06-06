#' Function to perform linear interpolation of rates/values. Uses 5-year intervals to compute values for 1-year intervals
#'
#' @param x vector of values
#' @param valN name of variable (str) to hold 1-year values
#' @param si should the first value be used for the start of the first interval (e.g., year=0) -> TRUE; or at the center of the first interval (e.g., year=2) -> FALSE
#' @return linearly interpolated values at 1-year interval
#' @export
#'
f.linIntE <- function(x, valN, si){

  #* Generate 5-year df
  if(si==T){
    df5 <- data.frame(year=seq(0,(length(x)*5)-1,5),stringsAsFactors=F)    # scenario value assigned at the start of the 5-year interval (e.g., year 0 for interval 0-4)
  }else if (si==F){
    df5 <- data.frame(year=seq(2,(length(x)*5)-3,5),stringsAsFactors=F)    # scenario value assigned at the middle of the 5-year interval (e.g., year 2 for interval 0-4)
  }
  df5[,valN] <- x # adds values to df

  #* Generate 1-year df
  df1        <- data.frame(year=0:steps,stringsAsFactors=F)
  df1[,valN] <- NA #generates empty vector to hold interpolated values

  # Perform interpolation
  for(y in 1:nrow(df1)){
    # y=1
    df1[y,valN] <- f.lInt(df=df5, x="year", y=valN, xval=df1$year[y])
  }
  return(df1)
}
