#' Linear interpolation
#'
#' @param df data frame
#' @param x variable in df that contains val
#' @param y variable in df for which value should be interpolated
#' @param xval value on x for which a corresponding value of y should be found
#' @return y value that corresponds to xval
#' @export
#'
f.lInt <- function(df, x, y, xval){

  # Limit df to important variables
  df <- df[,c(y,x)]

  # Find two values on x closest to val
  df$diff <- abs(df[,x]-xval) #generates difference measure
  pick    <- sort(df[df$diff%in%sort(df$diff)[1:2],x]) #picks the two x value closest to val

  # Define distance measures
  A1 <- pick[2]-pick[1] #difference between two x values
  A2 <- xval-pick[1] #difference between smalles x value and val
  B1 <- df[df[,x]==pick[2],y]-df[df[,x]==pick[1],y] #difference between respective y values

  # Linear interpolation y value
  # Note: A1/A2=B1/B2 -> B2=B1*A2/A1
  B2 <- (B1*A2/A1)+df[df[,x]==pick[1],y]

  return(B2)
}
