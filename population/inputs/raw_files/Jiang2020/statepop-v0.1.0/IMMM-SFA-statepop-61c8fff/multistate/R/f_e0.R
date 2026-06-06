#' Compute e0 (life expectancy at age zero) values from lx values of the observed mortality dataframe
#'
#' @param df data frame containing lx values
#' @param lx variable (str) containing lx values in 100,000 people
#' @param age variable (str) containing age information
#' @param g specify gender (str); "F" or "FR" or "FU" = female (general, rural, urban); "M" or "MR" or "MU" = male (general, rural, urban); only needed for mx max computation
#' @param e0u life expectancy at age zero e0 (numeric)
#' @return e0 (life expectancy at age zero)
#' @export
#'
f.e0 <- function(df, lx, age, g, e0u){

  # Restrict data to needed variables
  df <- df[,c(age,lx)]

  #* Percentage of survivals
  df$ax <- c(0.33, rep(0.5, nrow(df) - 1))

  #* Deaths per age group (dx)
  # Note: dx=lx0-lx1;
  df[, "dx"] <- NA
  df[1:(nrow(df)-1), "dx"] <- df[1:(nrow(df)-1), lx] - df[2:nrow(df), lx]

  #* Exposure to death per person year (Lx)
  # Note: Lx=(lx1*1)+(dx0*0.5); similar to Lx=0.33*(lx0+lx1)
  df[,"Lx"] <- NA
  df[1:(nrow(df) - 1), "Lx"] <- (df[2:nrow(df), lx] * 1) + (df[1:(nrow(df) - 1), "dx"] * df[1:(nrow(df) - 1), "ax"])

  #* Mortality rate (mx); the percentage of individuals in a certain age group that died in the last year
  # Note: mx=dx/Lx
  df[, "mx"] <- df[, "dx"] / df[, "Lx"]

  #* Compute mx for last age category (e.g., 100+)
  # Note: based on user specified life expectancy at age 0 for the base year
  mm   <- f.mxMax(g, e0u)             # compute mx max value
  maxy <- max(df[, age])              # obtain maximum age group
  df[df[, age] == maxy, "mx"] <- mm  # assign mx max value

  #* Compute Lx value for oldest age category (Lxmax)
  # Note: Lxmax=lxmax/mxmax
  df[df[, age] == maxy, "Lx"] <- df[df[, age] == maxy, lx] / mm

  #* Cumulative person years of survival
  # Note: Cumulative Lx from oldest to youngest age group
  df[, "Tx"] <- rev(cumsum(rev(df[, "Lx"])))

  #* Generate proportion of individuals that survived to the next age group (Sx)
  # Note: Sx=Lx1/Lx0; Smax-1=S99=T100+/T99; Smax=0
  df[, "Sx"] <- NA
  df[1:(nrow(df) - 1), "Sx"] <- df[2:nrow(df), "Lx"] / df[1:(nrow(df) - 1), "Lx"]
  maxy1 <- max(df[, age]) - 1 # obtain second to last year
  df[df[, age] == maxy1, "Sx"] <- df[df[, age] == maxy, "Tx"] / df[df[, age] == maxy1, "Tx"]
  df[df[, age] == maxy, "Sx"]  <- 0 # assign 0 value for Sx value of last age group because no one survived

  #* Compute life expectancy by age (ex)
  # Note: ex=Tx/lx
  df[,"ex"] <- df[, "Tx"] / df[, lx]

  #* Obtain life expectancy at birth (e0)
  e0result <- df[df[, age] == 0, "ex"]

  return(e0result)
}
