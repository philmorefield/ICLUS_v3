#' Linear interpolation of columns
#'
#' @param df data frame containing x1 and x2
#' @param x1 variable containing the x values at level y1
#' @param x2 variable containing the x values at level y2
#' @param y1 level of y for x1 (number)
#' @param y2 level of y for x2 (number)
#' @param yi level of y for which x should be interpolated (number)
#' @return vector containing the imputed values of xi
#' @export
#'
f.lIntC <- function(df, x1, x2, y1, y2, yi){
  # Note: (x2-x1)/(xi-x1)=(y2-y1)/(yi-y1) |solve for xi
  #       xi=x1+(x2-x1)*((yi-y1)/(y2-y1))

  # Limit df to important variables
  df <- df[, c(x1, x2)]

  # Generate scaler consisting of y values
  scaler <- (yi - y1) / (y2 - y1)

  # Compute xi values
  xi <- df[, x1] + (df[, x2] - df[, x1]) * scaler

  return(xi)
}
