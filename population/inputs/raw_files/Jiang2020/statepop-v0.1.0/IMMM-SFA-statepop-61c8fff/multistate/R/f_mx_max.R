#' Compute mx value of last age category (e.g., 100+) based on user specified life expectancy at age 0 for the base year
#'
#' @param g gender (str); "F" or "FR" or "FU" = female (general, rural, urban); "M" or "MR" or "MU" = male (general, rural, urban)
#' @param e0u life expectancy at age zero (numeric)
#' @return mx value of last age category
#' @export
#'
f.mxMax <- function(g, e0u){
  if(g == "F" | g == "FR" | g == "FU"){
    if(e0u <= 70){
      mm <- -0.0000018*e0u^3 + 0.0003520*e0u^2 - 0.023099*e0u + 1.0650
    }else if(e0u > 70){
      mm <- 0.00000029*e0u^3 - 0.0218*e0u + 2.0386
    }
  } else if(g == "M"|g == "MR"|g == "MU"){
    if(e0u <= 70){
      mm <- -0.0000021*e0u^3 + 0.0003771*e0u^2 - 0.023437*e0u + 1.11422
    }else if(e0u > 70){
      mm <- 0.00000037*e0u^3 - 0.025077*e0u + 2.2893914
    }
  }
  return(mm)
}
