#' Replace IDs
#'
#' @param dfLabs data frame containing labels
#' @param dfRep data frame containing the variable to be relabeled
#' @param varO variable in dfLabs containing the original ID
#' @param varN variable in dfLabs containing the new ID
#' @param varR variable in dfRep containing the original ID that should be replaced with the new ID
#' @return The replaced data frame
#' @export
#'
f.repID <- function(dfLabs, dfRep, varO, varN, varR){
  for(i in 1:nrow(dfLabs)){
    # i=1

    # Find matches of first value in varO in varR
    mat <- grep(paste0("^",dfLabs[i,varO],"$"),dfRep[,varR])

    # Replace match
    if(any(mat)){
      dfRep[mat,varR] <- dfLabs[i,varN]
    }
  }
  return(dfRep)
}
