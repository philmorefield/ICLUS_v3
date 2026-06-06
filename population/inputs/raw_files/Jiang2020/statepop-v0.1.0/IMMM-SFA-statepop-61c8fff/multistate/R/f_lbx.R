#' Function to compute matrix representation of birth rates (Bx)
#'
#' @param dfFert Data Frame containing fertility variables in the form "tpFx"+region+step (e.g., "tpFxR2")
#' @param dfSrR Data frame containing sex ratios for rural area (variables: propM and propF)
#' @param dfSrU Data frame containing sex ratios for urban area (variables: propM and propF)
#' @param Lx nested list containing maxtrices of Lx values
#' @param lx nested list containing maxtrices of lx values
#' @param SxF nested list containing maxtrices of Sx values for females
#' @param t time step used in the projection
#' @param g gender "F"=female; "M"=male
#' @return matrix representation of birth rates (Bx)
#' @export
#'
f.lBx <- function(dfFert, dfSrR, dfSrU, Lx, lx, SxF, t, g){
  # t=1

  # Initialize lists
  Fx <- list()
  Bx <- list()

  #* Generate matrix representation of fertility (Fx)
  for(a in 1:nrow(dfFert)){
    # a=1
    Fx[[a]] <- matrix(0,2,2)
    Fx[[a]][1,1] <- dfFert[a,paste0("tpFxU",t-1)]*dfSrU[dfSrU[,"year"]==t-1,paste0("prop",g)] # urban fertility * sex ratio
    Fx[[a]][2,2] <- dfFert[a,paste0("tpFxR",t-1)]*dfSrR[dfSrR[,"year"]==t-1,paste0("prop",g)] # rural fertility * sex ratio
  }

  #* Compute matrix of Birth rates (Bx)
  # Note: Bx=L0/l0*0.5*(Fx+(Fx+1*SxF))
  for(a in 1:length(SxF)){
    # a=21
    Bx[[a]] <- (Lx[[1]]%*%solve(lx[[1]]))*0.5*(Fx[[a]]+(Fx[[a+1]]%*%SxF[[a]]))
  }

  return(Bx)
}
