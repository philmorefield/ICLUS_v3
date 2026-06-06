#' Function to compute mortality variables as matrices
#'
#' @param dfMort Data Frame containing mortality variables in the form "mx"+gender+region+year (e.g., "mxFR2")
#' @param dfDomMig Data Frame containing variables on domestic (internal) migration propabilities in the form "dm"+gender+region+year (e.g., "dmMUR0")
#' @param t year for which mortality data is needed in the projection
#' @param g gender "F"=female; "M"=male
#' @return mortality variables
#' @export
#'
f.ltm <- function(dfMort, dfDomMig, t, g){

  #* Initialize lists
  Mx=list()
  Zx=list()
  lx=list()
  Lx=list()
  Sx=list()

  #* Initialize matrix tools
  im=diag(2)          # identity matrix
  lm0=im*100000       # lx for age 0

  #* Generate matrices of Mx
  # Note: Domestic migration rates do not change over time
  for(a in 1:nrow(dfMort)){
    # a=95
    Mx[[a]]=matrix(0,2,2)
    Mx[[a]][1,1]=dfMort[a,paste0("mx",g,"U",t-1)]+dfDomMig[a,paste0("dm",g,"UR",0)] # urban mortality + urban-to-rural migration
    Mx[[a]][2,2]=dfMort[a,paste0("mx",g,"R",t-1)]+dfDomMig[a,paste0("dm",g,"RU",0)] # rural mortality + rural-to-urban migration
  }

  #* Compute matrix Zx (survival rate)
  # Note: With I as the identity matrix, the computation of (I-Mg/2)/(I+Mg/2) is performed only for the diagonal cases (mortality rate + migration rate)
  #       The NaN values at positions [2,1] and [1,2] are replaced by the positive rural-to-urban and urban-to-rural migration rates
  for(a in 1:length(Mx)){
    # a=1
    Zx[[a]]=(im-(Mx[[a]]/2))/(im+(Mx[[a]]/2)) # regular division
    Zx[[a]][1,2]=dfDomMig[a,paste0("dm",g,"RU",0)] # rural-to-urban migration
    Zx[[a]][2,1]=dfDomMig[a,paste0("dm",g,"UR",0)] # urban-to-rural migration
  }

  #* Compute matrix lx
  # Note: lx+1=Zx*lx
  for(a in 1:length(Mx)){
    # a=1
    if(a==1){ # assign 100,000 to first diagonal matrix
      lx[[a]]=lm0
    }else if(a>1){
      lx[[a]]=Zx[[a-1]]%*%lx[[a-1]]
    }
  }

  #* Compute matrix Lx
  # Note: Lx=(lx+1+lx)/2
  #       Matrix division is conducted by multiplying (%*%) by the inverse matrix (solve(Mat))
  for(a in 1:(length(Mx))){
    # a=1
    if(a<length(Mx)){
      Lx[[a]]=(lx[[a]]+lx[[a+1]])/2
    }else if(a==length(Mx)){ # for last age category: L100=l100/M100
      Lx[[a]]=lx[[a]]%*%solve(Mx[[a]])
    }
  }

  #* Compute matrix Sx
  # Note: Sx=Lx+1/Lx; Sx value for last age group (e.g., 100+) is undefined - but not needed for projection
  for(a in 1:(length(Mx)-1)){
    # a=1
    Sx[[a]]=Lx[[a+1]]%*%solve(Lx[[a]])

    # Set domestic migration to 0 after age 85
    if(a>85){
      Sx[[a]][2,1]=0 # urban-to-rural migration
      Sx[[a]][1,2]=0 # rural-to-urban migration
    }
  }

  # Sx max; Use the prior to last age group values (e.g., 98) for the last age group (e.g., 99) to avoid Sx values >1
  maxP=length(Sx) # obtain maximum position for Sx (e.g., 100 for age group 99)
  Sx[[maxP]]=Sx[[maxP-1]]


  #* Add relevant outputs to named list
  rl=list(lx=lx,Lx=Lx,Sx=Sx)

  return(rl)
}
