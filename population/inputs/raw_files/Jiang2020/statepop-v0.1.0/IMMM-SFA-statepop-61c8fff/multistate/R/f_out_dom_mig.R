#' Function to calculate the total out migration numbers for all states
#'
#' @param input.path input folder containing the migration rates
#' @param pop.dataframe population data frame
#' @param scen.factor scale applied to migration rates based on the scenario
#' @return data frame containing out-migration values for all states
#' @export
#'
f.out.dom.mig.calc <- function(input.path, pop.dataframe, scen.factor){

  # The final output dataframe that holds total out-migration numbers for all states
  tot.tot.out           <- data.frame(matrix(0, nrow = nrow(pop.dataframe), ncol = ncol(pop.dataframe)))
  colnames(tot.tot.out)  <- colnames(pop.dataframe)

  states <- colnames(pop.dataframe)
  for (state in states){

    # Read the csv file holding out migration rates from the current state
    cur.out.path <- file.path(input.path, state, paste0(state, "_out_mig.csv"))
    cur.out.mig  <- read.csv(cur.out.path, stringsAsFactors = F, check.names = F)

    # Retrieve population of all states that received out-migration from the current one
    to.states  <- colnames(pop.dataframe)[which(colnames(pop.dataframe) != state)]
    cur.to.pop <- pop.dataframe[, state]

    # Create a dataframe to hold out migration rates of all states receiving from the current one
    cur.to.mig           <- data.frame(matrix(0, nrow = nrow(pop.dataframe), ncol = length(to.states)))
    colnames(cur.to.mig) <- to.states

    # Populate the out migration dataframe with the out migration rates of all receiving states
    for (col in colnames(cur.to.mig)){

      # The current state that is receiving population from the focal state (state)
      cur.to.state  <- substr(col, nchar(col) - 1, nchar(col))

      # Female rates, rural is assumed 0
      cur.to.mig[seq(1, nrow(pop.dataframe) / 2 , 2), col] <- cur.out.mig[cur.out.mig$to == cur.to.state & cur.out.mig$gender == "f",
                                                                          ncol(cur.out.mig)]

      # Male rates, rural is assumed 0
      cur.to.mig[seq((nrow(pop.dataframe) * 0.5) + 1, nrow(pop.dataframe), 2), col] <- cur.out.mig[cur.out.mig$to == cur.to.state & cur.out.mig$gender == "m",
                                                                                                   ncol(cur.out.mig)]
    }

    # Multiplication gives us the out migration numbers to each receiving state
    cur.out.net <- (cur.to.mig * scen.factor) * cur.to.pop

    # Sum the total out migration numbers across all receiving states, now we have one total value per age group
    cur.tot.out <- data.frame(apply(cur.out.net, 1, sum, na.rm = T))

    # Add the values of the current focal state to the output dataframe
    tot.tot.out[, state] <- cur.tot.out
  }

  return(tot.tot.out)
}
