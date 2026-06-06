#' Function to calculate the total in migration numbers for all states
#'
#' @param input.path input folder containing the migration rates
#' @param pop.dataframe population data frame
#' @param scen.factor scale applied to migration rates based on the scenario
#' @return data frame containing in-migration values for all states
#' @export
#'
f.in.dom.mig.calc <- function(input.path, pop.dataframe, scen.factor){

  # The final output dataframe that holds total in-migration numbers for all states
  tot.tot.in           <- data.frame(matrix(0, nrow = nrow(pop.dataframe), ncol = ncol(pop.dataframe)))
  colnames(tot.tot.in) <- colnames(pop.dataframe)

  states <- colnames(pop.dataframe)
  for (state in states){

    # Read the csv file holding in migration rates to the current state
    cur.in.path <- file.path(input.path, state, paste0(state, "_in_mig.csv"))
    cur.in.mig  <- read.csv(cur.in.path, stringsAsFactors = F, check.names = F)

    # Retrieve population of all states that contributed migration to the current one
    from.states  <- colnames(pop.dataframe)[which(colnames(pop.dataframe) != state)]
    cur.from.pop <- pop.dataframe[, from.states]

    # Create a dataframe to hold in migration rates of all states contributing to the current one
    cur.from.mig           <- data.frame(matrix(0, nrow = nrow(cur.from.pop), ncol = ncol(cur.from.pop)))
    colnames(cur.from.mig) <- colnames(cur.from.pop)

    # Populate the in migration dataframe with the in migration rates of all contributing states
    for (col in colnames(cur.from.mig)){

      # The current state that is contributing population to the focal state (state)
      cur.from.state  <- substr(col, nchar(col) - 1, nchar(col))

      # Female rates, rural is assumed 0
      cur.from.mig[seq(1, nrow(cur.from.pop) / 2 , 2), col] <- cur.in.mig[cur.in.mig$from == cur.from.state & cur.in.mig$gender == "f",
                                                                          ncol(cur.in.mig)]

      # Male rates, rural is assumed 0
      cur.from.mig[seq((nrow(cur.from.pop) * 0.5) + 1, nrow(cur.from.pop), 2), col] <- cur.in.mig[cur.in.mig$from == cur.from.state & cur.in.mig$gender == "m",
                                                                                                  ncol(cur.in.mig)]
    }

    # Multiplication gives us the in migration numbers from each contributing state
    cur.in.net <- (cur.from.mig * scen.factor) * cur.from.pop

    # Sum the total in migration numbers across all contributing states, now we have one total value per age group
    cur.tot.in <- data.frame(apply(cur.in.net, 1, sum, na.rm = T))

    # Add the values of the current focal state to the output dataframe
    tot.tot.in[, state] <- cur.tot.in
  }

  return(tot.tot.in)
}
