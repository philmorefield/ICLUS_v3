rm(list = ls(all.names = TRUE))
gc(reset = TRUE)

library(lmtest)
library(pscl)
library(RSQLite)
library(sandwich)

age_groups = c("5_TO_9", "10_TO_14", "15_TO_19", "20_TO_24", "25_TO_29",
               "30_TO_34", "35_TO_39", "40_TO_44", "45_TO_49", "50_TO_54",
               "55_TO_59", "60_TO_64", "65_TO_69", "70_TO_74", "75_TO_79",
               "80_TO_84", "85_TO_115")

input_db = "D:\\projects\\ICLUS_v3\\population\\inputs\\databases\\zinb_regression_inputs.sqlite"
output_db = "D:\\projects\\ICLUS_v3\\population\\outputs\\zinb_regression_outputs.sqlite"

idb = dbConnect(SQLite(), dbname = input_db)
odb = dbConnect(SQLite(), dbname = output_db)

for (race in c("WHITE", "BLACK", "AIAN", "API", "OTHER"))
  {
  for (age_group in age_groups)
    {
    print(paste("Started ", race, ", ", age_group, ": ", Sys.time(), sep = ""))

    df = dbReadTable(idb, paste("zinb_inputs_Census_1990", race, age_group, sep = '_'))

    df$ln_Pi = log(df$Pi + 1)
    df$ln_Pj = log(df$Pj + 1)
    df$ln_Tij = log(df$Tij + 1)
    df$ln_Cij = log(df$Cij + df$Pj + 1)
    df$ln_Pj_star = log(df$Pj_star + 1)

    m1 = zeroinfl(formula = FLOW ~ ln_Pi + ln_Pj + ln_Tij + ln_Cij + ln_Pj_star + factor(SAME_LABOR_MARKET) + factor(MICRODEST20) + factor(METRODEST20), data = df, dist = 'negbin', link = 'cloglog')
    print(summary(m1))
    robust = coeftest(x = m1, vcov = sandwich)
    new_row_coef = data.frame(t(robust[,1]))
    new_row_coef$RACE = race
    new_row_coef$AGE_GROUP = age_group
    if (exists("coef_table")) {
      coef_table = rbind(coef_table, new_row_coef)
    } else {
      coef_table = new_row_coef
    }

    new_row_sig = data.frame(t(robust[,4]))
    new_row_sig[is.na(new_row_sig)] = 9999.0  # occasional NA values show up as P = 0
    new_row_sig$CONVERGED = m1["converged"]$converged
    new_row_sig$RACE = race
    new_row_sig$AGE_GROUP = age_group

    if (exists("sig_table")) {
      sig_table = rbind(sig_table, new_row_sig)
    } else {
      sig_table = new_row_sig
    }

    predictions = cbind(subset(df, select = c('ORIGIN_FIPS', 'DESTINATION_FIPS', 'FLOW')), PREDICTED = predict(m1, newdata = df))
    predictions$PREDICTED = as.integer(round(predictions$PREDICTED))
    dbWriteTable(conn = odb, name = paste("zinb", "Census_1990", race, age_group, sep = "_"), value = predictions, overwrite = TRUE)
    }
  }

dbWriteTable(conn = odb, name = "coefficients_Census_1990", value = coef_table, overwrite = TRUE)
dbWriteTable(conn = odb, name = "significance_Census_1990", value = sig_table, overwrite = TRUE)

dbDisconnect(idb)
dbDisconnect(odb)

rm(list = ls(all.names = TRUE))
gc(reset = TRUE)
