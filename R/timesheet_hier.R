

create_ntrrque_hier <- function(opt_bann_conn,
                                opt_ntrrque_data,
                                opt_snapshot_data) {
  require(tidyverse)
  require(magrittr)

  # if (missing(opt_snapshot_data) | missing(opt_ntrrque_data)) {
  #
  #   if (missing(opt_bann_conn)) {
  #     bann_conn <- opa::get_banner_conn()
  #   } else {
  #     bann_conn <- opt_bann_conn
  #   }
  #
  #   if (missing(opt_ntrrque_data)) {
  #     ntrrque <- tbl(bann_conn, "NTRRQUE")
  #   } else {
  #     ntrrque <- opt_ntrrque_data
  #   }
  #
  #   if (missing(opt_snapshot_data)) {
  #     curr_date <- Sys.Date() %>% as.POSIXct() %>% as.character(format = "%Y-%m-%d")
  #     snpsht <- opa::get_banner_snapshot(date = curr_date, return_input_params = F)
  #   } else {
  #     snpsht <- opt_snapshot_data
  #   }
  #
  # } else {
    snpsht <- opt_snapshot_data
    ntrrque <- opt_ntrrque_data
  # }

  snpsht_ts <- snpsht %>%
    select(JOB_KEY, PIDM, POSN, SUFF, JOB_TS_ORG)

  ntrrque_approvers <- ntrrque %>%
    select(TS_CODE = NTRRQUE_ORGN_CODE,
           TS_APPR_POSN = NTRRQUE_APPR_POSN)

  snpsht_ts <- left_join(snpsht_ts, ntrrque_approvers, by = c("JOB_TS_ORG" = "TS_CODE"))

  pres_posn <- "4E3320"

  return(snpsht_ts)
}

# facilities_services
#   EJ Hook
#   Frank Stock
#   Kane Urdahl
#   Robert Hebert

# facilities_services_pidms <- c(1249497, 222760, 927195, 515438)
#
# level_one_posns <- snapshot %>%
#   filter(PIDM %in% facilities_services_pidms) %>%
#   select(POSN)
#
# ntrrque_l1_posns <- filter(ntrrque, NTRRQUE_APPR_POSN %in% level_one_posns$POSN)
#
# level_two_posns <- snapshot %>%
#   filter(JOB_TS_ORG %in% ntrrque_l1_posns$NTRRQUE_ORGN_CODE)
#
# ntrrque_l2_posns <- filter(ntrrque, NTRRQUE_APPR_POSN %in% level_two_posns$POSN)
#
# level_three_posns <- snapshot %>%
#   filter(JOB_TS_ORG %in% ntrrque_l2_posns$NTRRQUE_ORGN_CODE)
# ntrrque_l3_posns <- filter(ntrrque, NTRRQUE_APPR_POSN %in% level_three_posns$POSN)
#
# level_four_posns <- snapshot %>%
#   filter(JOB_TS_ORG %in% ntrrque_l3_posns$NTRRQUE_ORGN_CODE)
# ntrrque_l4_posns <- filter(ntrrque, NTRRQUE_APPR_POSN %in% level_four_posns$POSN)
#
# level_five_posns <- snapshot %>%
#   filter(JOB_TS_ORG %in% ntrrque_l4_posns$NTRRQUE_ORGN_CODE)
# ntrrque_l5_posns <- filter(ntrrque, NTRRQUE_APPR_POSN %in% level_five_posns$POSN)
