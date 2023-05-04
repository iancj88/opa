compile_rank_tenure_wide <- function(opt_tenure_data,
                                      opt_rank_data,
                                      opt_bann_conn,
                                      opt_start_date,
                                      opt_end_date) {

# intialize function ------------------------------------------------------

   suppressPackageStartupMessages({
     require(dplyr)
     require(tictoc)
     require(magrittr)
     require(tidyr)
     require(lubridate)
   })

   # set to UTC time zone to ensure compatibility with Banner dates
   Sys.setenv(TZ = "UTC")
   Sys.setenv(ORA_SDTZ = "UTC")

# pull tenure/rank data ---------------------------------------------------

   if (missing(opt_tenure_data) | missing(opt_rank_data)) {
     tic("pull rank and tenure data")
     if (missing(opt_bann_conn)) {
       bann_conn <- opa::get_banner_conn()
     } else {
       bann_conn <- opt_bann_conn
     }
     if (missing(opt_rank_data)) {
       rank_data <- opa::get_rank_records(return_most_recent = F,
                                          opt_bann_conn = bann_conn)

     } else {
       rank_data <- opt_rank_data
     }
     if (missing(opt_tenure_data)) {
       tenure_data <- opa::get_tenure_status(return_most_recent = FALSE,
                                             opt_bann_conn = bann_conn)
     } else {
       tenure_data <- opt_tenure_data
     }
     toc()
   } else {
     tenure_data <- opt_tenure_data
     rank_data <- opt_rank_data
   }


# build wide rank tenure df -----------------------------------------------

   tenure_data_wide <- tenure_data %>%
     group_by(pidm, tenure_code) %>%
     summarize(min_eff_date = min(tenure_eff_date)) %>%
     pivot_wider(id_cols = pidm,
                 names_from = tenure_code,
                 names_prefix = "eff_date_tenure_",
                 values_from = min_eff_date)

  rank_data_wide <- rank_data %>%
    group_by(pidm, rank_code_agg_desc) %>%
    summarize(min_eff_date = min(rank_begin_date)) %>%
    pivot_wider(id_cols = pidm,
                names_from = rank_code_agg_desc,
                names_prefix = "eff_date_rank_",
                values_from = min_eff_date)

  rank_tenure_all <- full_join(tenure_data_wide,
                               rank_data_wide,
                               by = "pidm")

  rank_tenure_all <- select(rank_tenure_all,
                            pidm,
                            eff_date_tenure_NT,
                            eff_date_tenure_PT,
                            eff_date_tenure_CT,
                            eff_date_tenure_NA,
                            eff_date_rank_asst_prof = `eff_date_rank_Assistant Professor`,
                            eff_date_rank_assc_prof = `eff_date_rank_Associate Professor`,
                            eff_date_rank_prof = `eff_date_rank_Professor`,
                            eff_date_rank_instructor = `eff_date_rank_Instructor`,
                            eff_date_rank_lecturer = eff_date_rank_Lecturer,
                            eff_date_rank_other = eff_date_rank_Other,
                            eff_date_rank_non_std = `eff_date_rank_Nonstandard academic ranking`) %>%
    mutate(rank_asst_assc_int = eff_date_rank_asst_prof %--% eff_date_rank_assc_prof,
           rank_asst_assc_day_length = int_length(rank_asst_assc_int) / 60 / 60 / 24,
           rank_assc_prof_int = eff_date_rank_assc_prof %--% eff_date_rank_prof,
           rank_assc_prof_day_length = int_length(rank_assc_prof_int) / 60 / 60 / 24,
           tenure_pt_ct_int = eff_date_tenure_PT %--% eff_date_tenure_CT,
           tenure_pt_ct_day_length = int_length(tenure_pt_ct_int)  / 60 / 60 / 24)

  return(rank_tenure_all)

 }

 build_rank_tenure_gantt <- function(df,
                                     pidm_col = pidm) {}

 build_rank_tenure_long <- function(df,
                                    opt_rank_data,
                                    opt_tenure_data,
                                    opt_bann_conn) {


    # pull tenure/rank data ---------------------------------------------------

    if (missing(opt_tenure_data) | missing(opt_rank_data)) {
       tic("pull rank and tenure data")
       if (missing(opt_bann_conn)) {
          bann_conn <- opa::get_banner_conn()
       } else {
          bann_conn <- opt_bann_conn
       }
       if (missing(opt_rank_data)) {
          rank_data <- opa::get_rank_records(return_most_recent = F,
                                             opt_bann_conn = bann_conn)

       } else {
          rank_data <- opt_rank_data
       }
       if (missing(opt_tenure_data)) {
          tenure_data <- opa::get_tenure_status(return_most_recent = FALSE,
                                                opt_bann_conn = bann_conn)
       } else {
          tenure_data <- opt_tenure_data
       }
       toc()
    } else {
       tenure_data <- opt_tenure_data
       rank_data <- opt_rank_data
    }



 }


#' Calculate faculty status based on Job Type
#'
#' @param df datafraem containing the JOB_TYPE Column
#'
#' @return
#' @export
#'
#' @examples
 calc_faculty_status <- function(df) {
   require(tidyverse)

   df_out <- df %>%
     mutate(IS_FACULTY = JOB_TYPE %in% c("Faculty TT/T", "Faculty NTT"))

   cat(paste0("IDENTIFIED Faculty jobs      (",
              sum(df_out$IS_FACULTY),
              " records)\n"))

   return(df_out)
 }

 calc_sabbatical_status <- function(df) {
   require(tidyverse)

   df_out <- df %>%
     mutate(IS_SABBATICAL = (JOB_TYPE == "Faculty TT/T" & (FTE == .75 | FTE == .667)))

   cat(paste0("IDENTIFIED Fac Sabbatical   (",
              sum(df_out$IS_SABBATICAL),
              " records)\n"))

   return(df_out)
 }

 calc_ayfy_status <- function(df) {
   require(tidyverse)

   df_out <- df %>%
     mutate(AY_FY = if_else(MONTHS == 26,
                            "FY",
                            "AY"))

   return(df_out)
 }
