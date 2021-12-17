#' Pull Gender data
#'
#' @description given a vector of pidms, pull the associated vector of genders
#'   from the SPBPERS Banner table.
#'
#' @param pidm_vec a vector of pidms not to exceed 1000 entries
#' @param opt_bann_conn an optional banner connection. one will be created if
#'   not supplied
#'
#' @return a dataframe containing the pidms and aassociated genders
#' @export
get_gender <- function(pidm_vec,
                       opt_bann_conn) {

  # set to UTC time zone to ensure compatibility with Banner dates
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  # get a one-time use banner connection if one is not supplied as an input
  # parameter
  if (missing(opt_bann_conn)) {
    bnr_conn <- msuopa::get_banner_conn()
  } else {
    bnr_conn <- opt_bann_conn
  }

  # max length of 1000 items in roracle query.
  # requires use of msuopa::split_vec_for_sql() function
  if (length(pidm_vec) > 1000) {
    stop("pidm_vec exceeds 1000 items.\nUse split_vec_for_sql function prior to calling get_gender function")
  }

  gender_out <- dplyr::tbl(bnr_conn, "SPBPERS")
  gender_out <- dplyr::select(gender_out,
                              PIDM = SPBPERS_PIDM,
                              Sex = SPBPERS_SEX)
  gender_out <- dplyr::filter(gender_out,
                              PIDM %in% pidm_vec)
  gender_out <- dplyr::collect(gender_out)

  return(gender_out)
}

#' Pull Race/Ethnicity Data
#'
#' @description Pull ipeds race data from bob's ipeds access db. The
#' path is optional incase the access database location changes.
#'
#' @param race_file_path the access filepath and name containing bob's IPEDS race/ethnicity data
#' @param re_tbl_name the access tbl_name from which to pull data
#' @param race_tbl_name the access table name from which race descriptions will be pulled
#' @param opt_pidm_filter an optional vector of pidms to filter the returned dataset
#'
#' @return the full ipeds race table as a dataframe keyed by PIDM
#' @export
get_race_data <- function(race_file_path = "X:/IRCommon/RACE/IPEDS_Race_2.accdb",
                          re_tbl_name = "RE20200922",
                          race_tbl_name = "Race20200922",
                          opt_pidm_filter) {
  # set to UTC time zone to ensure compatibility with Banner dates
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  suppressPackageStartupMessages({
    require(odbc)
    require(dplyr)
    require(tictoc)
    require(magrittr)
  })

  tic("Pull Race/Ethnicity table")

  race_conn <- opa::get_access_conn(race_file_path)

  if(!missing(opt_pidm_filter)) {
    re_tbl <- tbl(race_conn, re_tbl_name) %>%
      filter(SPBPERS_PIDM %in% opt_pidm_filter) %>%
      collect()

    race_tbl <- tbl(race_conn, race_tbl_name) %>%
      filter(GORPRAC_PIDM %in% opt_pidm_filter) %>%
      collect() %>%
      filter(substr(GORPRAC_RACE_CDE, 1, 1) == "5",
             GORPRAC_RACE_CDE  != "5") %>%
      select(PIDM = GORPRAC_PIDM,
             TRIBE = GORRACE_DESC) %>%
      group_by(PIDM) %>%
      summarize(TRIBE = paste(unique(TRIBE), sep = ", "))
  } else {
    re_tbl <- tbl(race_conn, re_tbl_name) %>%
      collect()
    race_tbl <- tbl(race_conn, race_tbl_name) %>%
      collect() %>%
      filter(substr(GORPRAC_RACE_CDE, 1, 1) == "5",
             GORPRAC_RACE_CDE  != "5") %>%
      select(PIDM = GORPRAC_PIDM,
             TRIBE = GORRACE_DESC) %>%
      group_by(PIDM) %>%
      summarize(TRIBE = paste(unique(TRIBE), sep = ", "))
  }

  re_out <- left_join(re_tbl,
                      race_tbl,
                      by = c("SPBPERS_PIDM" = "PIDM"))

  toc()

  odbc::dbDisconnect(race_conn)
  return(re_out)
}
