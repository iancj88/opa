

#' Classify job labor distribution rows into aggregate categories
#'
#' @description Using the index and program codes to determine 'fund source' for
#'   a job. The categories defined by the following regular expressions are
#'   verified yearly wiht the business/finance office. last updated Fall 2019
#'
#' @param df the dataframe which contains index and program data
#' @param index_col_name the unquoted name of the index column
#' @param program_col_name the unquoted name of the program column
#' @param percent_fund_col_name the unquoted name of the column containing the
#'   percent of the total job funding associated with the index, program data.
#' @param consolidate_rows return a single row per the consolidation key
#' @param consolidation_key_col_name the unique identifier used to simplify the
#'   dataset if consolidate_rows is True
#' @param date_col_name the field which specifies the as of date for the nbrjlbd
#'   data entered from the dataframe. typically 'as_of_date' or simply 'date'
#' @return a dataframe containing new boolean columns indicating the fund source
#'   type.
#' @export
calc_agg_fund_type <- function(df,
                               index_col_name = FUNDING_INDX,
                               program_col_name = FUNDING_PROG,
                               percent_fund_col_name = FUNDING_PERCENT,
                               consolidate_rows = T,
                               consolidation_key_col_name = job_date_key,
                               date_col_name = as_of_date) {
  # set to UTC time zone to ensu,re compatibility with Banner dates
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  suppressPackageStartupMessages({
    require(dplyr)
    require(purrr)
  })

  prog_enquo <- enquo(program_col_name)
  indx_enquo <- enquo(index_col_name)
  percent_enquo <- enquo(percent_fund_col_name)
  key_enquo <- enquo(consolidation_key_col_name)
  date_enquo <- enquo(date_col_name)

  # INST: Sum(IIf([PROG_CODE]="01",[PERCENT],0))
  # RES: Sum(IIf([PROG_CODE] Like "02" Or [PROG_CODE] Like "P02D*" Or [PROG_CODE] Like "P02S*" Or [PROG_CODE] Like "P02T*" Or [PROG_CODE] Like "P02U*",[PERCENT],0))
  # AES: Sum(IIf([PROG_CODE] Like "46*" Or [PROG_CODE] Like "P*A*",[PERCENT],0))
  # ES: Sum(IIf([PROG_CODE] Like "47*" Or [PROG_CODE] Like "48*" Or [PROG_CODE] Like "P*E*",[PERCENT],0))
  # AUX: Sum(IIf([PROG_CODE]="10" Or [PROG_CODE] Like "P10",[PERCENT],0))
  # SUPP: Sum(IIf([PROG_CODE] In ("04","05","06","P04","P05","P05DF","P06","P06WF"),[PERCENT],0))
  # OTHER: Sum(IIf([PROG_CODE] In ("03","07","08","P03","P03DF","P03HS","P03S*","P03T*","P03Z*","P07","P08*"),[PERCENT],0))
  # Grand and Contracts: Index Like "424*" Or Like "425*" Or Like "426*" Or Like "427*" Or Like "428*" Or Like "429*" Or "4gcgap" Or "z3011" Or Like "4W*"


  state_fund_pattern <- "(^01[1-6])|(^40[1-4])|(^49[1-3])|(^11)|(^41)|(^4[ABCRS])|(^91)"
  gen_fund_pattern <- "^4([A-V]|[X-Z]|[0-1])"
  inst_pattern <- "^01$"
  idc_fund_pattern <- "^43[6-8]"
  research_pattern <- "(^P02[DSTUW])|(^02)"
  aes_pattern <- "(^46)|(^P.*A)"
  es_pattern <- "(^4[7-8])|(^P.*E)"
  aux_pattern <- "(^10$)|(^P10)"
  other_pattern <- "(^0[378])|(^P03$)|(^P03DF$)|(^P03HS$)|(^P03[STZ])|(^P07$)|(^P08$)"
  supp_pattern <- "(^0[456])|(^P0[456])|(^P05DF)|(^P06WF)"
  gandc_pattern <- "(^42[4-9])|(^4GCGAP$)|(^Z3011$)|(^4W)"

  #if is matches a pattern above, place the percent in the column name
  df_out <- mutate(df,
                   is_state_fund = if_else(grepl(state_fund_pattern, !!indx_enquo),!!percent_enquo, as.numeric(NA)),
                   is_gen_fund = if_else(grepl(gen_fund_pattern, !!indx_enquo), !!percent_enquo, as.numeric(NA)),
                   is_idc_fund = if_else(grepl(idc_fund_pattern, !!indx_enquo),!!percent_enquo, as.numeric(NA)),
                   is_inst_fund = if_else(grepl(inst_pattern, !!prog_enquo), !!percent_enquo, as.numeric(NA)),
                   is_res_fund = if_else(grepl(research_pattern, !!prog_enquo), !!percent_enquo, as.numeric(NA)),
                   is_aes_fund = if_else(grepl(aes_pattern, !!prog_enquo), !!percent_enquo, as.numeric(NA)),
                   is_es_fund = if_else(grepl(es_pattern, !!prog_enquo), !!percent_enquo, as.numeric(NA)),
                   is_aux_fund = if_else(grepl(aux_pattern, !!prog_enquo), !!percent_enquo, as.numeric(NA)),
                   is_supp_fund = if_else(grepl(supp_pattern, !!prog_enquo), !!percent_enquo, as.numeric(NA)),
                   is_other_fund = if_else(grepl(other_pattern, !!prog_enquo), !!percent_enquo, as.numeric(NA)),
                   is_gc_fund = if_else(grepl(gandc_pattern, !!indx_enquo), !!percent_enquo, as.numeric(NA)))


  if (consolidate_rows == T) {
    # See https://stackoverflow.com/questions/48449799/join-datasets-using-a-quosure-as-the-by-argument
    # for why it is necessary to set quosure outside the c() in the by parameter in the left join


      #left_join(df_out, by = by_custom) %>%
      #select(-starts_with("is_")) %>%
      #select(-starts_with("FUNDING_")) %>%
      #distinct()    by_custom <-  set_names(quo_name(key_enquo), quo_name(key_enquo))
    df_out <- group_by(df_out,
                       !!key_enquo,
                       !!date_enquo) %>%
      summarize(state_fund_perc = sum(is_state_fund, na.rm = T),
                gen_fund_perc = sum(is_gen_fund, na.rm = T),
                idc_fund_perc = sum(is_idc_fund, na.rm = T),
                inst_fund_perc = sum(is_inst_fund, na.rm = T),
                res_fund_perc = sum(is_res_fund, na.rm = T),
                aes_fund_perc = sum(is_aes_fund, na.rm = T),
                es_fund_perc = sum(is_es_fund, na.rm = T),
                aux_fund_perc = sum(is_aux_fund, na.rm = T),
                supp_fund_perc = sum(is_supp_fund, na.rm = T),
                other_fund_perc = sum(is_other_fund, na.rm = T),
                gc_fund_perc = sum(is_gc_fund, na.rm = T))
  }
  return(df_out)

}

#' Aggregate funds into fund-type groupings.
#'
#' @description Categories defined by Megan Lasso, Terry Leist. Alternative to the
#' `calc_agg_fund_type()` function. These groupings are appropriate for tableau
#' visualizations for T. Leist.
#'
#' @param df the dataframe containing nbrjlbd information
#' @param fund_col_name the unquoted column name containing the nbrjlbd fund
#'   codes
#' @param percent_col_name the unquoted column name containing the nbrjlbd
#'   percent column name
#' @param format one of "wide" or "long". Wide returns individual columns
#'   containing associated funding percents. Long adds and additional column
#'   "fund_type" that contains the character fund type description
#' @param consolidate_rows a boolean used in conjunction with the format =
#'   "wide" input parameter. T reduces the dataset to a single  row per job
#' @param consolidation_key_col_name The unquoted name of the column that will
#'   be used to aggregate the rsults if consolidate_rows = T
#'
#' @return
#' @export
#'
#' @examples
classify_personnel_fund_types <- function(df,
                                          fund_col_name = NBRJLBD_FUND_CODE,
                                          percent_col_name = NBRJLBD_PERCENT,
                                          format = "long",
                                          consolidate_rows = T,
                                          consolidation_key_col_name = job_date_key) {

  require(tidyverse)

  stopifnot(format %in% c("wide", "long"))
  # Current Unrestricted  Fund matches 41xxxx
  # MAES  Fund matches 9xxxxx
  # ES  Fund matches 0xxxxx (that is a zero)
  # FSTS  Fund matches 1xxxxx
  # Designated  Fund matches 43xxxx (exclude IDCs which are fund 436xxx-438xxx)
  # Designated IDC  Fund matches 436xxx-438xxx
  # Auxiliary Fund matches 44xxxx
  # Restricted Sponsored  Fund matches 4Wxxxx
  # Other Restricted  Fund matches 42xxxx

  curr_unrestricted_pattern <- "^41"
  maes_pattern <- "^9"
  es_pattern <- "^0"
  fsts_pattern <- "^1"
  designated_nonidc_pattern <- "^43[^6-8]"
  designated_idc_pattern <- "^43[6-8]"
  aux_fund_pattern <- "^44"
  restricted_sponsored_pattern <- "^4W"
  restricted_other_pattern <- "^42"

  fund_enquo <- enquo(fund_col_name)
  percent_enquo <- enquo(percent_col_name)
  key_enquo <- enquo(consolidation_key_col_name)
  if (format == "wide") {
    df_out <- mutate(df,
                     is_curr_unrestricted = if_else(grepl(curr_unrestricted_pattern, !!fund_enquo),
                                                    !!percent_enquo,
                                                    as.numeric(NA)),
                     is_maes = if_else(grepl(maes_pattern, !!fund_enquo),
                                       !!percent_enquo,
                                       as.numeric(NA)),
                     is_es = if_else(grepl(es_pattern,
                                           !!fund_enquo),
                                     !!percent_enquo,
                                     as.numeric(NA)),
                     is_fsts = if_else(grepl(fsts_pattern, !!fund_enquo),
                                       !!percent_enquo,
                                       as.numeric(NA)),
                     is_designated_nonidc = if_else(grepl(designated_nonidc_pattern, !!fund_enquo),
                                                    !!percent_enquo,
                                                    as.numeric(NA)),
                     is_designated_idc = if_else(grepl(designated_idc_pattern, !!fund_enquo),
                                                 !!percent_enquo,
                                                 as.numeric(NA)),
                     is_aux = if_else(grepl(aux_fund_pattern, !!fund_enquo),
                                      !!percent_enquo,
                                      as.numeric(NA)),
                     is_restricted_sponsored = if_else(grepl(restricted_sponsored_pattern, !!fund_enquo),
                                                       !!percent_enquo,
                                                       as.numeric(NA)),
                     is_restricted_other = if_else(grepl(restricted_other_pattern, !!fund_enquo),
                                                   !!percent_enquo,
                                                   as.numeric(NA)))
    if (consolidate_rows == T) {
      by_custom <-  set_names(quo_name(key_enquo), quo_name(key_enquo))
      df_out <- group_by(df_out,
                         !!key_enquo) %>%
        summarize(curr_unrestricted_fund = sum(is_curr_unrestricted, na.rm = T),
                  maes_fund = sum(is_maes, na.rm = T),
                  es_fund = sum(is_es, na.rm = T),
                  fsts_fund = sum(is_fsts, na.rm = T),
                  designated_nonidc_fund = sum(is_designated_nonidc, na.rm = T),
                  designated_idc_fund = sum(is_designated_idc, na.rm = T),
                  aux_fund = sum(is_aux, na.rm = T),
                  restriced_sponsored_fund = sum(is_restricted_sponsored, na.rm = T),
                  restricted_other_fund = sum(is_restricted_other, na.rm = T),
                  total_sum = sum(!!percent_enquo))
    }
  }

  if (format == "long") {
    df_out <- df %>%
      mutate(fund_type = case_when(grepl(curr_unrestricted_pattern, !!fund_enquo) ~ "Current Unrestricted",
                                   grepl(maes_pattern, !!fund_enquo) ~ "MAES",
                                   grepl(es_pattern, !!fund_enquo) ~ "ES",
                                   grepl(fsts_pattern, !!fund_enquo) ~ "FSTS",
                                   grepl(designated_nonidc_pattern, !!fund_enquo) ~ "Designated Non-IDC",
                                   grepl(designated_idc_pattern, !!fund_enquo) ~ "Designated IDC",
                                   grepl(aux_fund_pattern, !!fund_enquo) ~ "Auxiliaries",
                                   grepl(restricted_sponsored_pattern, !!fund_enquo) ~ "Restricted Sponsored",
                                   grepl(restricted_other_pattern, !!fund_enquo) ~ "Restricted Other"))
  }



  return(df_out)

}


#' Pull Job Labor Distribution Records for specific job-keys and an as-of date.
#'
#' @description pull NBRJLBD table records from banner for specified
#'   pidm-posn-suff job-keys, as-of a specific date. Any modifications to labor
#'   distribution which occur after this date are not included. Likewise a
#'   boolean input parameter controls whether all previous labor distribution
#'   records are included, or merely the most recent that occurred prior to the
#'   as-of date.
#'
#' @param job_keys_in pidm-posn-suff job-key vector
#' @param as_of_date the string or POSIXct date used to filter future-dated
#'   records
#' @param most_recent_only a boolean indicating whether to include all
#'   historical records previous to as-of date or merely the most recent prior
#'   to the as-of date
#' @param opt_bann_conn an optional banner connnection object
#'
#' @return a data frame containing the critical columsn to aggregate fund
#'   sources. These include, job-key, index, orgn, account and distribution
#'   percent.
#' @export
pull_job_labor_dist <- function(job_keys_in,
                            as_of_date,
                            most_recent_only = T,
                            max_fund_percent_only = T,
                            opt_bann_conn) {

  suppressPackageStartupMessages({
    require(magrittr)
    require(dplyr)
    require(tictoc)
  })

  # set to UTC time zone to ensure compatibility with Banner dates
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  # get a banner connection if not supplied. Used to pull PERAPPT data.
  if (missing(opt_bann_conn)) {
    bann_conn <- opa::get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }

 # convert to posixct or date to use in the banner query
   if (inherits(as_of_date, "character")) {
     as_of_date <- as.POSIXct(as_of_date)
     message("converting character as_of_date to POSIXct  - pull_raw_nbrjlbd")
   } else if (inherits(as_of_date, "Date")) {
     as_of_date <- as.POSIXct(as_of_date)
   } else if (inherits(as_of_date, "POSIXct")) {
     as_of_date <- as_of_date
   } else {
     stop("Invalid as_of_date supplied to job_lbr_dist_qry")
   }

  stopifnot(length(job_keys_in) <= 1000)

  sql_query <- opa::load_sql_qry(file_path = "X:/icj_dts/sql_files/",
                            "labor_dist_qry.sql")

  #insert job keys into query
  job_keys_in_frmtd <- paste0("'", job_keys_in, "'", collapse = ", ")
  sql_query <- gsub(pattern = "JOB_KEYS_HERE",
                    replacement = job_keys_in_frmtd,
                    x = sql_query)

  #insert as_of date
  sql_query <- gsub(pattern = "AS_OF_IN_PARAM",
                    replacement = as_of_date,
                    x = sql_query)

  #filter to most_recent if specified
    sql_query <- gsub(pattern = "MOST_RECENT_ONLY_PARAM",
                      replacement = as.character(most_recent_only),
                      x = sql_query)

    #filter to max_fund_percent_rows
    sql_query <- gsub(pattern = "MAX_FUND_PERCENT_PARAM",
                      replacement = as.character(max_fund_percent_only),
                      x = sql_query)
    tic("pull NBRJLBD records for jobs and date")
    output <- DBI::dbGetQuery(bann_conn, sql_query)

    if (max_fund_percent_only == TRUE) {
      output <- opa::filter_by_max_per_key(output,
                                           key_col_name = JOB_KEY,
                                           col_to_max_name = NBRJLBD_PERCENT)
      toc()
    }

  # This function has been explicitly moved to the to the sql WHERE clause rather than post-pull filtering
  #
  # if (most_recent_only == TRUE) {
  #   output <- opa::filter_by_max_date_per_key(output,
  #                                             key_col_name = JOB_KEY,
  #                                             date_col_name = NBRJLBD_EFFECTIVE_DATE)
  # }

  return(output)
}


#' Pull or process Job Labor Distriubtion as of a specific date, or most recent
#' data available.
#'
#' @description Pull or process job labor distribution for each unique job
#'   defined by pidm, posn, suff, get the fund, orgn, account, program and index
#'   information. If the job has split labor distribution, each split will use a
#'   different row with associated percent (0-100) values. Use input parameters
#'   to control which labor distribution are returned (filtered by job and/or
#'   date). Requires unaltered NBRJLBD column names
#'
#' @param opt_job_lbr_dist_df dataframe containing NBRJLBD recrods with
#'   unaltered column names. See \code{pull_job_lbr_dist}.
#' @param opt_as_of_date a posixCT date used to specify the specific date on
#'   which the labor distribution shoudl be used. Defaults to Sys.Date()
#' @param most_recent_only a boolean determining if the function will return
#'   only the most recent labor distribution data, or all labor distribution
#'   data
#' @param majority_percent_only a boolean value
#' @param opt_bann_conn an optional banner database connection object
#'
#' @return a dataframe containing each job's fund, organization code, account
#'   code, index, program and percent
#' @author Ian C Johnson
#' @seealso get_job_lbr_dist job_lbr_dist_qry
#' @export
process_nbrjlbd_data <- function(opt_job_lbr_dist_df,
                                 opt_as_of_date = NA,
                                 opt_job_keys,
                                 most_recent_only = T,
                                 majority_percent_only = F,
                                 opt_bann_conn) {

  # set to UTC time zone to ensure compatibility with Banner dates
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  suppressPackageStartupMessages({
    require(tictoc)
    require(magrittr)
    require(dplyr)
  })

  # check for missing input parametes ---------------------------------------

  if (missing(opt_bann_conn)) {
    bann_conn <- opa::get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }

  if (!missing(opt_as_of_date)) {
    as_of_date <- opt_as_of_date
  } else {as_of_date = NA}

  # Pull NBRJLBD data if not supplied ---------------------------------------
  tic_msg <- paste0("Processing Labor Dist as of ", as_of_date)
  tic(tic_msg)
  if (missing(opt_job_lbr_dist_df)) {

    job_lbr_dist <- tbl(bann_conn, NBRJLBD) %>%
      collect() %>%
      mutate(job_key = paste0(NBRJLBD_PIDM, NBRJLBD_POSN, NBRJLBD_SUFF)) %>%
      filter(NBRJLBD_CHANGE_IND != "H")

    if (!missing(opt_job_keys)) {
      job_lbr_dist <- filter(job_lbr_dist,
                             job_key %in% opt_job_keys)
    }

  } else {job_lbr_dist <- opt_job_lbr_dist_df}


  # filter by date ----------------------------------------------------------


  # filter to the relevant labor distribution if given an as-of date. Any rows
  # with an effective date later than the as-of date will be removed.
  if (!is.na(as_of_date)) {
    job_lbr_dist$as_of_date <- as_of_date

    # remove all records after specified as of date, and return max remaining
    # values
    job_lbr_dist <- filter(job_lbr_dist,
                           NBRJLBD_EFFECTIVE_DATE <= as_of_date)

    job_lbr_dist <- filter_by_max_per_key(job_lbr_dist,
                                          key_col_name = job_key,
                                          col_to_max_name = NBRJLBD_EFFECTIVE_DATE)
  }

  # filter nbrjlbd by most recent -------------------------------------------
  # most_recent_only == TRUE will remove those records that do not have the most
  # recent effective date for each individual job
  if (most_recent_only == TRUE) {
    job_lbr_dist <-  filter_by_max_per_key(job_lbr_dist,
                                           key_col_name = job_key,
                                           col_to_max_name = NBRJLBD_EFFECTIVE_DATE)
  }

  # filter by majority percent funding --------------------------------------

  if (majority_percent_only == TRUE) {
    job_lbr_dist <- opa::filter_by_max_per_key(job_lbr_dist,
                                               key_col_name = job_key,
                                               col_to_max_name = NBRJLBD_PERCENT)
  }

  toc()

  return(job_lbr_dist)
}


#' job_lbr_dist_qry
#'
#' internal function in the opa package. queries Banner given a set of pidm,
#' posn, suffix job keys and returns labor distribution data from the NBRJLBD
#' table.
#'
#' @param pidm_vec a vector containing pidms to be used in the job key
#' @param posn_veca a vector containing position numbers to be used in the job
#'   key
#' @param suff_vec a vector containing suffixes  to be used in the job key
#' @param as_of_date a POXIXct date specifying the effective date used for the
#'   labor distribution
#' @param most_recent_only a boolean determining if only the most recently
#'   effective dated records should be returned
#' @param majority_percent_only a boolean value determining if only the majority
#'   funding rows should be returned for each job's effective date
#' @param bann_conn the banner connection object derived from
#'   `opa::get_banner_conn()`
#'
#' @export
#'
#' @return a dataframe containing pidm, posn, suff, job_key, account, program,
#'   index, fund, organization number
job_lbr_dist_qry <- function(job_key_vec,
                             bann_conn,
                             opt_as_of_date) {

  suppressPackageStartupMessages({
    require(magrittr)
    require(dplyr)
  })

  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  #convert to posixct or date to use in the banner query
  if (!missing(opt_as_of_date)) {
    #check that date is formatted properly
    if (class(opt_as_of_date) == "character") {
      as_of_date <- as.POSIXct(opt_as_of_date)
      message("converting character as_of_date to POSIXct  - job_lbr_dist_qry")
    } else if (inherits(opt_as_of_date, "Date")) {
      as_of_date <- opt_as_of_date
    } else {
      stop("Invalid as_of_date supplied to job_lbr_dist_qry")
    }
  }

  output <- dplyr::tbl(bann_conn, "NBRJLBD") %>%
  output <- dplyr::select(output,
                          pidm = NBRJLBD_PIDM,
                          posn = NBRJLBD_POSN,
                          suff = NBRJLBD_SUFF,
                          eff_date = NBRJLBD_EFFECTIVE_DATE,
                          fund = NBRJLBD_FUND_CODE,
                          orgn = NBRJLBD_ORGN_CODE,
                          acct = NBRJLBD_ACCT_CODE,
                          program = NBRJLBD_PROG_CODE,
                          index = NBRJLBD_ACCI_CODE,
                          percent = NBRJLBD_PERCENT)

  output <- dplyr::mutate(output, pidm_posn = paste0(pidm, posn))
  output <- dplyr::mutate(output, job_key = paste0(pidm_posn, suff))
  output <- dplyr::filter(output, job_key %in% job_key_in)
  output <- dplyr::collect(output)


  #remove time stamps from effective date

  output$eff_date <- as.POSIXlt(output$eff_date)
  lubridate::hour(output$eff_date) <- 0
  lubridate::minute(output$eff_date) <- 0
  lubridate::second(output$eff_date) <- 0
  output$eff_date <- as.POSIXct(output$eff_date)


  return(output)
}


#' Pull Account Codes and associated Account Name
#'
#' Make a lookup dataframe to associate account codes and account names as of a
#' particular date. Pulls from the FTVACCT Banner table
#'
#' @param as_of_date the date which will be used to filter the table. Any
#'   records with effective dates after the as_of_date will be removed
#' @param opt_bann_conn a banner connection object used to
#'
#' @return A table containing account codes and account names as-of the
#'   specified input parameter date. There will be a single row per unique
#'   account codes
#' @export
get_account_lu <- function(as_of_date, opt_bann_conn) {
  # get a banner connection if not supplied. Used to pull PERAPPT data.
  if (missing(opt_bann_conn)) {
    bann_conn <- opa::get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }

  if(inherits(as_of_date, "character")) {
    as_of_posixct <- as.POSIXct(as_of_date)
  } else if(inherits(as_of_date, "POSIXct")) {
    as_of_posixct <- as_of_date
  } else {
    stop("as_of_date must be a character or POSIXct type")
  }

  require(dplyr)

  acct_data <- tbl(bann_conn,
                   "FTVACCT") %>%
    collect()

  acct_data <- filter(acct_data,
                      FTVACCT_EFF_DATE <= as_of_date)

  acct_data <- opa::filter_by_max_per_key(acct_data,
                                          key_col_name = FTVACCT_ACCT_CODE,
                                          col_to_max_name = FTVACCT_EFF_DATE)

  acct_data <- select(acct_data,
                      acct_code = FTVACCT_ACCT_CODE,
                      acct_desc = FTVACCT_TITLE,
                      acct_type = FTVACCT_ATYP_CODE)

  return(acct_data)
}