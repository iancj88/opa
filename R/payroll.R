#' Estimate the number of workdays in a given date range.
#'
#' @description Assumes that workdays is the standard Monday-Friday work week excluding
#' holidays. See ?msuopa::msu_holidays for more information about the holidays
#' used in computation.
#'
#' @param date_start the start date as a character or date
#' @param date_end the end date as a character or date
#' @param holidays  a vector containing msu designated holidays as Date objects.
#'
#' @return a single integer quantifying the number of workdays between
#'   date_start and date_end, excluding holidays and weekends.
#' @export
#' @author Ian C Johynson
compute_n_workdays <- function(date_start, date_end, holidays = msuopa::msu_holidays) {
  # default set to UTC when pulling dates from banner. This sets the environment
  # so that the DBI package properly handles date conversions.
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  #by default, this uses the holiday dates set in the msuopa::msu_holidays
  #dataset. Ensure that this package dataset is updated for the date range you
  #are examining.
  holidays <- as.Date(holidays)

  # the length of this sequence represents the total number of work and non-work days
  date_seq <- seq.Date(from = as.Date(date_start),
                       to = as.Date(date_end),
                       by = "days")

  # the total number of holidays within this date sequence. Traditional
  # employees do not work these days, so they are removed from the total
  # workdays count
  n_holidays_in_seq <- sum(date_seq %in% holidays)

  #likewise, traditional employees don't work the weekends
  n_weekdays_in_seq <- sum(!weekdays(date_seq, abbreviate = F) %in% c("Saturday", "Sunday"))

  # subract holidays from the number of weekdays for total workdays.
  n_days_total <- n_weekdays_in_seq - n_holidays_in_seq
  return(n_days_total)
}

#' Return single FY's payroll data
#'
#' @description Payroll data can be difficult to compile due to misalignment of
#'   the calendar year (from which payno is defined) and the fiscal year, and
#'   differing payroll calendars between campuses. This function returns all
#'   payroll records for a given fiscal year and campus. An additional field,
#'   `pr_percent_in_fy` is included to indicate the percent of days in the
#'   payroll that fell into the requested FY in cases where the payroll spans
#'   multiple fiscal years.
#'
#' @param fy the fiscal year for which to return ptrcaln payno and year
#' @param campus_pict the campus pict code to filter by campus. Must be one of
#'   "ALL", "4M", "3B", "6B", "7M".
#' @param opt_ptrcaln_data an optional unmodified PTRCALN dataset. supply to
#'   avoid pulling data from Banner.
#' @param opt_bann_conn an optional banner connection object. Only needed if
#'   opt_ptrcaln_data not supplied
#'
#' @return a dataframe containing pict code and fy inputs, and corresponding
#'   PTRCALN_YEAR and PTRCALN_PAYNO. A percent of total indicates the percent of
#'   the payroll (# payroll working days in payroll in fy / # payroll working
#'   days in payroll)
#' @export
#' @author Ian C Johnson
return_fy_payrolls <- function(fy,
                               campus_pict = "ALL",
                               simplify = TRUE,
                               opt_ptrcaln_data,
                               opt_bann_conn) {

  suppressPackageStartupMessages({
    require(magrittr)
    require(dplyr)
    require(chron)
  })

  # default set to UTC when pulling dates from banner. This sets the environment
  # so that the DBI package properly handles, (doesn't handle for that matter!)
  # date conversions.
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  #verify campus_pict is an acceptable value
  stopifnot(campus_pict %in% c("ALL", "4M", "3B", "4B", "7M"))
  if (campus_pict == "ALL") {
    campus_pict <- c("4M",
                     "3B",
                     "6B",
                     "7M")
  }

  #pull ptrcaln data if necessary
  if (missing(opt_ptrcaln_data)) {
    # get a banner connection if not supplied. Used to pull PERAPPT data.
    if (missing(opt_bann_conn)) {
      bann_conn <- msuopa::get_banner_conn()
    } else {
      bann_conn <- opt_bann_conn
    }

    ptrcaln_data <- tbl(bann_conn, "PTRCALN") %>% collect()
  } else {
    ptrcaln_data <- opt_ptrcaln_data
  }

  #get first and last day of the fy
  fy_start <- paste0(fy - 1, "-07-01") %>% as.Date()
  fy_end <-  paste0(fy, "-06-30") %>% as.Date()

  #filter by campus pict code
  ptrcaln_data <- filter(ptrcaln_data,
                         PTRCALN_PICT_CODE %in% campus_pict)

  #reduce to only needed variables
  ptrcaln_data <- select(ptrcaln_data,
                         "PTRCALN_YEAR",
                         "PTRCALN_PICT_CODE",
                         "PTRCALN_PAYNO",
                         "PTRCALN_START_DATE",
                         "PTRCALN_END_DATE") %>%

    mutate(PTRCALN_START_DATE = as.Date(PTRCALN_START_DATE),
           PTRCALN_END_DATE = as.Date(PTRCALN_END_DATE),
           pr_start_date_fy = msuopa::compute_fiscal_year(PTRCALN_START_DATE),
           pr_end_date_fy = msuopa::compute_fiscal_year(PTRCALN_END_DATE),
           fy = fy,
           pr_key = paste0(PTRCALN_PICT_CODE,
                           PTRCALN_YEAR,
                           PTRCALN_PAYNO)) %>%
    filter(fy == pr_start_date_fy | fy == pr_end_date_fy) %>%
    distinct()


  #this would be way easier if more of the date functions were vectorized. See
  #the commented out dplyr code below to compare the necessary verbosity.

  #loop through each row of ptrcaln comparing FY start/end dates with span of
  #each row's payroll.
  suppressWarnings( {
    for (row in 1:nrow(ptrcaln_data)) {

      ptrcaln_data$start_end_date_seq[row] <- seq.Date(from = ptrcaln_data$PTRCALN_START_DATE[row],
                                                       to = ptrcaln_data$PTRCALN_END_DATE[row],
                                                       by = "days") %>% list()

      ptrcaln_data$n_days_in_pr[row] <- length(unlist(ptrcaln_data$start_end_date_seq[row]))
      ptrcaln_data$n_biz_days[row] <- compute_n_workdays(date_start = ptrcaln_data$PTRCALN_START_DATE[row],
                                                         date_end = ptrcaln_data$PTRCALN_END_DATE[row])

      ptrcaln_data$has_fy_start[row] <- fy_start %in% as.Date(unlist(ptrcaln_data$start_end_date_seq[row]), origin = lubridate::origin)
      ptrcaln_data$has_fy_end[row] <- fy_end %in% as.Date(unlist(ptrcaln_data$start_end_date_seq[row]), origin = lubridate::origin)

      # if the pr contains neither fy end or fy start, then 100 percent of
      # earnings apply to the given fiscal year.
      #
      # if it contains the fy end date in the span of the payroll, then the
      # percent of earnings is the ratio of work days in the specific payroll in
      # the fiscal year divided by total work days in the specific payroll.
      #
      # if it contains the fy start date in the span of the payroll, compute the same ratio
      ptrcaln_data$pr_percent_in_fy[row] <- ifelse(!ptrcaln_data$has_fy_end[row] & !ptrcaln_data$has_fy_start[row],
                                                   1,                           #so all the earnings count towards that particular fy (ratio = 1)
                                                   ifelse(ptrcaln_data$has_fy_end[row], #has a fiscal year end so get ratio of days in beginning of payroll
                                                          compute_n_workdays(ptrcaln_data$PTRCALN_START_DATE[row], fy_end) / ptrcaln_data$n_biz_days[row],
                                                          compute_n_workdays(fy_start, ptrcaln_data$PTRCALN_END_DATE[row]) / ptrcaln_data$n_biz_days[row]))
    }
  })

  #this is helpful when the data will be directly on payroll earnings data.
  if (simplify == TRUE) {
    ptrcaln_data <- ptrcaln_data %>%
      select(pr_key,
             pr_percent_in_fy)
  }

  return(ptrcaln_data)
}

#' Return Payroll numbers and dates covering a particular time interval and
#' optionally, campus.
#'
#' @description Pulls from PTRCALN
#'
#' @param start_date the start date of the time range to pull payroll numbers.
#'   Will include the payroll containing the date specified.
#' @param end_date the end date of the time range to pull payroll numbers. Will
#'   include the payroll containing the date specified.
#' @param opt_campus_code an optional campus code. One of 'BZ', 'BL', 'HV', 'GF'
#' @param opt_bann_conn an optional banner connection object supplied by
#'   `opa::get_banner_conn()`
#'
#' @return a dataframe containing payroll years, numbers, start and end dates,
#'   and a date interval object for each payroll
#' @export
pull_paynos <- function(start_date, end_date, opt_campus_code, opt_bann_conn) {

  require(tidyverse)
  require(magrittr)
  require(lubridate)

  # get a banner connection if not supplied. Used to pull PERAPPT data.
  if (missing(opt_bann_conn)) {
    bann_conn <- opa::get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }

  if(class(start_date) == "character" | inherits(start_date, "Date")) {start_date <- as.POSIXct(start_date)}
  if(class(end_date) == "character" | inherits(end_date, "Date")) {end_date <- as.POSIXct(end_date)}

  # set to UTC time zone to ensure compatibility with Banner dates
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  ptrcaln <- tbl(bann_conn, "PTRCALN") %>%
    collect()

  if(!missing(opt_campus_code)) {
    campus_pict <- case_when(opt_campus_code == "BZ" ~ "4M",
                             opt_campus_code == "BL" ~ "6B",
                             opt_campus_code == "HV" ~ "7M",
                             opt_campus_code == "GF" ~ "3B",
                             TRUE ~ "Error, invalid campus code")
    if(campus_pict == "Error, invalid campus code") {
      stop("opt_campus code must be one of 'BZ', 'BL', 'HV', 'GF'")
    }
    ptrcaln <- filter(ptrcaln, PTRCALN_PICT_CODE == campus_pict)
  }

  ptrcaln <- mutate(ptrcaln,
                    payroll_date_interval = interval(start = PTRCALN_START_DATE,
                                                     end = PTRCALN_END_DATE),
                    contains_input_start_date = start_date %within% payroll_date_interval,
                    contains_input_end_date = end_date %within% payroll_date_interval)

  # if the start or end date falls in the middle of a payperiod, be sure to pull the records that contain those dates
  ptrcaln <- filter(ptrcaln,
                    (PTRCALN_START_DATE >= start_date & PTRCALN_END_DATE <= end_date) |
                      contains_input_start_date | contains_input_end_date)

  ptrcaln <- select(ptrcaln,
                    year = PTRCALN_YEAR,
                    pict = PTRCALN_PICT_CODE,
                    payno = PTRCALN_PAYNO,
                    payno_start_date = PTRCALN_START_DATE,
                    payno_end_date = PTRCALN_END_DATE,
                    payroll_date_interval)

  return(ptrcaln)
}



#' Determine approximate month length of a contract given number of payrolls
#' over which it is paid.
#'
#' Depreciated as of bi-weekly payroll conversion. Use `pull_paynos` to  link
#' payroll number/factors and dates/time-spans
#'
#' @description this is only approximate because bi-weekly payrolls do not align
#'   with the calendar year. Pay factors are a static value found in the
#'   NBRJOBS_FACTORS Banner field.
#'
#' @param pay_factors the numeric payfactors from the NBRJOBS_FACTORS field.
#'   This is explicitley the default number of payrolls over which the total job
#'   compensation is paid.
#' @param campus_code
#'
#' @return the month duration of a contract depending on the number of payrolls
#'   in which it was active in a given fy, and the campus on which the job
#'   resides
pay_factor_month_conversion <-  function(pay_factors, campus_code) {

  stopifnot()
  # Bozeman and Havre are both on Monthly payrolls. the number of factors
  # reflects matches the 'month-length' of the contract.
  if (campus_code %in% c("BZ", "HV")) {
    months <-  pay_factors
  } else {
    # Billings and Great falls are on bi-weekly (every 2 weeks regardless of
    # month) and bi-monthly (twice a month regardless of weeks).
    # the months is APPROXIMATELY equal to number of payrolls divided by two.
    months <- pay_factor / 2
    months <- ifelse(months == .5, 1, floor(months))
  }

  return(months)
}


#' Pull historic payroll data by job key, campus, year, and payno.
#'
#' @description Pull all PHREARN records given a particular `pr_key` comprised
#'   of the concatenation of `Campus`, `Year`, and `Payno` OR given a particular
#'   set of `job_keys` comprised of `pidm`, `posn`, `suffix`. Must contain at
#'   least one of job_key or pr_key or both.
#'
#' @param job_keys the job keys whose payroll records will be returned
#' @param pr_keys the payroll keys (campus, year, payno) whose records will be
#'   returned
#' @param remove_leave_no_pay_rows a boolean flag that removes LNO and LNP earn
#'   code rows from teh dataset. These rows cause inflated payroll amount sums
#'   due to being populated with the pay amount if active but not actually being
#'   paid. Default TRUE.
#' @param opt_start_year an optional fiscal year to filter to only view records
#'   in and after the give year
#' @param opt_start_pr  an optional payroll number to filter to only view
#'   records in the or in a greater than payroll number. Not adapted to handle
#'   change from monthly to bi-weekly payroll
#' @param opt_end_year an optional fiscal year to filter to only view records in
#'   and before the give year
#' @param opt_end_pr an optional payroll number to filter to only view
#'   records in the or in a less than than payroll number. Not adapted to handle
#'   change from monthly to bi-weekly payroll
#' @param opt_bann_conn an optional banner connection object
#'
#' @return the phrearn rows specified by job key and/or payroll key
#' @export
pull_pr_data <- function(job_keys,
                         pr_keys,
                         remove_leave_no_pay_rows = T,
                         opt_bann_conn,
                         opt_start_year,
                         opt_start_pr,
                         opt_end_year,
                         opt_end_pr) {

  #TODO: Allow for optional start/end date parameters rather than optional
  #payroll number parameters to better handle the transition from monthly to
  #bi-weekly payroll cycles
  suppressPackageStartupMessages({
    require(tidyr)
    require(tictoc)
    require(dplyr)
  })

  stopifnot(!missing(job_keys) | !missing(pr_keys))

  tic("pull PHREARN data")

  # get a banner connection if not supplied. Used to pull PERAPPT data.
  if (missing(opt_bann_conn)) {
    bann_conn <- msuopa::get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }

  # default set to UTC when pulling dates from banner. This sets the environment
  # so that the DBI package properly handles, (doesn't handle for that matter!)
  # date conversions.
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  qry <- tbl(bann_conn, "PHREARN")
  qry <- mutate(qry,
                job_key1 = paste0(PHREARN_PIDM,
                                  PHREARN_POSN),
                job_key2 = paste0(job_key1,
                                  PHREARN_SUFF),
                pr_key1 = paste0(PHREARN_PICT_CODE,
                                 PHREARN_YEAR),
                pr_key2 = paste0(pr_key1,
                                 PHREARN_PAYNO))

  if (remove_leave_no_pay_rows == TRUE) {
    qry <- filter(qry,
                  !PHREARN_EARN_CODE %in% c("LNO", "LNP"))
  }

  if (!missing(job_keys)) {
    qry <- filter(qry, job_key2 %in% job_keys)
  }

  if (!missing(pr_keys)) {
    qry <- filter(qry, pr_key2 %in% pr_keys)
  }

  if (!missing(opt_start_year)) {
    qry <- filter(qry, PHREARN_YEAR >= opt_start_year)
  }
  if (!missing(opt_end_year)) {
    qry <- filter(qry, PHREARN_YEAR <= opt_end_year)
  }

  data_out <- collect(qry)

  #remove unneeded columns
  data_out <- select(data_out,
                     -job_key1,
                     -pr_key1) %>%
    rename()
  toc()
  return(data_out)
}


#' Determine earn codes used for a set of jobs and given date
#'
#' Determine the earn codes and associated earn code descriptions for a set of
#' jobs on a specific date.  This is particularly useful for adcomp jobs whose
#' 'reason' is stored in the earn code description field.
#'
#' @param job_key a vector of job keys, defined as the GID or PIDM concatenated
#'   with Position Number and Suffix. Used as unique table keys for most Banner
#'   Payroll Tables.
#' @param ee_id_type one of "PIDM" or "GID" used to specify the employee id used
#'   to create the job_key.
#' @param post_date the as-of date. any payroll data processed after this date
#'   will be ignored.
#' @param opt_bann_conn an optional banner connection object. If none is
#'   provided, the function will prompt for Banner login details and create a
#'   one-time use Banner connection.
#'
#' @return a dataframe containing PIDM, pidm-posn-suff job key, earn code and
#'   descriptions for the most recent payroll covering the as_of date.
#' @export
add_earn_codes <- function(job_key,
                           ee_id_type = "PIDM",
                           post_date,
                           opt_bann_conn) {

  suppressPackageStartupMessages({
    require(magrittr)
    require(dplyr)
  })
  # make a connection to banner if none is supplied. Requires valid Banner
  # username and password.
  if (missing(opt_bann_conn)) {
    bann_con <- opa::get_banner_conn()
  } else {
    bann_con <- opt_bann_conn
  }

  # If the job keys were contructed using GIDs, convert them to PIDMs. PIDMs are
  # the standard person identifier in banner and is used in nbrearn
  if (ee_id_type == "GID") {
    job_key <- convert_job_keys_id_type(job_key_in,
                                        from = "GID",
                                        to = "PIDM")
  }

  # get all nbrearnings rows for the pre/post adcomp jobs
  nbrearn <- tbl(bann_con, "NBREARN") %>%
    select("PIDM" = NBREARN_PIDM,
           "POSN" = NBREARN_POSN,
           "SUFF" = NBREARN_SUFF,
           "eff_date" = NBREARN_EFFECTIVE_DATE,
           "earn_code" = NBREARN_EARN_CODE) %>%
    collect() %>%
    distinct(.keep_all = TRUE) %>%
    mutate(jobkey = paste0(PIDM, POSN, SUFF)) %>%
    filter(jobkey %in% job_key,
           eff_date <= post_date) #only perform calcs on jobs that we are interested in

  #ensure only the most recent nbrearn row for each job
  nbrearn_max_dates <- nbrearn %>%
    group_by(jobkey) %>%
    summarize(max_date = max(eff_date)) %>%
    mutate(date_job_key = paste0(jobkey, max_date)) #key used to filter the entire list of nbrearn codes

  nbrearn_most_recent <- nbrearn %>%
    mutate(date_job_key = paste0(jobkey, eff_date)) %>%
    filter(date_job_key %in% nbrearn_max_dates$date_job_key)

  # get the earn code descriptions to link to the earn codes found on nbrearn
  ptrearn <- tbl(bann_con, "PTREARN") %>%
    collect() %>%
    select(PTREARN_CODE, PTREARN_LONG_DESC, PTREARN_SHORT_DESC)

  # theoretically, there can be more than one earn code for a given job.... need
  # to find a way to concatenate the earn code descriptions together if that
  # occurs? maybe not?

  nbrearn_out <- nbrearn_most_recent %>%
    left_join(ptrearn,
              by = c("earn_code" = "PTREARN_CODE")) %>%
    select(PIDM, job_key, earn_code,
           "code_desc_long" = PTREARN_LONG_DESC,
           "code_desc" = PTREARN_SHORT_DESC)

  return(nbrearn_out)
}