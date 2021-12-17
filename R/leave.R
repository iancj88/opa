#' #' Title
#' #'
#' #' @param opt_pidm_vec
#' #' @param opt_bann_conn
#' #' @param opt_start_date
#' #' @param opt_end_date
#' #' @param sick_or_annual
#' #'
#' #' @return
#' #' @export
#' pull_leave_accrual_data <- function(opt_pidm_vec, opt_bann_conn, opt_start_date, opt_end_date, sick_or_annual = "SICK") {
#'
#'   require(magrittr)
#'   require(tidyverse)
#'
#'   # default set to UTC when pulling dates from banner. This sets the environment
#'   # so that the DBI package properly handles, (doesn't handle for that matter!)
#'   # date conversions.
#'   Sys.setenv(TZ = "UTC")
#'   Sys.setenv(ORA_SDTZ = "UTC")
#'
#'   # get a banner connection if not supplied. Used to pull PERAPPT data.
#'   if (missing(opt_bann_conn)) {
#'     bann_conn <- opa::get_banner_conn()
#'   } else {
#'     bann_conn <- opt_bann_conn
#'   }
#'
#'   PHRACCR_conn <- tbl(bann_conn, "PHRACCR")
#'
#'   PHRACCR_conn <- select(PHRACCR_conn,
#'                          pidm = PHRACCR_PIDM,
#'                          year = PHRACCR_YEAR,
#'                          pr = PHRACCR_PAYNO,
#'                          leave_code = PHRACCR_LEAV_CODE,
#'                          accrued_hours = PHRACCR_CURR_ACCR,
#'                          used_hours = PHRACCR_CURR_TAKEN,
#'                          pict_code = PHRACCR_PICT_CODE)
#'
#'   if(!missing(opt_pidm_vec)) {
#'     stopifnot(length(opt_pidm_vec) <= 1000)
#'
#'     PHRACCR_conn <- filter(PHRACCR_conn,
#'                            pidm %in% pidm_vec),
#'                            pict_code == "4M")
#'   }
#'
#'
#'
#'   PHRACCR_conn <- filter(PHRACCR_conn,
#'                          leave_code == sick_or_annual)
#'   PHRACCR_out <- collect(PHRACCR_conn)
#'
#'
#'   PHRACCR_out <- mutate(PHRACCR_out,
#'                         date = convert_date_payroll(date = NA, year = year, payno = pr, simplify = TRUE)) %>%
#'     select(-pict_code)
#'
#'   #filter to opt_start and opt_end dates
#'   if (!missing(opt_start_date)) {
#'     #check that date is formatted properly
#'     if (class(opt_start_date) == "character") {
#'       start_date <- as.POSIXct(opt_start_date)
#'       message("converting character opt_start_date to POSIXct")
#'     } else if ("POSIXct" %in% class(opt_start_date)) {
#'       start_date <- opt_start_date
#'     } else {
#'       stop("Invalid opt_start_date supplied to get_leave_records")
#'     }
#'   }
#'
#'   if (!missing(opt_end_date)) {
#'     #check that date is formatted properly
#'     if (class(opt_end_date) == "character") {
#'       end_date <- as.POSIXct(opt_end_date)
#'       message("converting character opt_end_date to POSIXct")
#'     } else if ("POSIXct" %in% class(opt_end_date)) {
#'       end_date <- opt_end_date
#'     } else {
#'       stop("Invalid opt_end_date supplied to get_leave_records")
#'     }
#'   }
#'
#'   if (!missing(opt_start_date)) {
#'     PHRACCR_out <- filter(PHRACCR_out, date >= lubridate::floor_date(start_date, unit = "months"))
#'   }
#'   if (!missing(opt_end_date)) {
#'     PHRACCR_out <- filter(PHRACCR_out, date <= lubridate::floor_date(end_date, unit = "months"))
#'   }
#'
#'
#'   PHRACCR_out$fy <- compute_fiscal_year(PHRACCR_out$date)
#'
#'   return(PHRACCR_out)
#'
#'
#'
#' }
