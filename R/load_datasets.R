#' Pull the Employee snapshot for a particular as-of date
#'
#' @description The employee snapshot is a general dataset containing employee
#'   and job information.
#'
#' @param date an 'as-of' date primarily used to filter job-specific data.
#' @param include_leave an optional boolean value indicating whether or not to
#'   include LWOP and LWOP/WB i.e. with Benefits Jobs in the snapshot.
#' @param banner_org_vec an optional parameter allowing for filtering to only
#'   return jobs that have majority, or a portion, of job funding from the given
#'   vector of Org #s. Defaults to all Org #s.
#' @param max_fund_only boolean value determining if all labor distribution
#'   splits will be returned, or only those with the majority funding in each
#'   job. Defaults to TRUE.
#' @param opt_bann_conn an active banner connection object typically derived
#'   from `get_banner_conn`. If not provided will create a temp connection
#' @param remove_eclses a character vector containing eclasses which will be not
#'   be returned by sql query.
#' @param return_input_params a boolean value indicating whether an addtional
#'   column containing the input parameters should be included in the ouput
#'   dataframe.
#' @param opt_campus_filter a character vector containing one of more Campus
#'   codes to filter sql statement. Leave as NA to not filter by campus.
#' @param opt_rank_df a dataframe containing all Rank records supplied by
#'   `opa::get_rank_records()`. If not supplied, will pull data within this
#'   function. Useful for looping scripts that run this function multiple times
#'   (particularly for multiple dates, etc).
#' @param opt_tenure_df a dataframe containing all Tenure records supplied by
#'   `opa::get_tenure_status()`. If not supplied, will pull data within this
#'   function. Useful for looping scripts that run this function multiple times
#'   (particularly for multiple dates, etc).
#' @param join_org_hier a boolean indicating if the org_hierarchy should be
#'   joined based on the person's HOME_DEPT_CODE
#'
#' @return a list of snapshot query returns. The names of the list items specify
#'   the date on which query is set.
#' @export
get_banner_snapshot <- function(date,
                                include_leave = FALSE,
                                banner_org_vec = "All",
                                max_fund_only = TRUE,
                                opt_bann_conn,
                                remove_eclses,
                                return_input_params = TRUE,
                                opt_campus_filter = "BZ",
                                opt_rank_df,
                                opt_tenure_df,
                                join_org_hier = F) {

  suppressPackageStartupMessages({
    require(ROracle)
    require(dplyr)
    require(tictoc)
  })

# error check input parameters --------------------------------------------
  if (missing(date)) {stop("date input parameter MUST be supplied as of

                           revisions on 2019-07-10")
  }

  # get a banner connection if not supplied
  if (missing(opt_bann_conn)) {
    bann_conn <- get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }

  # NA used so that there is no filtering in teh SQL where clause
  if (banner_org_vec == "All") {banner_org_vec <- NA}

  #ensure dates/times are properly pulled from Banner. Default MST/MDT time zone
  #forces a timezone conversion with inaccurate results.
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  # this used to be stored within the package, but there were issues modifying
  # the sql script. Investigate how to cleanly export sql stored in package.
  #snapshot_sql_fname <- "X:/icj_dts/sql_files/snapshot_updated.sql"
  snapshot_sql_fname <- "//helene/opa$/icj_dts/sql_files/snapshot_updated.sql"
  if (!file.exists(snapshot_sql_fname)) {
    stop_msg <- paste0("Snapshot SQL query not found at ", snapshot_sql_fname)
    stop(stop_msg)
  }

  sql_query <-  msuopa::load_sql_qry(file_path = "//helene/opa$/icj_dts/sql_files/",
                                     file_name = "snapshot_updated.sql")

  #create a formatted list of the input parameters
  if (return_input_params == TRUE) {
    input_params <- list()
    input_params$date <- date
    if (is.na(banner_org_vec)) {
      input_params$banner_org_vec <- "All Orgs"
    } else {
      input_params$banner_org_vec <- banner_org_vec
    }
    if (missing(remove_eclses)) {
      input_params$remove_job_eclses <- "None"
    } else {
      input_params$remove_job_eclses <- remove_eclses
    }
    input_params$sql_file_name <- snapshot_sql_fname
    input_params$max_fund_only <- max_fund_only
  } else {input_params <- list()}
  input_params_formatted <- paste(names(input_params),
                                 input_params,
                                 sep = ":  ",
                                 collapse = ",   ")


  # Modify SQL query --------------------------------------------------------

  # insert the appropriate job statuses. must be properly quoted i.e. 'A' or
  #   'A','L','B', 'F'
  if (include_leave == TRUE) {
    acceptable_job_status <- c("'A', 'L', 'B', 'F'")
  } else {acceptable_job_status <- c("'A'")}

  sql_query <- gsub(pattern = "JOB_STATUS_IN_PARAM",
                    replacement = acceptable_job_status,
                    x = sql_query)

  #filter by campus code, 'BZ'. This was hardcoded prior to 7/10/2019
  if (opt_campus_filter == "ALL") {
    campus_code_formatted <- "'BZ', 'HV', 'GF', 'BL'"
  } else {
    campus_code_formatted <- paste0( "'", opt_campus_filter, "'")
  }

  sql_query <- gsub(pattern = "CAMPUS_CODE_IN_PARAM",
                    replacement = campus_code_formatted,
                    x = sql_query)

  #filter by eclass
  if (!missing(remove_eclses)) {
    bad_eclses_formatted <- paste0("'", remove_eclses, "'", collapse = ", ")
    sql_query <- gsub(pattern = "BAD_ECLSES_HERE",
                      replacement = bad_eclses_formatted,
                      x = sql_query)
  } else {
    sql_query <- gsub(pattern = "AND nbrjobs_ecls_code not in",
                      replacement = "--AND nbrjobs_ecls_code not in",
                      x = sql_query)

    sql_query <- gsub(pattern = "AND b.nbrjobs_ecls_code not in",
                      replacement = "--AND b.nbrjobs_ecls_code not in",
                      x = sql_query)
  }

  # set to filter to max percent or not
  max_fund_only <- as.character(max_fund_only)
  sql_query <- gsub(pattern = "MAX_FUNDING_PERCENT_PARAM_IN",
                    replacement = max_fund_only,
                    x = sql_query)

  # insert a banner_org_vec if provided
  # This is depcreiated. The sql script no longer filters by org code.
  if ( !is.na(banner_org_vec)) {

    stopifnot(length(banner_org_vec) <= 1000)
    if (is.numeric(banner_org_vec)) {
      banner_org_vec <- as.character(banner_org_vec)
    }

    banner_org_vec <- paste0("'", banner_org_vec, "'", collapse = ", ")
    sql_query <- gsub(pattern = "FORMATED_ORG_LIST_HERE",
                      replacement = banner_org_vec,
                      sql_query)

  }

  # insert as-of date into sql query
  if (inherits(date, "character")) {
    date <- as.POSIXct(date)
  } else if (!inherits(date, "POSIXct")) {
    stop("input date must be coercible to POSIXct in get_banner_snapshot")
  }

  #date_str <- as.character(date, format = "%Y%m%d")
  sql_query <- gsub(pattern = "AS_OF_IN_PARAM",
                    replacement = date,
                    x = sql_query)

  # Pull Data from Banner ---------------------------------------------------
  tic_msg <- paste0("Pulled ", date, " snapshot")
  tic(tic_msg)
  snapshot <- ROracle::dbGetQuery(statement = sql_query,
                                  conn = bann_conn)
  snapshot$date <- date

  if (return_input_params == TRUE) {
    snapshot$input_params <- input_params_formatted
  }

  toc()
  # Build Keys to join Rank/Tenure records ----------------------------------
  snapshot <- mutate(snapshot,
                     NAME = paste0(NAME_LAST,", ", NAME_FIRST),
                     JOB_KEY = paste0(PIDM, POSN, SUFF),
                     pidm_date_key = paste0(PIDM,
                                            date),
                     job_date_key = paste0(PIDM,
                                           POSN,
                                           SUFF,
                                           date))

  snapshot <- opa::trim_ws_from_df(snapshot)


  # Join Rank/Tenure --------------------------------------------------------
  if (missing(opt_rank_df)) { # if no data supplied, pull it from perrank banner table
    rank_data <- opa::get_rank_records(opt_as_of_date = date,
                                       opt_bann_conn = bann_conn)
  } else {
    rank_data <- opa::get_rank_records(opt_as_of_date = date,
                                       opt_bann_conn = bann_conn,
                                       opt_rank_records = opt_rank_df)
  }

  if (missing(opt_tenure_df)) { # if no data supplied, pull it from perappt banner table
    tenure_data <- opa::get_tenure_status(opt_as_of_date = date,
                                          opt_bann_conn = bann_conn)
  } else {
    tenure_data <- opa::get_tenure_status(opt_as_of_date = date,
                                          opt_bann_conn = bann_conn,
                                          opt_tenure_records = opt_tenure_df)
  }


  rank_data <- select(rank_data,
                      PIDM = pidm,
                      RANK_CODE = rank_code,
                      RANK_CODE_DESC = rank_code_desc,
                      RANK_CODE_AGG = rank_code_agg,
                      RANK_EFF_DATE = rank_begin_date)

  tenure_data <- tenure_data %>%
    select(PIDM = pidm,
           TENURE_CODE = tenure_code,
           TENURE_EFF_DATE = tenure_eff_date)

  snapshot <- snapshot %>%
    left_join(rank_data, by = "PIDM") %>%
    left_join(tenure_data, by = "PIDM")

  if (join_org_hier == T)  {
    org_hier <- opa::build_org_hierarchy_lu(as_of_date = date, opt_bann_conn = bann_conn)
    org_hier <- select(org_hier,
                       seed,
                       HOME_DEPT_DESC = seed_desc,
                       HOME_DEPT_DIVISION_L2 = Org2_desc,
                       HOME_DEPT_COLLEGE_L3 = Org3_desc,
                       HOME_DEPT_DEPT_L4 = Org4_desc,
                       HOME_DEPT_SUBDEPT_L5 = Org5_desc,
                       HOME_DEPT_SUBDEPT_L6 = Org6_desc)
    snapshot <- left_join(snapshot,
                          org_hier,
                          by = c("HOME_DEPT_CODE" = "seed"))
    snapshot <- relocate(snapshot,
                         starts_with("HOME_DEPT"),
                         .after = HOME_DEPT_CODE)
  }

  return(snapshot)
}

#' Pull historical cleaned OPA employee snapshots
#'
#' @description Historically, the snapsht has been taken once a year in
#'   mid-October. These datasets are reviewed for accuracy and have driven much
#'   of the head count, job count, and FTE reporting. The primary snapshot table
#'   uses a name int he form of 'Employees19F' where 19 indicates the calendar
#'   year of the snapshot and F indicates the Fall Semester. Due to the working
#'   file nature of these documents, care should be taken to ensure that only
#'   the proper tables are loaded
#'
#' @param opt_snapshot_fpath the filepath to an access database containing the
#'   most recent employee snapshot table.
#' @param opt_tables_to_load, an optional vector containing names of the tables
#'   to load. If no vector is supplied, all primary Employee snapshots from 2010
#'   to current will be loaded
#' @param opt_bann_conn an optional banner connection object.
#'
#' @return a dataframe containing all appended reviewed-snapshots.
#' @export
#' @author Ian C Johnson
get_opa_access_snapshots <- function(opt_snapshot_fpath = "X:/Employees/EmployeesFY21.accdb",
                                     opt_tables_to_load,
                                     opt_bann_conn) {
  suppressPackageStartupMessages({
    require(magrittr)
    require(dplyr)
    require(tictoc)
  })

  # Initialize function -----------------------------------------------------

  # default set to UTC when pulling dates from banner. This sets the environment
  # so that the DBI package properly handles, date conversions.
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  # get a banner connection if not supplied. Used to pull PERAPPT data.
  if (missing(opt_bann_conn)) {
    bann_conn <- msuopa::get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }


  tic("load OPA snapshot data from Access")

  # Pull data from Access Tables --------------------------------------------

  db_conn <- opa::get_access_conn(db_file_path = opt_snapshot_fpath)

  tbl_names <- opa::get_db_table_names(db_conn)
  if(missing(opt_tables_to_load)) {
    tbl_names <- tbl_names[grepl("Employees[0-9]{2}F", tbl_names)]
    tbl_names <- tbl_names[!tbl_names %in% c("Employees19F_TEMP",
                                             "Employees19F_Temp_20190816",
                                             "Employees19F_Temp_20190930")]
  } else {
    tbl_names <- tbl_names[tbl_names %in% opt_tables_to_load]
  }

  if(length(tbl_names) == 0) {
    stop(paste0("No Tables meeting names supplied in opt_tables_to_load found in ", opt_snapshot_fpath))
  }

  snapshot_data <- lapply(tbl_names, function(x) {tbl(db_conn, x) %>% collect()})
  names(snapshot_data) <- tbl_names

  toc()
  # drop unneeded columns ---------------------------------------------------
  # these are columns not systematically produced in every snapshot. Due to the
  # inconsistent application/definition, they are dropped from the final dataframe
  tic("drop and add columns to snapshots")
  snapshot_data <- lapply(snapshot_data,
                          drop_unneeded_snapshot_cols)

  snapshot_out <- bind_rows(snapshot_data, .id = "tbl_names")

  # assign proper column data types -----------------------------------------
  snapshot_out <- mutate(snapshot_out,
                         PIDM = as.integer(PIDM),
                         BIRTH_DATE = as.POSIXct(BIRTH_DATE),
                         FIRST_HIRE_DATE = as.POSIXct(FIRST_HIRE_DATE),
                         CURRENT_HIRE_DATE = as.POSIXct(CURRENT_HIRE_DATE),
                         JOB_START_DATE = as.POSIXct(JOB_START_DATE),
                         JOB_END_DATE = as.POSIXct(JOB_END_DATE),
                         EFF_DATE = as.POSIXct(EFF_DATE),
                         HOURLY_RATE = as.numeric(HOURLY_RATE),
                         MONTHLY_RATE = as.numeric(MONTHLY_RATE),
                         SALARY = as.numeric(SALARY),
                         FTE = as.numeric(FTE),
                         MONTHLY_HRS = as.numeric(MONTHLY_HRS),
                         MONTHS = as.numeric(MONTHS)) %>%
    rename(rank_code_agg = RANK_AGG,
           rank_code = RANK_CODE,
           tenure_code = TENURE)


  # add job type, race, ethn column -----------------------------------------
  snapshot_out <- msuopa::classify_job(snapshot_out,
                                       ecls_col_name = ECLS,
                                       opt_bann_conn = bann_conn)
  ethn_race <- msuopa::get_race_data(pidm_vec = unique(snapshot_out$PIDM),optional_return_all_cols = T)

  snapshot_out <- left_join(snapshot_out, ethn_race, by = c("PIDM" = "PIDM"))


  # assign faculty type -----------------------------------------------------

  #this should really be wrapped into opa::classify_job function...
  snapshot_out <- mutate(snapshot_out,
                         is_faculty = (ESKL == "20"),
                         job_type = if_else(rank_code %in% c("1", "2", "3") &
                                              is_faculty == T,
                                            "Faculty TT/T",
                                            job_type),
                         job_type = if_else(!rank_code %in% c("1", "2", "3") &
                                              is_faculty == T,
                                            "Faculty NTT",
                                            job_type),
                         job_type = if_else(!rank_code %in% c("1", "2", "3") &
                                              is_faculty == F,
                                            "Faculty NTT",
                                            job_type))
  toc()

  # return data -------------------------------------------------------------

  return(snapshot_out)
}



#' Drop inconsistently applied columns from the historical opa snapshots
#'
#' @description these are columns not systematically produced in every snapshot.
#'   Due to the inconsistent application/definition, they are dropped from the
#'   final dataframe. Failure to remove causes row bind issues.
#'
#' @param df the historical opa_snapshot dataframe that may or may not contain
#'   the column.
#'
#' @return the dataframe minus the inconsistent columns
#'
#' @author Ian C Johnson
drop_unneeded_snapshot_cols <- function(df) {
  suppressPackageStartupMessages({
    require(stringr)
    require(dplyr)
    require(magrittr)
  })
  # drop unneeded columns ---------------------------------------------------
  # these are columns not systematically produced in every snapshot. Due to the
  # inconsistent application/definition, they are dropped from the final dataframe
  df_out <- df
  names(df_out) <- stringr::str_to_upper(names(df))
  df_out <- dplyr::mutate_all(df_out,
                              as.character)

  df_out <- df_out %>%
    drop_col(AGE) %>%
    drop_col(ASIAN) %>%
    drop_col(BLACK) %>%
    drop_col(Dept_Name) %>%
    drop_col(CUPA_ADMIN) %>%
    drop_col(CUPA_MID) %>%
    drop_col(HISP) %>%
    drop_col(WHITE) %>%
    drop_col(OTHER) %>%
    drop_col(NATIVEHI) %>%
    drop_col(IPEDSRACE) %>%
    drop_col(NORESP) %>%
    drop_col(TOTALSAL) %>%
    drop_col(OTHERETHN) %>%
    drop_col(POSTRAISESAL) %>%
    drop_col(SPRINGSALARY) %>%
    drop_col(SPRINGMONTHS) %>%
    drop_col(SPRINGFTE) %>%
    drop_col(OTHERRACE) %>%
    drop_col(AGE) %>%
    drop_col(COLLEGE) %>%
    drop_col(OTHER_ETH) %>%
    drop_col(TOTAL) %>%
    drop_col(TOTALPOSTRAISESAL) %>%
    drop_col(IPEDSAYFYTERRY) %>%
    drop_col(IPEDSFACTERRY) %>%
    drop_col(TERRYDEGREE) %>%
    drop_col(IPEDSRANK) %>%
    drop_col(IPEDSTENURE) %>%
    drop_col(WORLDRANKINGS) %>%
    drop_col(FT_PT) %>%
    drop_col(MARKET) %>%
    drop_col(CATEGORY) %>%
    drop_col(`IPEDS9/1011/12`) %>%
    drop_col(CIP2TERRY) %>%
    drop_col(DIVISION) %>%
    drop_col(IPEDSGENDER) %>%
    drop_col(OTHERRE) %>%
    drop_col(NATIONALITY) %>%
    drop_col(INDIAN) %>%
    drop_col(`FT/PT`) %>%
    drop_col(IPEDSFTPT) %>%
    drop_col(JCAT) %>%
    drop_col(JCAT_DESC) %>%
    drop_col(JCAT_CUPA) %>%
    drop_col(JCAT_SOC) %>%
    drop_col(IPEDS2DIGSOCCODE) %>%
    drop_col(CUPA_STAFF) %>%
    drop_col(CUPA_PROF) %>%
    drop_col(CUPA_NEW)

  # return df without columns -----------------------------------------------
  return(df_out)
}

#' Pull PERRANK and PTRRANK tables from Banner.
#'
#' these tables contain the necessary information to determine an individual's
#' rank status on any given date.
#'
#' @param bann_conn a banner connection object supplied by the ROracle package
#'   or `opa::get_banner_conn()` function.
#'
#' @return joined PERRANK, PTRRANK tables with renamed columns
pull_rank_tables <- function(bann_conn) {

  # get a banner connection if not supplied. Used to pull PERAPPT data.
  if (missing(bann_conn)) {
    bann_conn <- msuopa::get_banner_conn()
  }

  suppressPackageStartupMessages({
    require(dplyr)
    require(tictoc)
  })

  #these are small tables with less than 10,000 rows. There's no need to filter
  #prior to extraction.
  tic("Pulled PERRANK and PTRRANK Tables from Banner")

  perrank_qry <- tbl(bann_conn, "PERRANK")
  perrank_data <- collect(perrank_qry)
  ptrrank_qry <- tbl(bann_conn, "PTRRANK")
  ptrrank_data <- collect(ptrrank_qry)

  toc()

  rm(perrank_qry, ptrrank_qry)

  # Join PERRANK, PTRRANK into single df ----------------------------------
  #combine the rank desc, rank aggregated, and rank aggregated desc
  ptrrank_desc_join <- select(ptrrank_data,
                              rank_code = PTRRANK_CODE,
                              rank_code_agg = PTRRANK_RANK_CODE,
                              rank_code_desc = PTRRANK_DESC)

  perrank_output <- select(perrank_data,
                           pidm = PERRANK_PIDM,
                           rank_code = PERRANK_RANK_CODE,
                           rank_begin_date = PERRANK_BEGIN_DATE)

  perrank_output <- left_join(perrank_output,
                              ptrrank_desc_join,
                              by = "rank_code")

  ptrrank_desc_agg_join <- select(ptrrank_data,
                                  rank_code = PTRRANK_CODE,
                                  rank_code_agg_desc = PTRRANK_DESC)

  perrank_output <- left_join(perrank_output,
                              ptrrank_desc_agg_join,
                              by = c("rank_code_agg" = "rank_code"))

  return(perrank_output)
}

#' Return Rank and Aggergated Rank records per person as-of a particular date.
#'
#' These values are used to differentiate NTT and TT/T Faculty. They are pulled
#' from the PERRANK and PTRRANK Banner tables.
#'
#'
#' @param return_most_recent a boolean filter specifying if only the most recent
#'   records relative to the as_of_date should be returned. If no as_of_date
#'   parameter supplied, will default to the most recent to the Sys.Date()
#' @param opt_as_of_date use to filter to only those rank records applicable to
#'   a certain date. will convert ISO formated character strings to POSIXct. If
#'   missing, will default to the current date.
#' @param opt_bann_conn an optional banner connection to pull data from PERAPPT
#'   banner table
#' @param opt_rank_records an optional dataframe consisting of joined ptrrank
#'   and perrank data. WARNING: No error checking is done on this input!
#'
#' @return a data frome consisting of pidm, rank, rank desc, aggregated rank,
#'   agg rank desc, begin date, and begin date month floor. Only most recent
#'   rank record (less than as_of_date) returned per per person
#' @seealso get_tenure_status
#' @export
#' @author Ian C Johnson
get_rank_records <- function(return_most_recent = TRUE,
                             opt_as_of_date,
                             opt_bann_conn,
                             opt_rank_records) {
  suppressPackageStartupMessages({
    require(dplyr)
  })

  # Initialize function, input parameters -----------------------------------
  if (missing(opt_bann_conn)) {
    bann_conn <- msuopa::get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }

  # ensure as_of_date is correctly formatted, if it was supplied
  if (!missing(opt_as_of_date)) {
    #make sure that the data type is correct for filtering rank records
    if (inherits(opt_as_of_date, c("character", "Date"))) {
      as_of_date <- as.POSIXct(opt_as_of_date)
      message("converting as_of_date to POSIXct (get_rank_records)")
    } else if (inherits(opt_as_of_date, "POSIXct")) {
      as_of_date <- opt_as_of_date
    } else {
      stop("Invalid as_of_date supplied to get_rank_records")
    }
  }

  # default set to UTC when pulling dates from banner. This sets the environment
  # so that the DBI package properly handles, date conversions.
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  if (missing(opt_rank_records)) {
    perrank_output <- pull_rank_tables(bann_conn)
  } else {
    perrank_output <- opt_rank_records
  }

  # Filter by as-of-date ----------------------------------------------------

  if (!missing(opt_as_of_date)) {
    # get rid of future dated rows relative to the input 'as-of date'
    perrank_output <- filter(perrank_output,
                             rank_begin_date <= as_of_date) %>%
      mutate(as_of_date = as_of_date)

    # only keep the most recent row for each person for non-future dated rows
    # max_dates <- group_by(perrank_output, pidm)
    # max_dates <- summarize(max_dates, max_date = max(rank_begin_date))
    # max_dates <- mutate(max_dates, join_key = paste0(pidm, max_date))
    #
    # perrank_output <- mutate(perrank_output,
    #                          as_of_date = as_of_date,
    #                          pidm_date_key = paste0(pidm, as_of_date),
    #                          pidm_join_key = paste0(pidm, rank_begin_date))
    #
    # perrank_output <- filter(perrank_output,
    #                          pidm_join_key %in% max_dates$join_key)
    #
    # perrank_output <- select(perrank_output,
    #                          -pidm_join_key)

  }

  # Truncate the begin date to round to the floor of the month. i.e. set every
  # begin date to the first of it's month

  #only give the most recent record in the dataset. It's possible to use this
  #along with the as-of-date filter
  if (return_most_recent == TRUE) {
    #function stored in utility.R file
    perrank_output <- opa::filter_by_max_per_key(perrank_output,
                                                 key_col_name = pidm,
                                                 col_to_max_name = rank_begin_date)
  }

  # Create common time-series date for data-joins --------------------------

  perrank_output$date_mnth_floor <- perrank_output$rank_begin_date
  lubridate::day(perrank_output$date_mnth_floor) <- 1

  # return data -------------------------------------------------------------
  if (!"pidm_date_key" %in% names(perrank_output)) {

  }
  return(perrank_output)
}

#' Pull entire PERAPPT table from Banner.
#'
#' PERAPPT used to store tenure/appointment specific data. Necessary for
#' determining tenure status for a given employee and date.
#'
#' @param bann_conn a Banner connection object typically derived from
#'   `opa::get_banner_conn()` or the ROracle package
#'
#' @return an unmodified dataframe containing all unmodified records from PERAPPT
#' @export
pull_tenure_table <- function(bann_conn) {
  # get a banner connection if not supplied. Used to pull PERAPPT data.
  if (missing(bann_conn)) {
    bann_conn <- msuopa::get_banner_conn()
  }

  suppressPackageStartupMessages({
    require(dplyr)
    require(tictoc)
  })
  tic("Pull PERAPPT table from Banner")

  perappt_qry <- tbl(bann_conn, "PERAPPT")
  perappt_data <- collect(perappt_qry)

  toc()

  return(perappt_data)
}

#' Get a dataframe containing tenure status records.
#'
#' @description The tenure status dataframe can be filtered to * all records *
#'   only the most recent record for each individual employee * only the most
#'   recent record as of a specific date * all records prior to a specific as-of
#'   date
#'
#' @param return_most_recent a boolean which specifies whether all tenure status
#'   records should be returned as of a specific date, or if only the most
#'   recent record prior or equal to as-of-date. Defaults to TRUE
#' @param opt_as_of_date an optional as-of date to filter the tenure status
#'   records. if supplied, will discard any records occuring after this date.
#'   Uses PERAPPT_APPT_EFF_DATE to determine cutoff.
#' @param opt_bann_conn an optional banner connection object to pull data from
#'   PERAPPT
#' @param opt_tenure_records tenure record table data which may be included as
#'   an input here to avoid repulling the data from banner. Useful when using in
#'   a loop.
#' @param opt_rename_columns choose whether to rename output columns to
#'   shorthand or use the default banner table column names.
#'
#' @return tenure status data from PERAPPT table specific to a given as-of date.
#' @export
#' @author Ian C Johnson
get_tenure_status <- function(return_most_recent = TRUE,
                              opt_as_of_date,
                              opt_bann_conn,
                              opt_tenure_records,
                              opt_rename_columns = TRUE) {
  # default set to UTC when pulling dates from banner. This sets the environment
  # so that the DBI package properly handles, (doesn't handle for that matter!)
  # date conversions.
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  # Initialize function, input parameters. -----------------------------------
  if (missing(opt_bann_conn)) {
    bann_conn <- get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }



  # Pull PERAPPT data -------------------------------------------------------
  if (missing(opt_tenure_records)) {
    perappt_qry <- tbl(bann_conn, "PERAPPT")
    perappt_data <- collect(perappt_qry)
  } else {
    # the dataset can be loaded as in input parameter dataframe rather than
    # pulling from Banner. This is helpful to minimize the number of direct
    # database queries.
    perappt_data <- opt_tenure_records

    # the function renames the columns assuming the input dataframe of perappt
    # data was created by this function.
    perappt_data <-  rename(perappt_data,
                            "PERAPPT_PIDM" = pidm,
                            "PERAPPT_APPT_EFF_DATE" = tenure_eff_date,
                            "date_mnth_floor" = perappt_eff_flr,
                            "PERAPPT_TENURE_CODE" = tenure_code)
    #TODO convert the column names to custom input parameters to allow for
    #custom column naming of input and output dataframe columns. This will
    #enable the function to be more generalizable to PERAPPT data from other
    #sources
  }

  perrappt_output <- perappt_data

  # Filter by as-of-date ----------------------------------------------------
  if (!missing(opt_as_of_date)) {
    #check that date is formatted properly
    if (inherits(opt_as_of_date,"character")) {
      as_of_date <- as.POSIXct(opt_as_of_date)
      message("converting character as_of_date to POSIXct (get_tenure_status)")
    } else if (inherits(opt_as_of_date, "POSIXct")) {
      as_of_date <- opt_as_of_date
    } else {
      stop("Invalid as_of_date supplied to get_tenure_status")
    }


    perappt_data <- filter(perappt_data,
                           PERAPPT_APPT_EFF_DATE <= as_of_date)

    max_dates <- group_by(perappt_data, PERAPPT_PIDM)
    max_dates <- summarize(max_dates, max_date = max(PERAPPT_APPT_EFF_DATE))
    max_dates <- mutate(max_dates, join_key = paste0(PERAPPT_PIDM, max_date))

    perrappt_output <- mutate(perappt_data,
                              as_of_date = as_of_date,
                              pidm_date_key = paste0(PERAPPT_PIDM, as_of_date),
                              pidm_join_key = paste0(PERAPPT_PIDM, PERAPPT_APPT_EFF_DATE))

    perrappt_output <- filter(perrappt_output,
                              pidm_join_key %in% max_dates$join_key)
    perrappt_output <- select(perrappt_output,
                              -pidm_join_key)
  }

  # Filter to most recent data ----------------------------------------------
  if (return_most_recent == TRUE) {
    perrappt_output <- opa::filter_by_max_per_key(perrappt_output,
                                                       key_col_name = PERAPPT_PIDM,
                                                       col_to_max_name = PERAPPT_APPT_EFF_DATE)
  } else {
    perrappt_output <- perrappt_output
  }

  # Create time-series join varialbe ----------------------------------------
  perrappt_output$date_mnth_floor <- perrappt_output$PERAPPT_APPT_EFF_DATE
  lubridate::day(perrappt_output$date_mnth_floor) <- 1

  perrappt_output <- mutate(perrappt_output,
                            pidm_date_flr_key = paste0(PERAPPT_PIDM,
                                                       date_mnth_floor))

  # Select output columns ---------------------------------------------------
  if (missing(opt_as_of_date)) {
    perrappt_output <- select(perrappt_output,
                              pidm = PERAPPT_PIDM,
                              tenure_eff_date = PERAPPT_APPT_EFF_DATE,
                              perappt_eff_flr = date_mnth_floor,
                              pidm_date_flr_key,
                              tenure_code = PERAPPT_TENURE_CODE)
  } else {
    perrappt_output <- select(perrappt_output,
                              pidm = PERAPPT_PIDM,
                              tenure_eff_date = PERAPPT_APPT_EFF_DATE,
                              perappt_eff_flr = date_mnth_floor,
                              tenure_code = PERAPPT_TENURE_CODE)
    perrappt_output$as_of_date <- as_of_date
  }


  # return data -------------------------------------------------------------
  return(perrappt_output)
}

#' Get a dataframe containing one or more all employees reports
#'
#' @description Load one or more All Employee reports from csv source. Optionally, save the
#' compiled RDS file for faster loading in the future.
#'
#' @param folderpath the folderpath containing the csv files.
#' @param most_recent_only Boolean. Simplest way to specify loading the most
#'   recent csv file only for performance reasons.
#' @param opt_start_date An optional start date if only certain files should be
#'   loaded. If not specified and \code{most_recent_only == FALSE}, then will
#'   load all files found in folderpath.
#' @param opt_end_date An optional end date if only certain files should be
#'   loaded. If not specified and \code{most_recent_only == FALSE}, then will
#'   load all files found in folderpath.
#' @param supplement Boolean value indicating if the dataframes should have
#'   additional derived columns added. Examples of columns include EMR Job Type,
#'   Fiscal Year, and Longevity Bonus.
#' @param save_output A logical parameter to determine if a RDS file should be
#'   saved to a user selected directory. Useful for large-data pulls that will
#'   have recurrent needs.
#'
#' @return a single dataframe containing one or more all employees reports.
#'   report data can be distinguished by the added 'date' column.
#' @author Ian C Johnson
#' @export
get_all_ee_report <- function(folderpath,
                              most_recent_only = TRUE,
                              opt_start_date,
                              opt_end_date,
                              supplement = TRUE,
                              save_output = FALSE) {

# Initialize all ee pull script -------------------------------------------

  # default set to UTC when pulling dates from banner. This sets the environment
  # so that the DBI package properly handles date conversions.
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  # check that the input filepath leads to a valid directory
  if (missing(folderpath)) {
    folderpath <- "X:/Employees/All EEs Reports/rds_src/"
  }

  stopifnot(dir.exists(folderpath))

  # check that the directory is not empty
  stopifnot(length(list.files(path = folderpath)) > 0)


# convert csv files to RDS ------------------------------------------------

  # check that the directory contains a .txt file for parsing
  csv_fpath <- "X:/Employees/All EEs Reports/csv_src/"
  csv_names_only <- list.files(csv_fpath, full.names = FALSE)
  if (sum(grepl(".txt", csv_names_only)) > 0) {
    convert_allee_txt_rds(folder_path = csv_fpath)
    print(paste0("Converted ", length(csv_names_only), " CSV files to RDS"))
  }


# load RDS files ----------------------------------------------------------

  folderpath <- "X:/Employees/All EEs Reports/rds_src/"
  stopifnot(dir.exists(folderpath))
  file_names_only <- list.files(folderpath, full.names = FALSE)
  # get the full paths to the files in the directory so that each can be
  # fed into the rds reader one a time.
  file_names_paths <- list.files(folderpath, full.names = TRUE)

  # only keep those files that are .rds
  file_names_paths <- file_names_paths[grepl(".rds",
                                             file_names_only)]
  file_names_only <- file_names_only[grepl(".rds",
                                           file_names_only)]

  # if only the most recent all ee file is to be loaded (for performance
  # considerations), select the final file in the list. This will correspond
  # to the most 'recent' file because of the file name containing the date.
  if (most_recent_only == TRUE) {
    last_file_indx <- length(file_names_only)
    file_names_paths <- file_names_paths[last_file_indx]
    file_names_only <- file_names_only[last_file_indx]
    # otherwise filter the text file list to include those that fall
    # in the desired date range
  } else if ((most_recent_only == FALSE) &
             (!missing(opt_start_date)) &
             (!missing(opt_end_date))) {
    file_dates <- allee_dates_from_fnames(file_names_only)
    file_names_paths <- file_names_paths[((file_dates >= opt_start_date) &
                                            (file_dates <= opt_end_date))]

    file_names_only <- file_names_only[((file_dates >= opt_start_date) &
                                          (file_dates <= opt_end_date))]
  }

  # use data table fread (because it is fast) to load the csvs
  # deprecated use of csvs now stored as RDS
  t1 <- Sys.time()
  loaded_data <- lapply(file_names_paths, readRDS)
  t2 <- Sys.time()
  t_diff <- t2 - t1

  print(paste0("Loaded ",
               length(file_names_paths),
               " RDS files in ",
               as.double(t_diff),
               " ",
               units(t_diff)))


  # ensure dates are correctly formated prior to binding all dataframes together
  t1 <- Sys.time()
  loaded_data <- lapply(loaded_data, format_allEE_dates)
  t2 <- Sys.time()
  t_diff <- t2 - t1
  print(paste0("Formatted allEE Dates in ",
               as.double(t_diff),
               " ",
               units(t_diff)))

  #This is now done when converting the data file from csv to rds
  #
  # t1 <- Sys.time()
  # loaded_data <- lapply(loaded_data, supplement_all_ee)
  # t2 <- Sys.time()
  # t_diff <- t2 - t1
  # print(paste0("Supplemented datasets in ",
  #              as.double(t_diff),
  #              " ",
  #              units(t_diff)))


  # combine the loaded data and change to dataframe
  names(loaded_data) <- file_names_only
  loaded_data <- dplyr::bind_rows(loaded_data, .id = "fname")

  # save the RDS file a snapshot if the user requests it
  if (save_output == TRUE) {
    save_ques_dialog <- "Caution.\n\nContains sensitive information.\n\nSave to secure locations only"
    user_wants_to_save <- rstudioapi::showQuestion("Save All EE RDS File",
                                                   save_ques_dialog,
                                                   ok = "Save",
                                                   cancel = "Cancel")
    if (user_wants_to_save) {
      dir_select_capt <- "Select Location to Save All EE RDS File."
      user_selected_dir <- rstudioapi::selectDirectory(caption = dir_select_capt,
                                                       label = "Select",
                                                       path = "./")

      max_date <- max(loaded_data$date)

      rds_name <- paste0(user_selected_dir,
                         "/all_ee_compiled-",
                         max_date,
                         ".RDS")

      print(paste0("Compiled RDS file saved to ", rds_name))

      saveRDS(loaded_data, rds_name)
    }
  }
  return(loaded_data)
}

#' Read All Employee Report from CSV source
#'
#' given a csv file containing the all employees report data, load it into a
#' dataframe. Uses data.table's \code{fread} function for performance reasons.
#' Column types are specified using \code{all_ee_col_types} function.
#'
#' @param path the full path the csv file
#' @param name the full name of the csv file
#'
#' @return an unnamed dataframe containing the all employees data
#'
#' @seealso allee_dates_from_fnames, all_ee_col_types, get_all_ee_report
fread_allee_csv <- function(path, name) {
  suppressPackageStartupMessages({
    require(data.table)
  })

  # compute the date of the file to determine the column types
  # contained in it. This date will be placed into it's own column
  # after it is read into a data.table
  fname_date <- allee_dates_from_fnames(name)

  col_fread_types <- all_ee_col_types(date = fname_date)

  dt <- data.table::fread(path,
                          header = TRUE,
                          sep = ";",
                          colClasses = col_fread_types,
                          skip = 12)


  df <- data.table::setDF(dt)
  # place the date into it's own column. The date will be added as a name of the
  # df after being placed in a list of other All Employee report dataframes.
  df$date <- fname_date

  return(df)
}

#' Get FTVORGN table data
#'
#' @description ftvorgn data is used to calculate organization hierarchy via the
#'   logical structure of the organization codes. This validation table is
#'   maintained by the finance team and regularly updated with new organization
#'   codes and org. titles. For this reason, it is recommended to use this
#'   function to pull the most up-to-date ftvorgn table data directly from
#'   Banner. Requires Banner logon credentials.
#'
#' @param opt_as_of_date a POSIXct formated date to be used to select only
#'   records on or before a certain time. useful when pulling backdated banner
#'   snapshots
#' @param opt_bann_conn an active banner connection object typically derived
#'   from \code{get_banner_conn()}. If not provided will create a temp
#'   connection
#'
#' @return a dataframe containing all FTVORGN table variables. Depreciated rows
#'   are removed leaving only the most recent record on or prior to the
#'   as_of_date.
#' @export
get_ftvorgn_data <- function(opt_as_of_date, opt_bann_conn) {
  # ensure that timezones are properly handled by ROracle. This avoids daylight
  # savings conversions
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  if (missing(opt_bann_conn)) {
    # make a banner connection
    banner_con <- get_banner_conn()
  } else banner_con <- opt_bann_conn

# pull ftvorgn table ------------------------------------------------------

  sql_qry <- "select * from FTVORGN"
  ftvorgn_data <- ROracle::dbGetQuery(banner_con,
                                      statement = sql_qry)


# filter out future rows --------------------------------------------------

  if (!missing(opt_as_of_date)) {
    ftvorgn_data <- dplyr::filter(ftvorgn_data,
                                  FTVORGN_EFF_DATE <= opt_as_of_date,
                                  FTVORGN_NCHG_DATE >= opt_as_of_date,
                                  FTVORGN_TERM_DATE > opt_as_of_date | is.na(FTVORGN_TERM_DATE))
    ftvorgn_data$as_of_date <- opt_as_of_date
  }


# filter out depreciated rows ---------------------------------------------

  # get the max assignment date for each organization code. These will be used
  # to filter out the old depreciated rows.
  orgn_max_dates <- dplyr::group_by(ftvorgn_data,
                                    FTVORGN_ORGN_CODE)
  orgn_max_dates <- dplyr::summarize(orgn_max_dates,
                                     max_date = max(FTVORGN_EFF_DATE))

  orgn_max_dates_keys <- paste0(orgn_max_dates$FTVORGN_ORGN_CODE,
                                orgn_max_dates$max_date)

  ftvorgn_data$key <- paste0(ftvorgn_data$FTVORGN_ORGN_CODE,
                             ftvorgn_data$FTVORGN_EFF_DATE)

  # #do the filtering for the final dataset
  # ftvorgn_data_out <- dplyr::filter(ftvorgn_data, key %in% orgn_max_dates_keys)
  # #FTVORGN_STATUS_IND == "A",
  # is.na(ftvorgn_data$FTVORGN_TERM_DATE) |
  #  ftvorgn_data$FTVORGN_TERM_DATE > Sys.Date(),
  # #added nchg filter to try address duplicate org names. may break things?
  # !FTVORGN_NCHG_DATE < as.POSIXct(Sys.Date()))

  return(ftvorgn_data)
}


#' Load Flat file 'Payroll Earnings and Labor Distribution by Employee' from
#' file
#'
#' @description load payroll datafiles into a single dataframe. Start and end
#'   date should cover first day of the month in question. Data originally
#'   sourced from the ReportWeb 'Payroll and Earnings Labor Distribution
#'   Report.'
#'
#' @param opt_fpath an optional parameter if the payroll files are located in a
#'   non-default location
#' @param most_recent_only pull only the most recent payroll saved to the
#'   fpath.Overrules opt_start_date.
#' @param opt_start_date an optional POSIXct start date. Payrolls covering dates
#'   prior will be excluded from the returned dataframe.
#' @param opt_end_date, an optional POSIXct end date. Payrolls covering dates
#'   after will be exclude form the returned dataframe.
#' @param filter_max_org a boolean operator spcifying whether to return only the
#'   row corresponding to the highest funding organizations for each job, or to
#'   return the full labor distribution
#' @param add_pidm a boolean specifying whether a pidm should be included in the
#'   output. the file currently only contains GID values
#' @param opt_bann_conn a banner connection option required if pidms are being
#'   returned. can be dropped if pidms not included
#'
#' @return a single dataframe containing all payroll data
#' @import data.table tictoc
#' @export
get_payroll_data <- function(opt_fpath = "X:/Employees/Payroll Earnings & Labor Distrubtion by Employee/",
                             most_recent_only = T,
                             opt_start_date,
                             opt_end_date,
                             filter_max_org = T,
                             add_pidm = T,
                             opt_bann_conn) {
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  suppressPackageStartupMessages({
    require(dplyr)
    require(data.table)
    require(tictoc)
    require(stringr)
    require(lubridate)
  })

  fpattern <-  "^2[0-9]{3}PR[0-9]{2}.txt"

  if (missing(opt_fpath)) {
    fpath <- "X:/Employees/Payroll Earnings & Labor Distrubtion by Employee/"
  } else {
    fpath <- opt_fpath
  }

# Determine payroll files to load -----------------------------------------
  file_df <- data.frame(fname = list.files(fpath),
                        fpath = list.files(fpath, full.names = TRUE),
                        stringsAsFactors = FALSE) %>%
    mutate(f_year =  as.numeric(substr(fname,1,4)),
           f_pr_number = as.numeric(substr(fname,7,8)),
           f_true_year = if_else(f_pr_number == 1,
                                 f_year - 1,
                                 f_year),
           f_true_month = if_else(f_pr_number == 1,
                                  12,
                                  f_pr_number - 1),
           f_date_start = as.POSIXct(paste0(f_true_year, "-", f_true_month, "-01")),
           f_date_end = as.POSIXct(paste0(f_true_year, "-", f_true_month, "-", days_in_month(as.Date(paste0(f_true_year, "-", f_true_month, "-01"))))))

  # file by start_date
  if (!missing(opt_start_date)) {
    start_date <- as.POSIXct(opt_start_date)
    day(start_date) <- 1
    file_df <- filter(file_df,
                      f_date_start >= start_date)
  }
  if (!missing(opt_end_date)) {
    end_date <- as.POSIXct(opt_end_date)
    file_df <- filter(file_df,
                      f_date_end <= end_date | (end_date >= f_date_start & end_date <= f_date_end))
  }

  if (nrow(file_df) < 1) {
    stop("Could not find payroll files for given start/end dates - exiting")
  }

  if (most_recent_only == TRUE) {
    file_df <- filter(file_df,
                      f_date_start == max(file_df$f_date_start))
  }

# load files from disk ----------------------------------------------------

  files_to_load <- unlist(file_df$fpath)
  file_names <- unlist(file_df$fname)

  col_classes <- list(character = c("GID",
                                    "Name",
                                    "Position Number",
                                    "Suffix",
                                    "Earn Code",
                                    "Earn Code Desc",
                                    "Index",
                                    "Activity Code",
                                    "Organization"),
                      numeric = c("Hours or Units",
                                  "Amount",
                                  "Percent"))

  tic("reading payroll data from file source")
  output <- lapply(files_to_load,
                   FUN = data.table::fread,
                   sep = ";",
                   stringsAsFactors = FALSE,
                   skip = 8,
                   colClasses = col_classes,
                   verbose = F)

  names(output) <- gsub(pattern = ".txt",
                        replacement = "",
                        x = file_names)
  toc()


  output <- data.table::rbindlist(output,
                                  idcol = "fname")
  old_names <- names(output)
  new_names <- old_names %>%
    stringr::str_to_lower() %>%
    stringr::str_replace_all(" ", "_")

  data.table::setnames(output, old = old_names, new = new_names)

  output <- rename(output,
                   posn = position_number,
                   suff = suffix)

  if (!"fname" %in% names(output)) {
    #only a single payroll is being returned so the fname column was not added
    #in the data.table::rbindlist(output,idcol) function
    #
    output$fname <- file_names[1]
  }

# Supplement output with true year, month and FY ------------------------
  if(is.na(output)) {
    stop("Output was NA, exiting")
  }

  if (!is.data.table(output)) {
    output <- data.table(output)
  }


  # add the year and payroll number as seperate columns. Currenly data is stored
  # in the fname column in character type with the format "YYYYPR##"
  # data.table syntax
  #
  output[, "pr_year" := list(substr(fname, 1, 4))]
  output[, "pr_year" := list(as.numeric(pr_year))]

  output[, `pr` := list(substr(`fname`, 7 , 8))] [,`pr` := list(as.numeric(`pr`))]

  # add a 'true' year and month that corrects for payroll being on the 11 day of
  # the following month. PR 1 of 2018 truely represents work done in December,
  # 2017
  output[pr == 1, true_month := 12][pr != 1, true_month := pr - 1]
  output[pr == 1, true_year := pr_year - 1][pr != 1, true_year := pr_year]

  output[true_month >= 7, fy := true_year + 1][!true_month >= 7, fy := true_year]
  output[,`true_date` := as.POSIXct(paste0(true_year, "-",true_month,"-01"))]

  output <- data.table::setDF(output)


  if (add_pidm == T) {
    tic("pull and join pidms onto payroll dataset")
    if (missing(opt_bann_conn)) {
      bann_conn <- opa::get_banner_conn()
    } else {
      bann_conn <- opt_bann_conn
    }

    gid_list <- opa::split_vec_for_sql(output$gid)
    pidm_out <- tibble()

    for (i in 1:length(gid_list)) {
      pidm_temp <- opa::get_pidm_gid_lu(bann_conn, opt_gid_vec = gid_list[[i]])

      pidm_out <- bind_rows(pidm_out, pidm_temp)
    }
    output <- left_join(output,
                        pidm_out,
                        by = c("gid"))
    toc()
  }
  toc()
  return(output)
}


#' @title Pull a GID-PIDM lookup table from Banner
#'#' @description get a dataset containing gids and their corresponding pidms. This is pulled
#' directly from banner. If a banner connection is not possible, see OPA's
#' employee snapshot files. This dataset is comprehensive for every student and
#' employee that has ever worked on campus while banner has been implemented
#'
#' @param opt_banner_conn if a banner connection has already been made, supply
#'   it here. Otherwise, this function will prompt for logon credentials for a
#'   one time use connection.
#' @param opt_pidm_vec use this optional parameter to filter the underlying sql
#'   query. Useful for time-sensitive applications.
#' @param opt_gid_vec use this optional parameter to filter the underlying sql
#'   query. Useful for time-sensitive applications.
#'
#' @return a two column dataframe containing gids and corresponding pidms
#'
#' @import magrittr
#' @export
#' @author Ian C Johnson
get_pidm_gid_lu <- function(opt_banner_conn, opt_pidm_vec, opt_gid_vec) {

  suppressPackageStartupMessages({
    require(magrittr)
    require(dplyr)
  })

  # stop the function if both a gid and pidm vector is supplied for filtering
  # purposes. it is likely possible to filter on both but will require further
  # development.
  if (!missing(opt_pidm_vec) & !missing(opt_gid_vec)) {
    stop("function get_pidm_gid_lu supplied both a pidm and gid vector to filter.
         Only one can be supplied at a time.")
  }

  # ensure that timezones are properly handled by ROracle. This avoids daylight
  # savings conversions
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  # get a one-time use banner connection if one is not supplied as an input
  # parameter
  if (missing(opt_banner_conn)) {
    bnr_conn <- msuopa::get_banner_conn()
  } else {
    bnr_conn <- opt_banner_conn
  }

  # ensure that the optional gid or pidm filter vectors do not contain more than
  # 1000 members. This limitation is imposed by Oracle.
  if (!missing(opt_pidm_vec)) {
    stopifnot(length(opt_pidm_vec) <= 1000)
  }
  if (!missing(opt_gid_vec)) {
    stopifnot(length(opt_gid_vec) <= 1000)
  }

  results <- tbl(bnr_conn, "SPRIDEN") %>%
    filter(is.na(SPRIDEN_CHANGE_IND)) %>%
    select(SPRIDEN_PIDM,
           SPRIDEN_ID)

  if (!missing(opt_gid_vec)) {
    results <- filter(results, SPRIDEN_ID %in% opt_gid_vec)
  } else if (!missing(opt_pidm_vec)) {
    results <- filter(results, SPRIDEN_PIDM %in% opt_pidm_vec)
  }

  results <- collect(results) %>%
    rename(pidm = SPRIDEN_PIDM,
                    gid = SPRIDEN_ID) %>%
    distinct(.keep_all = TRUE)

  return(results)
}

#' Pull GID, PIDM, or NetID identifiers for a vector of employee ids
#'
#' Three primary identifier values used in various datasets. GID is the banner
#' ID typically used for outward facing implementations. PIDM is the internal ID
#' used on banner database tables. NetID is the the external id used for
#' single-sign-on and other third party applications.
#'
#' @param ids_in a vector of ids for which an alternative type will be pulled. Can handle leng
#' @param type_in one of 'pidm', 'gid', or 'netid' which specifies the input id
#'   type
#' @param type_out one of 'pidm', 'gid', or 'netid' which specifies the output id
#'   type
#'
#' @return
#' @export
get_netid_pidm_gid <- function(ids_in,
                               type_in,
                               opt_bann_conn) {

  # get a banner connection if not supplied.
  if (missing(opt_bann_conn)) {
    bann_conn <- opa::get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }

  # ensure that input type parameter is one of the following accepted arguments
  if (!type_in %in% c("gid", "pidm", "netid")) {
    stop("type_in parameter must be one of 'gid', 'pidm', or 'netid")
  }

  # split if the length of the vector is greater than 1000
  exceeds_1000 <- length(ids_in) > 1000
  if (exceeds_1000) {ids_in_list <- opa::split_vec_for_sql(ids_in)}

  # the base query that joins and pulls the three primary id types. doesn't
  # filter or collect the results because it depends on the input parameter
  # types.
  goradid_tbl <- tbl(bann_conn, "GORADID")
  ids_out_unfiltered <- tbl(bann_conn, "SPRIDEN") %>%
    left_join(goradid_tbl, by = c("SPRIDEN_PIDM" = "GORADID_PIDM")) %>%
    select(PIDM = SPRIDEN_PIDM,
           GID = SPRIDEN_ID,
           NETID = GORADID_ADDITIONAL_ID)
  # for each input type filter and pull the data.
  # use an in-line lapply function to pull if the vector of input data exceeds 1000 values
  if (type_in == "gid") {
    if (exists("ids_in_list")) {
      ids_out <- lapply(ids_in_list,
                        FUN = function(x){ids_out_unfiltered %>% filter(GID %in% x) %>% collect() %>% distinct()}) %>% bind_rows()
    } else {
      ids_out <- ids_out_unfiltered %>%
        filter(GID %in% ids_in) %>%
        collect()
    }
  } else if (type_in == "pidm") {
    if (exists("ids_in_list")) {
      ids_out <- lapply(ids_in_list,
                        FUN = function(x){ids_out_unfiltered %>% filter(PIDM %in% x) %>% collect() %>% distinct()}) %>% bind_rows()
    } else {
      ids_out <- ids_out_unfiltered %>%
        filter(PIDM %in% ids_in) %>%
        collect()
    }
  } else if (type_in == "netid") {
    if (exists("ids_in_list")) {
      ids_out <- lapply(ids_in_list,
                        FUN = function(x){ids_out_unfiltered %>% filter(NETID %in% x) %>% collect() %>% distinct()}) %>% bind_rows()
    } else {
      ids_out <- ids_out_unfiltered %>%
        filter(NETID %in% ids_in) %>%
        collect()
    }
  }

  # clean up bad netid and gid values to avoid duplicate and erroneous rows
  ids_out <- filter(ids_out,
                    nchar(NETID) == 7,
                    substr(GID, 1, 1) == "-") %>%
    distinct()

  return(ids_out)
}


#' Load Datamart history files
#'
#' @param most_recent_only boolean to determine whether to load all files, or only the most recent
#' @param opt_year an optional 4-digit numeric year paramater
#' @param opt_month an optional 1 or 2 digit numeric month parameter
#' @param fpath an optional fpath to be used if the source file location changes or the hellene drive is not mapped to the X Drive
#'
#' @return a single dataframe containing the BASE table joined with the JOBS table for the requested months
#' @export
get_dmhs <- function(most_recent_only = TRUE,
                     opt_year = NA,
                     opt_month = NA,
                     fpath = "X:/Employees/EMR Report (production)/DM csv files/") {

  suppressPackageStartupMessages({
    require(readr)
    require(dplyr)
    require(magrittr)
  })

  # set to UTC time zone to ensure compatibility with Banner dates
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  # get the list of files and reverse them so that the 'newest' is indexed to
  # the first entry in the list i.e. list[1]
  dmhs_jobs_files <- list.files(path = fpath,
                                pattern = "dmhs_jobs",
                                full.names = TRUE)
  dmhs_jobs_files <- rev(dmhs_jobs_files)

  dmhs_base_files <- list.files(path = fpath,
                                pattern = "dmhs_base",
                                full.names = TRUE)
  dmhs_base_files <- rev(dmhs_base_files)

  if (most_recent_only == TRUE) {
    dmhs_jobs_files <- dmhs_jobs_files[[1]]
    dmhs_jobs <- readr::read_csv(dmhs_jobs_files,
                                 col_types = dmhs_jobs_get_col_types())
    dmhs_base_files <- dmhs_base_files[[1]]
    dmhs_base <- readr::read_csv(dmhs_base_files,
                                 col_types = dmhs_base_get_col_types())
    # if all data is needed, use every file found by list.files,
  } else if (!is.na(opt_year) | !is.na(opt_month)) {

    # both optional year and optional month are needed here. perhaps this could
    # be revised to dynamically filter if one is not supplied as a parameter?
    if (is.na(opt_year) | is.na(opt_month)) {
      stop("Both opt_year and opt_month are required if one is supplied. msuopa::get_dhms()")
    }

    #basic error checking on opt_year and opt_month inputs
    stopifnot(nchar(opt_year) == 4)

    if (is.numeric(opt_month) & nchar(opt_month) == 1) {
      opt_month <- paste0("0", opt_month)
    }

    stopifnot(nchar(opt_month) == 2)
    #date filtering requires input files to be named in the
    #dmhs_base_YYYYMMDD.csv format
    file_char_length <- stringr::str_length(dmhs_base_files)

    dmhs_year_month_str <- substr(dmhs_base_files,
                                  file_char_length - 11,
                                  file_char_length - 6)
    matching_dte_indx <- dmhs_year_month_str == paste0(opt_year, opt_month)

    dmhs_jobs <- readr::read_csv(dmhs_jobs_files[matching_dte_indx],
                                 col_types = dmhs_jobs_get_col_types())

    dmhs_base <- readr::read_csv(dmhs_base_files[matching_dte_indx],
                                 col_types = dmhs_base_get_col_types())

  } else {
    dmhs_jobs <- lapply(dmhs_jobs_files,
                        readr::read_csv,
                        col_types = dmhs_jobs_get_col_types())
    dmhs_jobs <- dplyr::bind_rows(dmhs_jobs)

    dmhs_base <- lapply(dmhs_base_files,
                        readr::read_csv,
                        col_types = dmhs_base_get_col_types())

    dmhs_base <- dplyr::bind_rows(dmhs_base)

  }


  dmhs_jobs$JOBS_PIDM <- as.numeric(dmhs_jobs$JOBS_PIDM)
  dmhs_base$BASE_PIDM <- as.numeric(dmhs_base$BASE_PIDM)

  # join to BASE to JOBS on pidm and report date
  dmhs <- dplyr::left_join(dmhs_jobs,
                           dmhs_base,
                           by = c("JOBS_PIDM" = "BASE_PIDM",
                                  "JOBS_REPORT_DATE" = "BASE_REPORT_DATE"))
  dmhs <- dplyr::mutate(dmhs,
                        job_key = paste0(JOBS_PIDM, JOBS_POSN, JOBS_SUFF),
                        job_key_date = paste0(job_key, JOBS_REPORT_DATE))

  dmhs <- filter(dmhs, JOBS_PICT_CODE == "4M", substr(JOBS_POSN, 1, 1) == "4")

  return(dmhs)
}

#' Not to be exported
dmhs_base_get_col_types <- function() {
  types <- readr::cols(BASE_PIDM = readr::col_number(),
                       BASE_FIRST_HIRE_DATE = readr::col_date(format = "%m/%d/%Y"),
                       BASE_ADJ_SERVICE_DATE = readr::col_date(format = "%m/%d/%Y"),
                       BASE_SENIORITY_DATE = readr::col_date(format = "%m/%d/%Y"),
                       BASE_TERMINATION_DATE = readr::col_date(format = "%m/%d/%Y"),
                       BASE_CURRENT_HIRE_DATE = readr::col_date(format = "%m/%d/%Y"),
                       BASE_FIRST_WORK_DATE = readr::col_date(format = "%m/%d/%Y"),
                       BASE_REPORT_DATE = readr::col_date(format = "%m/%d/%Y"),
                       BASE_AREA_CODE = readr::col_character(),
                       BASE_CELL_AREA_CODE = readr::col_character(),
                       BASE_ETHNICITY_CODE = readr::col_character(),
                       BASE_PHONE_NUMBER = readr::col_character(),
                       BASE_CELL_INTL_NUMBER = readr::col_character(),
                       BASE_CELL_PHONE_NUMBER = readr::col_character(),
                       BASE_CH_ZIP = readr::col_character(),
                       .default = readr::col_character())
  return(types)
}

#' Not to be exported
dmhs_jobs_get_col_types <- function() {
  types <- readr::cols(JOBS_PIDM = readr::col_number(),
                       JOBS_FTE = readr::col_double(),
                       JOBS_BEGIN_DATE = readr::col_date(format = "%m/%d/%Y"),
                       JOBS_EFFECTIVE_DATE = readr::col_date(format = "%m/%d/%Y"),
                       JOBS_OCHE_JOB_BEGIN_DATE = readr::col_date(format = "%m/%d/%Y"),
                       JOBS_REPORT_DATE = readr::col_date(format = "%m/%d/%Y"),
                       JOBS_ORG_LVL1_CODE = readr::col_character(),
                       JOBS_ORG_LVL2_CODE = readr::col_character(),
                       JOBS_ORG_LVL2_DESC = readr::col_character(),
                       JOBS_ORG_LVL3_CODE = readr::col_character(),
                       JOBS_ORG_LVL3_DESC = readr::col_character(),
                       JOBS_ORG_LVL4_CODE = readr::col_character(),
                       JOBS_ORG_LVL4_DESC = readr::col_character(),
                       JOBS_ORG_LVL5_CODE = readr::col_character(),
                       JOBS_ORG_LVL5_DESC = readr::col_character(),
                       JOBS_ORG_LVL6_CODE = readr::col_character(),
                       JOBS_ORG_LVL6_DESC = readr::col_character(),
                       JOBS_ORG_LVL7_CODE = readr::col_character(),
                       JOBS_ORG_LVL7_DESC = readr::col_character(),
                       JOBS_ORGN_1_PERCENT = readr::col_double(),
                       JOBS_ORGN_2_PERCENT = readr::col_double(),
                       JOBS_ORGN_3_PERCENT = readr::col_double(),
                       JOBS_ORGN_4_PERCENT = readr::col_double(),
                       JOBS_ORGN_5_PERCENT = readr::col_double(),
                       JOBS_ORGN_6_PERCENT = readr::col_double(),
                       JOBS_ORGN_7_PERCENT = readr::col_double(),
                       JOBS_NEW_HIRE = readr::col_double(),
                       JOBS_LONGEVITY_HOURLY_RATE = readr::col_double(),
                       JOBS_TERMINATION = readr::col_double(),
                       JOBS_TRANSFER_OUT = readr::col_double(),
                       JOBS_UNION_CODE = readr::col_character(),
                       JOBS_PAY_FACTOR = readr::col_double(),
                       JOBS_CIP_CODE = readr::col_character(),
                       JOBS_LWOP_START = readr::col_double(),
                       JOBS_FTE_CHANGE = readr::col_character(),
                       JOBS_REG_RATE = readr::col_double(),
                       .default = readr::col_character())
  return(types)
}

#' Union & Join all DMHS datafiles
#'
#' Load, Union, and Join all dmhs files found in "X:/Employees/EMR Report
#' (production)/DM csv files/". Save single csv file into the same folder as
#' "full_dmhs_data"
#'
#' @return A .csv file containing the joined and unioned base, jobs datasets
#' @export
build_emr_dataset <- function(min_date, max_date) {
  # load all dmhs tables saved in "X:/Employees/EMR Report (production)/DM csv files/"

  #TODO: build ability to set a min date in get_dmhs function. use min/max to
  #filter this at load rather than after load
  dmhs <- opa::get_dmhs(most_recent_only = F)

  # dmhs <- filter(dmhs, JOBS_REPORT_DATE >= min_report_date,
  #        JOBS_REPORT_DATE <= max_report_date)

  opa::write_report(df = dmhs,
                    fpath = "X:/Employees/EMR Report (production)/DM csv files/",
                    fname = "full_dmhs_data", sheetname = "NA", include_xlsx = F)

  return(dmhs)
}


#' Get the most recent Mailing and Campus Address Data for a set of PIDMs.
#'
#' Depreciated. Use 'pull_contact_info' function instead.
#'
#' @description An unoptimized banner pull for campus and address data.
#'   Primarily pulled from SPRADDR table.
#'
#' @param pidm_vec a vector containing a set of pidms for which the addresses
#'   will be returned
#' @param opt_bann_conn if a banner connection has already been made, supply it
#'   here. Otherwise, this function will prompt for logon credentials for a one
#'   time use connection.
#'
#' @return a dataframe containing one row per person per address type with
#'   corresponding address columns
#' @export
#' @import magrittr
#'
get_address_data <- function(pidm_vec, opt_bann_conn) {

  # set to UTC time zone to ensure compatibility with Banner dates
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  suppressPackageStartupMessages({
    require(dplyr)
  })


  # if a banner connection isn't supplied, create one
  if (missing(opt_bann_conn)) {
    bann_conn <- opa::get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }

  # get a lookup of max dates for each pidm and address type
  # get the max sequence number used for each type
  spraddr_max_seq <- dplyr::tbl(bann_conn, "SPRADDR") %>%
    dplyr::filter(SPRADDR_ATYP_CODE %in% c("MA", "CA"),
                  SPRADDR_PIDM %in% pidm_vec) %>%

    dplyr::group_by(SPRADDR_PIDM, SPRADDR_ATYP_CODE) %>%

    dplyr::summarize(max_seq_no = max(SPRADDR_SEQNO, na.rm = T)) %>%

    dplyr::mutate("key" = paste0(SPRADDR_PIDM, SPRADDR_ATYP_CODE)) %>%
    dplyr::select(key, max_seq_no)


  #filter the spraddr data to only include max dated addresses
  spraddr_data <- dplyr::tbl(bann_conn, "SPRADDR") %>%
    dplyr::select(SPRADDR_PIDM,
                  SPRADDR_ATYP_CODE,
                  SPRADDR_SEQNO,
                  SPRADDR_STREET_LINE1,
                  SPRADDR_STREET_LINE2,
                  SPRADDR_STREET_LINE3,
                  SPRADDR_CITY,
                  SPRADDR_STAT_CODE,
                  SPRADDR_ZIP,
                  SPRADDR_CNTY_CODE,
                  SPRADDR_PHONE_AREA,
                  SPRADDR_PHONE_NUMBER)

  spraddr_data <- dplyr::mutate(spraddr_data,
                                "key" = paste0(SPRADDR_PIDM,
                                               SPRADDR_ATYP_CODE))

  spraddr_data <- dplyr::left_join(spraddr_data,
                                   spraddr_max_seq, by = "key")

  spraddr_data <-   dplyr::filter(spraddr_data,
                                  SPRADDR_ATYP_CODE %in% c("MA", "CA"),
                                  SPRADDR_PIDM.x %in% pidm_vec,
                                  SPRADDR_SEQNO == max_seq_no)

  spraddr_data <- dplyr::collect(spraddr_data)

  return(spraddr_data)
}


#' Pull most Employee Name data
#'
#' return the full and most up-to-datre names of individuals specified by the
#' input gid or pidm vector. No limit on length of input vectors
#'
#' @param gid_vec a vector of gids specifiying the individuals whose names will
#'   be returned
#' @param pidm_vec a vector of pidms specifiying the individuals whose names will
#'   be returned
#' @param opt_bann_conn an optional banner connection object
#'
#' @return a dataframe containing names and pidms
#' @export
get_person_names_data <- function(pidm_vec, gid_vec, opt_bann_conn) {
  if (missing(gid_vec) & missing(pidm_vec)) {
    stop("must supply gid or pidm vec to get_name_data")
  }

  if (missing(opt_bann_conn)) {
    bann_conn <- get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }

  # default set to UTC when pulling dates from banner. This sets the environment
  # so that the DBI package properly handles, (doesn't handle for that matter!)
  # date conversions.
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")


  if (!missing(gid_vec) & missing(pidm_vec)) {
    pidm_vec <- get_pidm_gid_lu(bann_conn,
                                opt_gid_vec = gid_vec)
    pidm_vec <- pidm_vec$pidm
  }

  if (length(pidm_vec) > 1000) {
    pidms_split <- opa::split_vec_for_sql(pidm_vec)
    names <- lapply(pidms_split, name_query, bann_conn = bann_conn)
    names <- dplyr::bind_rows(names)
  } else {
    names <- name_query(pidm_vec, bann_conn)
  }

  names <- dplyr::mutate(names,
                         full_name = paste0(lname, ", ", fname))

  return(names)
}


#' Pull most recent name values from SPRIDEN table
#'
#' helper function. Fails with more than 1000k pidm values. Use
#' `get_person_names_data` function instead.
#'
#' @param pidm_vec a vector containing pidm values of the employees for which
#'   names will be pulled
#' @param bann_conn a banner connection object supplied by `opa::get_banner_conn()`
#'
#' @return a dataframe containing pidm and name.
name_query <- function(pidm_vec, bann_conn) {
  #pull the needed filds from SPRIDEN. CHANGE_IND is used to determine which
  # record to use if there are mult records with the same activity date
  names <- dplyr::tbl(bann_conn, "SPRIDEN") %>%
    dplyr::select(pidm = SPRIDEN_PIDM,
                  gid = SPRIDEN_ID,
                  lname = SPRIDEN_LAST_NAME,
                  fname = SPRIDEN_FIRST_NAME,
                  activity_date = SPRIDEN_ACTIVITY_DATE,
                  change_ind = SPRIDEN_CHANGE_IND) %>%
    dplyr::filter(pidm %in% pidm_vec,
                  is.na(change_ind)) %>%
    collect()

  # get the max activity date. For most people, this is their most recent record
  # those people with nrows > 1 have multiple records with the same activity
  # date, they will have to be handled separately
  max_dates <- names %>%
    group_by(pidm) %>%
    summarize(max_date = max(activity_date)) %>%
    mutate(pidm_date_max = paste0(pidm,
                                  max_date))




  # get a list of pidms for those people with multiple rows
  mult_row_pidms <- names %>%
    group_by(pidm) %>%
    summarize(n = n()) %>%
    filter(n > 1)

  # first, filter to the max activity date for each employee
  names <- names %>%
    mutate(pidm_dates = paste0(pidm,
                               activity_date)) %>%
    filter(pidm_dates %in% max_dates$pidm_date_max) %>%
    select(pidm,
           gid,
           fname,
           lname,
           change_ind)

  # if anybody still has multiple rows, remove the one that doesn't have an
  # "N" change indicator specifying name change
  #
  # get a list of pidms for those people with multiple rows
  mult_row_pidms <- names %>%
    group_by(pidm) %>%
    summarize(n = n()) %>%
    filter(n > 1)

  names <- names %>%
    mutate(remove = ifelse(!pidm %in% mult_row_pidms$pidm,
                           FALSE,
                           ifelse(pidm %in% mult_row_pidms$pidm &
                                    change_ind == "N",
                                  FALSE,
                                  TRUE))) %>%
    filter(remove == FALSE) %>%
    select(-remove,
           -change_ind)


  return(names)
}

#' Pull Supervisor Names and PIDMs
#'
#' For a set of input pidms, pull the supervisor name and pidm data from the
#' NBRJOBS_SUPERVISOR_PIDM field.
#'
#' @param pidm_vec a vector or PIDM values not to exceed 1000 values
#' @param as_of_date a character date that will be used to filter values to the appropriate time-period
#' @param opt_bann_conn an optional banner connection object
#'
#' @return a dataframe containing the input pidms and associated supervisor pidms and names
#' @export
pull_supv_names <- function(pidm_vec, as_of_date, opt_bann_conn) {
  require(tidyverse)
  require(magrittr)

  # get a banner connection if not supplied. Used to pull PERAPPT data.
  if (missing(opt_bann_conn)) {
    bann_conn <- opa::get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }

  as_of_date_psx <- as.POSIXct(as_of_date)

  if(length(pidm_vec) > 1000) {
    stop("PIDM vector exceeds max length of 1000")
  }

  if(as_of_date < as.POSIXct("2020-01-01")) {
    warning(paste0("Supervisor data does not exist prior to 2020-01-01 - no values to return for as of date = ", as_of_date))
  }

  df_out <- tbl(bann_conn, "NBRJOBS") %>%
    select(PIDM = NBRJOBS_PIDM,
           SUPV_PIDM = NBRJOBS_SUPERVISOR_PIDM,
           EFF_DATE = NBRJOBS_EFFECTIVE_DATE) %>%
    filter(PIDM %in% pidm_vec,
           !is.na(SUPV_PIDM)) %>%
    collect() %>%
    distinct(PIDM, SUPV_PIDM, .keep_all = T) %>%
    group_by(PIDM) %>%
    filter(EFF_DATE < as_of_date_psx,
           EFF_DATE == max(EFF_DATE))
  supv_names <- opa::get_person_names_data(unique(df_out$SUPV_PIDM),opt_bann_conn = bann_conn) %>% select(pidm, full_name)
  df_out <- left_join(df_out, supv_names, by = c("SUPV_PIDM" = "pidm")) %>%
    rename(SUPV_NAME = full_name)

  return(df_out)
}

#' Pull Degree History Data for all Employees
#'
#' @param opt_bann_conn an optional banner connection object
#'
#' @return
#' @export
pull_degree_history <- function(opt_bann_conn) {

  # get a banner connection if not supplied. Used to pull PERAPPT data.
  if (missing(opt_bann_conn)) {
    bann_conn <- msuopa::get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }

  sql_query <- opa::load_sql_qry(file_path = "//helene/opa$/icj_dts/sql_files/",
                                 file_name = "degree_hist_pull.sql")

  ROracle::dbGetQuery(statement = sql_query,
                      conn = bann_conn)

}

#' Pull Up-to-date and Preferred contact information
#'
#' Pulls data from the V_S_CURRENT_CONTACT_INFO View in Banner. This view
#' replaces Bob's old Auto Address Access script.
#'
#' @param pidm_vec a vector of pidms to filter the returned dataset. Can handle
#'   vectors greater than 1k items long.ms
#' @param opt_bann_conn an optional banner connection object
#'
#' @return a dataframe containing preferred/up-to-date email, phone, and address
#'   information.
#' @export
pull_contact_info <- function(pidm_vec,
                              opt_bann_conn) {
  require(tidyverse)

  # get a banner connection if not supplied. Used to pull PERAPPT data.
  if (missing(opt_bann_conn)) {
    bann_conn <- opa::get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }

  if (length(pidm_vec) > 1000) {
    split_pidm_vec <- opa::split_vec_for_sql(pidm_vec)
    contact_results <- lapply(split_pidm_vec, contact_sql_pull, bann_conn)
    contact_results <- bind_rows(contact_results)
  } else {
    contact_results <- contact_sql_pull(pidm_vec, bann_conn)
  }


  return(contact_results)
}

#' Helper function for pull_contact_info.
#'
#' Not to be exported.
#'
#' @param pidm_vec_in a vector of pidms not to exceed 1k items in length.
#' @param bann_conn banner connection object
contact_sql_pull <- function(pidm_vec_in, bann_conn) {
  df_out <- tbl(bann_conn, "V_S_CURRENT_CONTACT_INFO") %>%
    filter(PIDM %in% pidm_vec_in) %>%
    collect()

  return(df_out)
}

#' Pull PEREHIS employee data for a set of pidms
#'
#' @param pidm_vec_in a vector containing pidms for which perehis data will be
#'   returned
#' @param opt_as_of_date only return records occuring prior to this optional
#'   date. all records returned if not specified.
#' @param opt_bann_conn an optional banner connection object
#'
#' @return PEREHIS banner table records for a given set of employees
#' @export
pull_perehis_data <- function(pidm_vec_in, opt_as_of_date, opt_bann_conn) {
  # get a banner connection if not supplied. Used to pull PERAPPT data.
  if (missing(opt_bann_conn)) {
    bann_conn <- opa::get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }

  # set to UTC time zone to ensure compatibility with Banner dates
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  stopifnot(length(pidm_vec_in) <= 1000)
  #
  data_out <- tbl(bann_conn, "PEREHIS") %>%
    filter(PEREHIS_PIDM %in% pidm_vec_in)

  # convert to posixct or date to use in the banner query
  if (!missing(opt_as_of_date)) {
    if (class(opt_as_of_date) == "character") {
      as_of_date <- as.POSIXct(opt_as_of_date)
      message("converting character as_of_date to POSIXct  - job_lbr_dist_qry")
    } else if (inherits(opt_as_of_date, "Date")) {
      as_of_date <- opt_as_of_date
    } else {
      stop("Invalid as_of_date supplied to job_lbr_dist_qry")
    }
  }

  if(!missing(opt_as_of_date)) {
    data_out <- data_out %>%
      filter(PEREHIS_EFFECTIVE_DATE <= as_of_date)
  }

  data_out <- data_out %>% collect()

  return(data_out)
}

#
# pull_phraccr_data <- function(opt_pidms,
#                               opt_start_date,
#                               opt_end_date,
#                               opt_bann_conn) {
#
#   opa::return_fy_payrolls()
#
# }


#' Pull Term start and end dates from STVTERM table
#'
#' @param opt_bann_conn an optional banner connection object
#'
#' @return a dataframe containing term code, term desc, term start, and term end
#'   dates
#' @export
get_term_dates <- function(opt_bann_conn) {

  require(tidyverse)
  require(lubridate)

  # set to UTC time zone to ensure compatibility with Banner dates
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  # get a banner connection if not supplied. Used to pull PERAPPT data.
  if (missing(opt_bann_conn)) {
    bann_conn <- opa::get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }

  term_dates  <- tbl(bann_conn,
                     "STVTERM") %>%
    select(STVTERM_CODE,
           STVTERM_DESC,
           STVTERM_START_DATE,
           STVTERM_END_DATE) %>%
    collect() %>%
    rename(term_code = STVTERM_CODE,
           term_desc = STVTERM_DESC,
           term_start_date = STVTERM_START_DATE,
           term_end_date = STVTERM_END_DATE) %>%
    mutate(term_date_int = interval(term_start_date,
                                    term_end_date))

  save(term_dates, file = "./data/term_dates.rda")

  return(term_dates)
}

#' Build a Term code per day dataset to join term code by particula
#' @param opt_bann_conn
#'
#' @return
#' @export
build_term_start_end_date_df <- function(opt_bann_conn) {

  # get a banner connection if not supplied. Used to pull PERAPPT data.
  if (missing(opt_bann_conn)) {
    bann_conn <- opa::get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }

  day_dates <- seq.Date(as.Date("1997-01-01"), as.Date("2023-01-01"),
                        by = "days")
  day_dates <- as.POSIXct(day_dates)

  day_seq_df <- data.frame(as_of_date = day_dates)

  term_dates <- opa::get_term_dates(bann_conn)

  day_seq <- left_join(day_seq_df, term_dates, by = c("as_of_date" = "term_start_date"))
  day_seq <- mutate(day_seq, term_start_date = if_else(!is.na(term_end_date),
                                                       as_of_date,
                                                       as.POSIXct(NA)))

  day_seq <- relocate(day_seq, term_start_date, .before = term_end_date)
  #day_seq <- arrange(day_seq, desc(as_of_date))

  day_seq <- day_seq %>% do(zoo::na.locf(.))
  return(day_seq)

}
