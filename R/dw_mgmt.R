#' To be filled
#'
#' @param fy fiscal year of org hierarchy
#' @param write_to_dw boolean defaulted to TRUE
#' @param opt_campus_code optional campus code. when missing writes all campus
#'   hierarchies
#'
#' @return TRUE if exited properly
#' @export
build_org_hierarchy_tbl <- function(fy, write_to_dw = T, opt_campus_code) {

  # get connections to datawarehouse, banner
  dw_conn       <- opa::get_postgres_conn(opt_pword = "pin4sts")
  bann_conn     <- opa::get_banner_conn(verbose = F)

  # pull the underlying banner table
  org_hier_data <- opa::get_ftvorgn_data(opt_bann_conn = bann_conn)

  fy_dates <- paste0(fy, "-07-01")

  if (length(fy) > 1) {
    org_hiers <- lapply(fy_dates,
                        opa::build_org_hierarchy_lu,
                        opt_bann_conn = bann_conn,
                        opt_ftvorgn_data = org_hier_data)
    org_hiers_all <- bind_rows(org_hiers)
  } else {
    org_hiers <- opa::build_org_hierarchy_lu(fy_dates,
                                             opt_bann_conn = bann_conn,
                                             opt_ftvorgn_data = org_hier_data)
  }



  org_hiers_all <- mutate(org_hiers_all,
                          fy = opa::compute_fiscal_year(date)) %>%
    rename(orgn_code = seed,
           orgn_desc = seed_desc,
           orgn_hier_l1 = Org1_desc,
           orgn_hier_l2 = Org2_desc,
           orgn_hier_l3 = Org3_desc,
           orgn_hier_l4 = Org4_desc,
           orgn_hier_l5 = Org5_desc,
           orgn_hier_l6 = Org6_desc,
           orgn_hier_l7 = Org7_desc,
           orgn_code_l1 = Org1,
           orgn_code_l2 = Org2,
           orgn_code_l3 = Org3,
           orgn_code_l4 = Org4,
           orgn_code_l5 = Org5,
           orgn_code_l6 = Org6,
           orgn_code_l7 = Org7) %>%
    mutate(campus_code = case_when(substr(orgn_code_l1,1,1) == "4" ~ "BZ",
                                   substr(orgn_code_l1,1,1) == "3" ~ "GF",
                                   substr(orgn_code_l1,1,1) == "6" ~ "BL",
                                   substr(orgn_code_l1,1,1) == "7" ~ "HV"),
           is_timesheet_org = substr(orgn_code, 1, 1) %in% c("T", "Z") & !orgn_code == "TICAUX") %>%
    relocate(fy, campus_code, is_timesheet_org) %>%
    select(-date)




  DBI::dbWriteTable(dw_conn,
                    "orgn_hierarchy",
                    org_hiers_all,
                    overwrite = F,
                    append = T)
  return(TRUE)
}



#' Build time series
#'
#' @param start_date a start date for the series. overridden by
#'   use_default_start_end if set to true
#' @param end_date an end date for the series  overridden by
#'   use_default_start_end if set to true
#' @param freq one of 'week', 'month', or 'year'
#' @param use_default_start_end default of first day of fiscally year or month
#'   depending on freq
#'
#' @return
#' @export
build_time_series <- function(start_date,
                              end_date,
                              freq,
                              use_default_start_end = T) {
  require(xts)
  require(tidyverse)
  require(lubridate)

  if (use_default_start_end == T) {
    # default start date is the first day of FY11 or the first day of the week
    # containing the first day of FY11
    start_date <- case_when(freq == "week" ~ as.Date("2010-06-28"),
                            freq == "month" ~ as.Date("2010-07-01"),
                            freq == "year" ~ as.Date("2010-07-01"),
                            freq == "day" ~ as.Date("2010-07-01"))

    curr_date <- Sys.Date()
    # if the frequency is week, find the date of Monday of the week containing the current sys.date
    day_of_week <- lubridate::wday(curr_date, week_start = 1) # set week start t0 Monday
    curr_date_week_start <- curr_date
    lubridate::day(curr_date_week_start) <- lubridate::day(curr_date) - (day_of_week - 1)
    end_date   <- case_when(freq == "week" ~ curr_date_week_start,
                            freq == "month" ~ lubridate::`day<-`(curr_date, 1),
                            freq == "year" ~ as.Date(paste0(lubridate::year(curr_date), "-07-01")),
                            freq == "day" ~ curr_date)
  }

  if (freq == "week") {

    ts_c <- seq(from = start_date, to = end_date, by = freq) %>% as.POSIXct()

    ts_df <- data.frame(start_date = ts_week, end_date = ts_week, series_type = freq)
    ts_df <- mutate(ts_df, n_days = 7)
    lubridate::day(ts_week_df$end_date) <- lubridate::day(ts_week_df$start_date) + 6


  } else if(freq == "month") {

    ts_c  <- seq(from = start_date, to = end_date, by = freq) %>% as.POSIXct()
    ts_df <- data.frame(start_date = ts_week, series_type = freq)

    ts_df <- mutate(ts_df,
                    end_date = lubridate::rollforward(start_date),
                    n_days   = lubridate::days_in_month(start_date))


  } else if (freq == "year") {
    ts_df <- NA
  }



  return(ts_df)

}

#' Pull a report suitable for sharing salary info from the datawarehouse
#'
#' @param fy fiscal year of salaries
#' @param dw_conn datawarehouse postgres connection
#' @param opt_job_types optional job type filter, Any combo of 'Faculty NTT',
#'   'Classified', 'Professional', etc
#'
#' @return a dataframe containing salary data
pull_dw_salary <- function(fy,
                           dw_conn,
                           opt_job_types) {
  require(tidyverse)

  snap_out <- tbl(dw_conn, "snapshot_annual") %>%
    filter(FY == fy) %>%
    select(PIDM,
           GID,
           NAME,
           HOME_DEPT_DESC,
           HOME_DEPT_L2_COLLUNIT,
           HOME_DEPT_L3_DEPT,
           JOB_TITLE,
           JOB_TYPE,
           FTE_JOB,
           HOURLY_RATE,
           PAYPERIOD_RATE,
           SALARY,
           PAYPERIODS_N,
           AY_FY) %>%
    collect() %>%
    arrange(HOME_DEPT_L2_COLLUNIT,
            HOME_DEPT_L3_DEPT,
            JOB_TYPE,
            JOB_TITLE,
            NAME)

  if (!missing(opt_job_types)) {
    snap_out <- snap_out %>%
      filter(JOB_TYPE %in% opt_job_types)
  }


  return(snap_out)
}


build_degree_hist_tbl <- function(dw_conn, bann_conn, max_degree_year, write_to_dw = F) {

  require(lubridate)
  require(tidyverse)

  if (missing(bann_conn)) {bann_conn <- opa::get_banner_conn(verbose = F)}

  raw_degr_data <- pull_raw_degree_data(bann_conn)

  #fix degree dates outside of expected range
  raw_degr_data <- mutate(raw_degr_data,
                          DEGR_DATE = case_when(year(DEGR_DATE) > max_degree_year ~ as.POSIXct(NA),
                                                year(DEGR_DATE) < 1900 ~ as.POSIXct(NA),
                                                T ~ DEGR_DATE))

  #raw_degr_data is in a long one row per degree format.
  #convert this into a wide format
  degree_summary_wide <- make_degr_crosstbl(raw_degr_data, bann_conn)
  #saveRDS(degree_summary_wide, "temp_backup.RDS")

  degree_summary_wide <- degree_summary_wide %>%
    dplyr::mutate_if(any_column_empty, replace_empty_NA) #See support.R file for these functions

  # degree_summary_current_ee <- filter(degree_summary_wide,
  #                                     PIDM %in% df$PIDM)
  # raw_degr_current_ee <- filter(raw_degr_data, PIDM %in% df$PIDM)
  #
  degree_summary <- calc_highest_degree(degree_summary_wide)

  list_out <- list("degree_history_wide" = degree_summary_wide,
                   "degree_history_long" = raw_degr_data,
                   "degree_history_summary" = degree_summary)

  if (write_to_dw == T) {
    DBI::dbWriteTable(dw_conn,
                      "degr_history_wide",
                      list_out$degree_history_wide,
                      overwrite = T,
                      append = F)

    DBI::dbWriteTable(dw_conn,
                      "degr_history_long",
                      list_out$degree_history_long,
                      overwrite = T,
                      append = F)

    DBI::dbWriteTable(dw_conn,
                      "degr_history_summary",
                      list_out$degree_history_summary,
                      overwrite = T,
                      append = F)

  }

  return(list_out)
}

make_degr_crosstbl <- function(raw_degree_data,
                               opt_bann_conn) {

  # get a banner connection if not supplied. Used to pull PERAPPT data.
  if (missing(opt_bann_conn)) {
    bann_conn <- opa::get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }

  if(missing(raw_degree_data)) {
    raw_degree_data <- pull_raw_degree_data(bann_conn)
  }

  # Access SQL used to categorize Degree descriptions into degree types
  #
  # TRANSFORM Last(Degr1.DEGREE_DESC) AS LastOfDEGREE_DESC
  # SELECT Degr1.ID, Degr1.LAST_NAME, Degr1.PIDM
  # FROM Degr1
  # GROUP BY Degr1.ID, Degr1.LAST_NAME, Degr1.PIDM
  # PIVOT IIf([DEGREE_DESC] Like "a.b.","BACH", x
  #           IIf([DEGREE_DESC] Like "a.m.","MASTERS", x
  #               IIf([degree_desc] Like "a*","ASSOC", x
  #                   IIf([DEGREE_DESC] Like "b*","BACH", x
  #                       IIf([DEGREE_DESC] Like "c*","Certificate", x
  #                           IIf([DEGREE_DESC] Like "d*","DOCTORAL", x
  #                               IIf([DEGREE_DESC] Like "*.d.*","DOCTORAL",
  #                                   IIf([DEGREE_DESC] Like "e*","SPECIALIST",
  #                                       IIf([DEGREE_DESC] Like "M*","MASTERS",
  #                                           IIf([DEGREE_DESC] Like "R*","RN",[DEGREE_DESC]))))))))));
  #
  #
  #   SELECT
  #     degrees1.ID,
  #     degrees1.LAST_NAME,
  #     degrees1.PIDM,
  #     degrees1.ASSOC,
  #     degrees1.BACH,
  #     degrees1.Certificate,
  #     degrees1.DOCTORAL,
  #     degrees1.MASTERS,
  #     degrees1.[Graduate Certificate],
  #     degrees1.[Undeclared Degree Seeking],
  #     DegrTerm.TERM_DEGR,
  #     degrees2.ASSOC AS AASchool,
  #     degrees2.BACH AS BASchool,
  #      degrees2.Certificate AS CertSchool,
  #      degrees2.DOCTORAL AS DocSchool,
  #      degrees2.MASTERS AS MASchool,
  #      degrees2.[Graduate Certificate] AS GrCertSchool,
  #       degrees2.[Undeclared Degree Seeking] AS DegrSeekSchool,
  #       degrees3.ASSOC AS AADate,
  #       degrees3.BACH AS BADate,
  #       degrees3.Certificate AS CertDate,
  #       degrees3.DOCTORAL AS DocDate,
  #       degrees3.MASTERS AS MADate,
  #       degrees3.[Graduate Certificate] AS GrCertDate,
  #       degrees3.[Undeclared Degree Seeking] AS DegrSeekDate INTO DegreesCrosstab19F

  degree_recipients <- raw_degree_data %>%
    distinct(PIDM)

  degree_terminal_recipients <- raw_degree_data %>%
    filter(TERM_DEGR == "Y") %>%
    distinct(PIDM)
  degree_terminal_nonrecipients  <- raw_degree_data %>%
    filter(TERM_DEGR == "N") %>%
    distinct(PIDM)
  degree_terminal_unknown <- raw_degree_data %>%
    filter(is.na(TERM_DEGR)) %>%
    distinct(PIDM)

  cat(paste("CLASSIFYING Degree type\n"))

  raw_degree_data <- raw_degree_data %>%
    select(PIDM,
           DEGR_DATE,
           ACT_DATE,
           TERM_DEGR,
           DEGREE_DESC,
           UNIV_NAME) %>%
    #categorize each degree description into a 'Type', Type is the variable that is used to pivot wide.
    mutate(degree_type = case_when(DEGREE_DESC == "Diploma" ~ "Unclassified",
                                   DEGREE_DESC == "A.B." | substr(DEGREE_DESC, 1, 1) == "B" ~ "BACH",
                                   (substr(DEGREE_DESC, 1, 1) == "D" | grepl("\\.D\\.", DEGREE_DESC)) ~ "DOCTORAL",
                                   DEGREE_DESC == "A.M" | substr(DEGREE_DESC, 1, 1) == "M" ~ "MASTERS",
                                   substr(DEGREE_DESC, 1, 1) == "A" ~ "ASSOC",
                                   substr(DEGREE_DESC, 1, 1) == "C" |
                                     DEGREE_DESC == "Professional Certificate" |
                                     DEGREE_DESC == "Postmasters Certificate FNP" ~ "Certificate",
                                   substr(DEGREE_DESC, 1, 1) == "E"               ~ "Specialist",
                                   substr(DEGREE_DESC, 1, 1) == "R"               ~ "RN",
                                   DEGREE_DESC == "Graduate Certificate"          ~ "Grad Certificate",
                                   DEGREE_DESC == "Undeclared Degree Seeking"     ~ "Undeclared Degree Seeking",
                                   TRUE ~ "Unclassified")) %>%

    #for each type, create a key containing the type and person
    mutate(max_act_degr_key = paste0(PIDM, degree_type)) %>%
    #filter each person-type key to the max date, this will leave the most recent
    #degree earned for each degree type
    opa::filter_by_max_per_key(key_col_name = max_act_degr_key,
                               col_to_max_name = DEGR_DATE) %>%
    select(-c(max_act_degr_key, to_max_key))

  cat(paste("FILTERING to degree recieving school only\n"))

  # Output unclassified degree description for review
  unclassifed_degree_desc <- raw_degree_data %>%
    filter(degree_type == "Unclassified") %>%
    distinct(DEGREE_DESC)

  cat("\nThe following Degree Descriptions were not classified. \n...Review if they should be associated with a particular degree Type:\n")
  print(unclassifed_degree_desc)

  cat("\nPIVOT WIDE to one row per Employee\n")
  degree_info_wide <- raw_degree_data %>%
    select(PIDM, degree_type, DEGREE_DESC, DEGR_DATE, UNIV_NAME) %>%
    group_by(PIDM, degree_type) %>%
    mutate(DEGR_DATE = as.character(DEGR_DATE )) %>%
    summarize(DESC_SUMMARY = paste0(unique(DEGREE_DESC), collapse = ", "),
              DATE_SUMMARY = paste0(unique(DEGR_DATE), collapse = ", "),
              UNIV_SUMMARY = paste0(unique(UNIV_NAME), collapse = ", ")) %>%
    pivot_wider(id_cols = PIDM,
                names_from = degree_type,
                values_from = c(DESC_SUMMARY, DATE_SUMMARY, UNIV_SUMMARY),
                values_fill = list(DESC_SUMMARY = "", DATE_SUMMARY = "", UNIV_SUMMARY = ""))

  #reorder columns
  cat("JOIN Term Degree boolean \nDROP unneeded columns \nREORDER Columns for readability\n")

  degree_info_wide <- degree_info_wide %>%
    mutate(TERM_DEGR = case_when(PIDM %in% degree_terminal_recipients$PIDM    ~ TRUE,
                                 PIDM %in% degree_terminal_nonrecipients$PIDM ~ FALSE,
                                 T ~ as.logical(NA))) %>%
    select(PIDM,
           TERM_DEGR,
           ends_with("ASSOC"),
           ends_with("BACH"),
           ends_with("MASTERS"),
           ends_with("DOCTORAL"),
           ends_with("_Certificate"),
           ends_with("Specialist"),
           ends_with("Grad Certificate"),
           ends_with("Seeking"),
           ends_with("Unclassified"))

  #Cleanup the field names
  names(degree_info_wide) <- str_remove_all(names(degree_info_wide), "SUMMARY_")
  names(degree_info_wide) <- str_replace_all(names(degree_info_wide), "Undeclared Degree Seeking", "Undeclared")

  #Ensure that the field doesn't exceed Access's max characters per field
  degree_info_wide <- mutate(degree_info_wide,
                             UNIV_Undeclared = if_else(nchar(UNIV_Undeclared) > 255,
                                                       substr(UNIV_Undeclared, 1, 255),
                                                       UNIV_Undeclared))
  degree_info_wide <-  degree_info_wide %>%
    mutate_if(any_column_empty, replace_empty_NA)

  cat("RETURN wide degree info dataset\n  ...\n")
  return(degree_info_wide)

}

pull_raw_degree_data <- function(opt_bann_conn) {

  cat(paste("PULLING Degree History data from Banner",
            "\nSQL Script read from //helene/opa$/icj_dts/sql_files/degree_hist_pull.sql\n"))

  sql_query <- opa::load_sql_qry(file_path = "//helene/opa$/icj_dts/sql_files/",
                                 file_name = "degree_hist_pull.sql")

  # this is the data found in the Degr1 Table in the access queries
  degree_data_raw <- ROracle::dbGetQuery(statement = sql_query,
                                         conn = bann_conn)
  return(degree_data_raw)

}

calc_highest_degree <- function(degr_hist_wide) {
  highest_degree <- degr_hist_wide %>%
    mutate(DEGR_TYPE = case_when(!is.na(DATE_DOCTORAL) ~ "DOCTORAL",
                                 !is.na(DATE_MASTERS) ~ "MASTERS",
                                 !is.na(DATE_BACH) ~ "BACHELORS",
                                 !is.na(DATE_ASSOC) ~ "ASSOC",
                                 TRUE ~ as.character(NA)),
           DEGR_DESC = case_when(DEGR_TYPE == "DOCTORAL" ~ DESC_DOCTORAL,
                                 DEGR_TYPE == "MASTERS" ~ DESC_MASTERS,
                                 DEGR_TYPE == "BACHELORS" ~ DESC_BACH,
                                 DEGR_TYPE == "ASSOC" ~ DESC_ASSOC,
                                 TRUE ~ as.character(NA)),
           DEGR_UNIV = case_when(DEGR_TYPE == "DOCTORAL" ~ UNIV_DOCTORAL,
                                 DEGR_TYPE == "MASTERS" ~ UNIV_MASTERS,
                                 DEGR_TYPE == "BACHELORS" ~ UNIV_BACH,
                                 DEGR_TYPE == "ASSOC" ~ UNIV_ASSOC,
                                 TRUE ~ as.character(NA)),
           DEGR_DATE = case_when(DEGR_TYPE == "DOCTORAL" ~ DATE_DOCTORAL,
                                 DEGR_TYPE == "MASTERS" ~ DATE_MASTERS,
                                 DEGR_TYPE == "BACHELORS" ~ DATE_BACH,
                                 DEGR_TYPE == "ASSOC" ~ DATE_ASSOC,
                                 TRUE ~ as.character(NA))) %>%
    select(PIDM,
           DEGR_TERMINAL = TERM_DEGR,
           starts_with("DEGR")) %>%
    filter(!is.na(DEGR_TYPE))

  #df_out <- left_join(df, highest_degree, by = "PIDM")
  return(highest_degree)
}


build_race_ethnicity_tbl <- function(opt_bann_conn, write_to_dw = F) {

  require(tidyverse)

  # this assumes that the the input dataframe, df, contains a column named 'PIDM'
  # this could be written to accept any PIDM column name... if that's really necessary
  #stopifnot("PIDM" %in% names(df))

  # pull race/ethn data by pidm. Split to lists of 1000 or less
  #df_pidms <- df$PIDM %>% unique()
  #pidm_split <- opa::split_vec_for_sql(df_pidms)


  # get a banner connection if not supplied.
  if (missing(opt_bann_conn)) {
    bann_conn <- opa::get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }

  # pull GORPRAC table rows for each pidm
  # each row is unique to PIDM and Race Code
  # gorprac_rows <- lapply(pidm_split,
  #                        race_pull, # see race_pull function in this file
  #                        bann_conn)

  gorprac_rows <- tbl(bann_conn, "GORPRAC") %>%
    #filter(GORPRAC_PIDM %in% pidm_vec) %>%
    collect()


  # GORPRAC data contains race codes, but not race descriptions.
  # get the lookup between Code and Description
  race_lu <- tbl(bann_conn, "GORRACE") %>%
    select(race_code = GORRACE_RACE_CDE,
           race_desc = GORRACE_DESC) %>%
    collect()

  # the simplified race does not include tribal details
  # convert from a long data structure to a narrow data structure
  race_out <- gorprac_rows %>%
    select(pidm = GORPRAC_PIDM,
           GORPRAC_RACE_CDE) %>%
    mutate(race_code_simp = substr(GORPRAC_RACE_CDE, 1, 1)) %>%
    select(-GORPRAC_RACE_CDE) %>%
    distinct(pidm, race_code_simp, .keep_all = T) %>%
    left_join(race_lu, by = c("race_code_simp" = "race_code")) %>%
    mutate(value = 1) %>%
    pivot_wider(id_cols = pidm,
                names_from = race_desc, values_from = value)

  # the tribal rows use two digits, i.e '5A', '5B', '5C' only examine these rows
  tribe_rows <- filter(gorprac_rows,
                       nchar(GORPRAC_RACE_CDE) == 2) %>%
    select(pidm = GORPRAC_PIDM,
           GORPRAC_RACE_CDE) %>%
    left_join(race_lu, by = c("GORPRAC_RACE_CDE" = "race_code")) %>%
    ungroup() %>%
    group_by(pidm) %>%
    # consolidate the multiple tribes into a single field using a comma separator
    summarize(TRIBE = paste(unique(race_desc), collapse = ", ")) %>%
    mutate(TRIBE = if_else(nchar(TRIBE) == 716,
                           "ALL",
                           TRIBE))

  # ethnicity data contains SPBPERS nationality code and hispanic/latino code...
  ethn_data <- ethn_pull(bann_conn)

  race_out <- left_join(race_out, ethn_data, by = c("pidm" = "SPBPERS_PIDM")) %>%
    left_join(tribe_rows, by = "pidm")

  #these are the original column names from the employee snapshot dataset.
  #RE_INDIAN is definitely not pc, could definitley use an update.
  race_out <- race_out %>%
    mutate(RE_HISP = if_else(SPBPERS_ETHN_CDE == 2,
                             1,
                             as.numeric(NA)),
           RE_WHITE = if_else(!is.na(White),
                              1,
                              as.numeric(NA)),
           RE_BLACK = if_else(!is.na(`Black/African American`),
                              1,
                              as.numeric(NA)),
           RE_ASIAN = if_else(!is.na(Asian),
                              1,
                              as.numeric(NA)),
           RE_INDIAN = if_else(!is.na(`American Indian or Alaska Native`),
                               1,
                               as.numeric(NA)),
           RE_OTHER = if_else(!is.na(`Other - Unknown`),
                              1,
                              as.numeric(NA)),
           RE_NR = if_else(!is.na(`No Response`),
                           1,
                           as.numeric(NA)),
           RE_NATIVEHI = if_else(!is.na(`Native Hawaiian and Other Pacific Islander`),
                                 1,
                                 as.numeric(NA)),
           RE_TRIBE = TRIBE,
           NRALIEN_STATUS = if_else(SPBPERS_CITZ_CODE %in% c("N"),
                                    TRUE,
                                    FALSE)) %>%
    select(pidm, starts_with("RE"), NRALIEN_STATUS) %>%
    distinct()

  #race_out <- filter(race_out, pidm %in% df$PIDM)

  race_out <- calc_ipeds_code(race_out) %>%
    distinct()


  # modify by subsetting for quicker results
  race_out[is.na(race_out$RE_IPEDS_CODE),"RE_IPEDS_CODE"] <- "Unknown"
  race_out[race_out$RE_IPEDS_CODE == "Unknown", "RE_UNKNOWN"] <- TRUE
  race_out <- distinct(race_out)
  race_out <- rename(race_out, PIDM = pidm)

  # this was the traditional dplyr method which is very slow:
  #
  #
  # race_out <- race_out %>%
  #   # if an ipeds code could not be calculated, label as 'Unknown'
  #   mutate(RE_IPEDS_CODE = if_else(is.na(RE_IPEDS_CODE),
  #                                  "Unknown",
  #                                  RE_IPEDS_CODE),
  #          RE_UNKNOWN = if_else(RE_IPEDS_CODE == "Unknown",
  #                               TRUE,
  #                               as.logical(NA))) %>%
  #   distinct()



  if (write_to_dw == T) {
    DBI::dbWriteTable(dw_conn,
                      "race_ethnicity",
                      race_out,
                      overwrite = T,
                      append = F)
  }

  return(race_out)
}
#' Pull the GORPRAC race code table and optionally filter to a vector of pidms
#'
#' @param pidm_vec the optional vector of pidms for which GORPRAC rows will be returned
#' @param bann_conn the banner connection object typically supplied by opa::get_banner_conn()
#'
#' @return a dataframe containing the raw unsummarized GORPRAC rows
race_pull <- function(pidm_vec, bann_conn) {
  require(tidyverse)

  # dbplyr equivalent of
  # SELECT *
  # FROM GORPRAC
  # WHERE GORPRAC_PIDM IN pidm_vec;


  df_out <- tbl(bann_conn, "GORPRAC")
  if (!missing(pidm_vec)) {
    df_out <- filter(df_out, GORPRAC_PIDM %in% pidm_vec)
  }

    df_out <- collect(df_out)
  return(df_out)
}

#' Pull SPBPRES Ethnicity Code and Citizen code data for the input vector of pidms
#'
#' @param pidm_vec the vector of pidms for which SPBPRES data will be returned
#' @param bann_conn the banner connection object typically supplied by opa::get_banner_conn()
#'
#' @return a dataframe containing SPBPERS_PIDM, SPBPERS_ETHN_CDE, and SPBPERS_CITZ_CODE
ethn_pull <- function(bann_conn) {
  require(tidyverse)

  # dbplyr equivalent of
  # SELECT SPBPERS_PIDM, SPBPERS_ETHN_CDE, SPBPERS_CITZ_CODE
  # FROM SPBPERS
  # WHERE SPBPERS_PIDM IN pidm_vec;

  df_out <- tbl(bann_conn, "SPBPERS") %>%
    select(SPBPERS_PIDM, SPBPERS_ETHN_CDE, SPBPERS_CITZ_CODE)  %>%
    #filter(SPBPERS_PIDM %in% pidm_vec) %>%
    collect()

  return(df_out)
}

#' Given a dataframe containing the appropriate Race/ethnicity columns specified
#' by the `join_race_data()` function, calculate the official ipeds
#' race/ethnicity specification.
#'
#' @param df the input dataframe containing the necessary race/ethnicity columns
#'   specified by `join_race_data()` function
#'
#' @return the original dataframe with two additional columns: RE_N_TYPES
#'   specifying the number of race/ethncicities selected by the individual, and RE_IPEDS_CODE specifying the
#' @export
calc_ipeds_code <- function(df) {

  # case_when is an ordered function, i.e. it stops evaluating once a row is
  # true this allows Non-residents and hispanic individuals to be correctly
  # coded and avoid placing them into the 'two-or-more' categories.
  df_out <- df %>%
    rowwise() %>%
    mutate(RE_N_TYPES = sum(!is.na(RE_WHITE), !is.na(RE_BLACK),
                            !is.na(RE_ASIAN), !is.na(RE_INDIAN),
                            !is.na(RE_OTHER), !is.na(RE_NATIVEHI)),
           RE_IPEDS_CODE = case_when(NRALIEN_STATUS == TRUE ~ "NRAlien",
                                     !is.na(RE_HISP) ~ "Hispanic",
                                     RE_N_TYPES > 1 ~ "Multi",
                                     !is.na(RE_WHITE) ~ "White",
                                     !is.na(RE_BLACK) ~ "Black",
                                     !is.na(RE_ASIAN) ~ "Asian",
                                     !is.na(RE_INDIAN) ~ "Indian",
                                     !is.na(RE_OTHER) ~ "Other",
                                     #!is.na(RE_NR) ~ "NoResp",
                                     !is.na(RE_NATIVEHI) ~ "NativeHI",
                                     TRUE ~ "Unknown"))
  return(df_out)
}


write_access_tbl <- function(access_conn,
                             tbl_name,
                             df,
                             overwrite = T) {

  require(data.table)
  require(tidyverse)
  require(DBI)
  require(tictoc)

  # Access will attempt to place all character vectors into varchar(255)
  # datatype columns. This will fail if a character string value
  # exceeding 255 bytes exists in the data frame to be written.
  #
  # Best practice would be to use a more robust databasing system such as
  # PostGreSQL.Alternatively, code an automated method of injecting the proper
  # code into the dbWriteTable Call. See https://adv-r.hadley.nz/metaprogramming.html
  #
  # Instead, we will manually code these column names into the dbWriteTable call below.
  # These column names can be found in the col_names_exceeding vector.
  #

  #check the data frame for too long of strings before attempting to write to the db
  cat("Checking for data exceeding 255 byte length\n")
  expected_cols_exceeding <- "EXTERNAL_BANNER_MODS"
  max_byte_length <- 255
  cols_exceeding_length <- lapply(df,
                                  FUN = function(x) {max(nchar(x, type = "bytes", keepNA = F))} ) %>%
    bind_rows() %>%
    data.table::transpose(keep.names = "col_names") %>%
    filter(V1 > max_byte_length)

  col_names_exceeding <- unique(cols_exceeding_length$col_names)
  #col_names_vec <- paste0(col_names_exceeding, sep = " = 'Memo'", collapse = ", ")
  if(any(!col_names_exceeding %in% expected_cols_exceeding)) {
    missing_cols <- col_names_exceeding[!col_names_exceeding %in% expected_cols_exceeding]
    stop(paste0("Must manually modify this function to set ",
                missing_cols, " to the Memo Field Type"))
  }

  cat(paste0("WRITING ", tbl_name, " to "))
  print(access_conn)
  tic("wrote df")
  if (tbl_name %in% dbListTables(access_conn) & overwrite == T) {
    cat(paste0(tbl_name, " already exists in db. Overwriting. \n"))
    dbRemoveTable(access_conn, tbl_name)
  } else if (tbl_name %in% dbListTables(access_conn) & overwrite == F) {
    stop(paste0(tbl_name, " already exists in db. Set overwrite parameter to TRUE to override this error.\n"))
  }


  if ("EXTERNAL_BANNER_MODS" %in% names(df)) {

    write_success <- dbWriteTable(access_conn, tbl_name, df,
                                  batch_rows = 1, append = FALSE, overwrite = FALSE,
                                  field.types = c("EXTERNAL_BANNER_MODS" = "Memo"))
  } else {
    write_success <- dbWriteTable(access_conn, tbl_name, df,
                                  batch_rows = 1, append = FALSE, overwrite = FALSE)
  }

  toc()

  if (write_success == TRUE) {
    cat("SUCCESS! Wrote table to access\n")
  }

}

any_column_empty <- function(x){
  any(x == "")
}

replace_empty_NA <- function(x){
  if_else(x == "",as.character(NA),x)
}