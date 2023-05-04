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
get_sex_gender <- function(pidm_vec,
                           opt_bann_conn) {

  # set to UTC time zone to ensure compatibility with Banner dates
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  # get a one-time use banner connection if one is not supplied as an input
  # parameter
  if (missing(opt_bann_conn)) {
    bnr_conn <- opa::get_banner_conn()
  } else {
    bnr_conn <- opt_bann_conn
  }

  # max length of 1000 items in roracle query.
  # requires use of opa::split_vec_for_sql() function
  if (length(pidm_vec) > 1000) {
    stop("pidm_vec exceeds 1000 items.\nUse split_vec_for_sql function prior to calling get_gender function")
  }
  tic("Pulled Sex/Gender")
  gender_out <- dplyr::tbl(bnr_conn, "SPBPERS")
  gender_out <- dplyr::select(gender_out,
                              pidm = SPBPERS_PIDM,
                              sex = SPBPERS_SEX,
                              gender = SPBPERS_GNDR_CODE,
                              pronoun = SPBPERS_PPRN_CODE,
                              activity_date = SPBPERS_ACTIVITY_DATE)
  gender_out <- dplyr::filter(gender_out,
                              pidm %in% pidm_vec) %>%
    group_by(pidm) %>%
    filter(activity_date == max(activity_date))
  gender_out <- dplyr::collect(gender_out)

  toc()
  return(gender_out)
}

#' Pull Race/Ethnicity data for a population defined by the unique PIDMs in the
#' input dfataframe
#'
#' @param df The input dataframe. Must contain a column named 'PIDM' which
#'   defines the individuals for which race/ethncicity will be pulled.
#' @param opt_bann_conn An optional banner connection object supplied by `opa::get_banner_conn()`
#'
#' @return the original dataframe, df, with the following additional columns:
#'   \itemize{"RE_HISP", "RE_WHITE", "RE_BLACK", "RE_ASIAN", "RE_INDIAN",
#'            "RE_OTHER", "RE_NR", "RE_NATIVEHI","RE_TRIBE", "NRALIEN_STATUS"
#'            "RE_N_TYPES", "RE_IPEDS_CODE"}
#' @export
join_race_data <- function(df,
                           opt_bann_conn) {

  require(tidyverse)

  # this assumes that the the input dataframe, df, contains a column named 'PIDM'
  # this could be written to accept any PIDM column name... if that's really necessary
  stopifnot("PIDM" %in% names(df))

  # pull race/ethn data by pidm. Split to lists of 1000 or less
  df_pidms <- df$PIDM %>% unique()
  pidm_split <- opa::split_vec_for_sql(df_pidms)


  # get a banner connection if not supplied.
  if (missing(opt_bann_conn)) {
    bann_conn <- opa::get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }

  # pull GORPRAC table rows for each pidm
  # each row is unique to PIDM and Race Code
  gorprac_rows <- lapply(pidm_split,
                         race_pull, # see race_pull function in this file
                         bann_conn)

  gorprac_rows <- bind_rows(gorprac_rows)

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
    group_by(pidm) %>%
    # consolidate the multiple tribes into a single field using a comma separator
    summarize(TRIBE = paste(unique(race_desc), sep = ", "))

  # ethnicity data contains SPBPERS nationality code and hispanic/latino code...
  ethn_data <- lapply(pidm_split,
                      ethn_pull,
                      bann_conn) %>%
    bind_rows()

  race_out <- left_join(race_out, ethn_data, by = c("pidm" = "SPBPERS_PIDM")) %>%
    left_join(tribe_rows, by = "pidm")

  mandatory_columns <- c("White", "Black/African American", "Asian",
                         "American Indian or Alaska Native",
                         "Other - Unknown",
                         "No Response",
                         "Native Hawaiian and Other Pacific Islander")
  race_out[mandatory_columns[!(mandatory_columns %in% colnames(race_out))]] = as.numeric(NA)

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



  race_out <- filter(race_out, pidm %in% df$PIDM)

  race_out <- calc_ipeds_code(race_out) %>%
    distinct()


  df_out <- df %>%
    # if the columns are already in the original df, drop them
    select(-starts_with("RE_", ignore.case = F),
           -starts_with("NRALIEN")) %>%
    left_join(race_out, by = c("PIDM" = "pidm")) %>%
    # if an ipeds code could not be calculated, label as 'Unknown'
    mutate(RE_IPEDS_CODE = if_else(is.na(RE_IPEDS_CODE),
                                   "Unknown",
                                   RE_IPEDS_CODE),
           RE_UNKNOWN = if_else(RE_IPEDS_CODE == "Unknown",
                                1,
                                as.numeric(NA))) %>%
    distinct()

  return(df_out)
}

#' Pull GORPRAC race code table rows for the input vector of pidms
#'
#' @param pidm_vec the vector of pidms for which GORPRAC rows will be returned
#' @param bann_conn the banner connection object typically supplied by opa::get_banner_conn()
#'
#' @return a dataframe containing the raw unsummarized GORPRAC rows
race_pull <- function(pidm_vec, bann_conn) {
  require(tidyverse)

  # dbplyr equivalent of
  # SELECT *
  # FROM GORPRAC
  # WHERE GORPRAC_PIDM IN pidm_vec;

  df_out <- tbl(bann_conn, "GORPRAC") %>%
    filter(GORPRAC_PIDM %in% pidm_vec) %>%
    collect()
  return(df_out)
}

#' Pull SPBPRES Ethnicity Code and Citizen code data for the input vector of pidms
#'
#' @param pidm_vec the vector of pidms for which SPBPRES data will be returned
#' @param bann_conn the banner connection object typically supplied by opa::get_banner_conn()
#'
#' @return a dataframe containing SPBPERS_PIDM, SPBPERS_ETHN_CDE, and SPBPERS_CITZ_CODE
ethn_pull <- function(pidm_vec, bann_conn) {
  require(tidyverse)

  # dbplyr equivalent of
  # SELECT SPBPERS_PIDM, SPBPERS_ETHN_CDE, SPBPERS_CITZ_CODE
  # FROM SPBPERS
  # WHERE SPBPERS_PIDM IN pidm_vec;

  df_out <- tbl(bann_conn, "SPBPERS") %>%
    select(SPBPERS_PIDM, SPBPERS_ETHN_CDE, SPBPERS_CITZ_CODE)  %>%
    filter(SPBPERS_PIDM %in% pidm_vec) %>%
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
#'
#' @examples
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
