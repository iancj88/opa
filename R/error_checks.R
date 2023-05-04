#' Error check Primary Job designation
#'
#' Flag job data for having no Primary designated rows, or for having multiple
#' designated rows.
#'
#' @param df the dataframe containing job data including the job key and primary
#'   job indicator
#' @param person_key_col the unquoted column name containing the individual
#'   person key, typically PIDM or GID
#' @param job_key_col the unquoted col name containing the job key i.e.
#'   concatenation of PIDM, POSN, SUFF
#' @param primary_col the unquoted col name containing the primary job indicator
#'   "P"
#' @param write_out_df a boolean indicating if the flagged employees' records
#'   should be output to xlsx
#' @param write_out_fpath the filepath to which the erroneous records will be
#'   writen
#'
#' @return the input dataframe with two new columns: `has_mult_primary` and
#'   `has_no_primary` each containing boolean flags for erroneous job_keys
#' @export
#'
#' @examples
#'   df <- data.frame("job_key" = c("A", "A", "A", "B", "B", "C", "C"),
#'                    "PRIMARY" = c("P", "P", "S", "P", "S", "S", "S"))
#'   flag_bad_primary_jobs(df)
flag_bad_primary_jobs <- function(df,
                                  person_key_col = PIDM,
                                  job_key_col = JOB_KEY,
                                  primary_col = PRIMARY,
                                  write_out_df = T,
                                  write_out_fpath = "./") {

  require(tidyverse)

  primary_enquo    <- enquo(primary_col)
  job_key_enquo    <- enquo(job_key_col)
  person_key_enquo <- enquo(person_key_col)

  stopifnot(quo_name(primary_enquo) %in% names(df))
  stopifnot(quo_name(person_key_enquo) %in% names(df))
  stopifnot(quo_name(job_key_enquo) %in% names(df))

  primary_cnts_per_key <- df %>%
    distinct(!!person_key_enquo,
             !!job_key_enquo,
             !!primary_enquo) %>%
    group_by(!!person_key_enquo) %>%
    filter(!grepl("GP$", !!job_key_enquo),
           !grepl("4IPFRS", !!job_key_enquo),
           !grepl("SD$", !!job_key_enquo),
           !grepl("4ADCMP", !!job_key_enquo)) %>%
    summarize(n_primary = sum(!!primary_enquo == "P"))

  job_cnts_per_key <- df %>%
    distinct(!!person_key_enquo,
             !!job_key_enquo,
             !!primary_enquo) %>%
    group_by(!!job_key_enquo) %>%
    filter(!grepl("GP$", !!job_key_enquo),
           !grepl("4IPFRS", !!job_key_enquo),
           !grepl("SD$", !!job_key_enquo),
           !grepl("4ADCMP", !!job_key_enquo)) %>%
    summarize(n_jobs = n_distinct(!!job_key_enquo))

  mult_primary_cnts <- primary_cnts_per_key %>%
    filter(n_primary > 1)
  mult_primary_pidms <- unlist(mult_primary_cnts[,quo_name(person_key_enquo)])

  message(paste0("Identified ", nrow(mult_primary_cnts), " employees with more than one Primary Job"))

  no_primary_cnts <- primary_cnts_per_key %>%
    filter(n_primary == 0)
  no_primary_pidms <- unlist(no_primary_cnts[,quo_name(person_key_enquo)])

  message(paste0("Identified ", nrow(no_primary_cnts), " employees with no Primary Job"))

  df_out <- df %>%
   mutate(has_mult_primary = !!person_key_enquo %in% mult_primary_pidms,
          has_no_primary = !!person_key_enquo %in% no_primary_pidms)

  if (write_out_df == T) {

    df_out_temp <- select(df_out,
                          GID,
                          NAME,
                          POSN,
                          SUFF,
                          DIVISION = HOME_DEPT_DIVISION_L2,
                          HOME_DEPT_DESC,
                          POSITION_TITLE,
                          JOB_TITLE,
                          FTE,
                          HOURLY_RATE,
                          PRIMARY,
                          ECLS_JOBS,
                          ECLS_PEAM,
                          JOB_START_DATE,
                          JOB_END_DATE,
                          JOB_KEY,
                          has_mult_primary,
                          has_no_primary)


    list_out <- list()
    list_out$`Mult Primary`   <- filter(df_out_temp, has_mult_primary == T)
    list_out$`Has No Primary` <- filter(df_out_temp, has_no_primary == T)
    date_chr <- Sys.Date() %>% as.character(format = "%Y%m%d")
    fpath_out <- paste0(write_out_fpath, "primary_job_indicator_errors_", date_chr)

    opa::write_list_report(list_out, fpath_out)
    message(paste0("Wrote primary job error report to ", fpath_out, ".xlsx"))
  }

  return(df_out)
}

#' Identify Employees whose total FTE exceeds a threshold
#'
#' Threshold typically set at 1.0 FTE. Excludes adcomp, summer-session, and
#' OT/OL jobs.
#'
#' @param df the dataframe containing atleast the following necessary columns
#' @param person_key_col the unquoted column name containing the individual
#'   person key, typically PIDM or GID
#' @param job_key_col the unquoted column name containing the individual job key
#' @param suffix_col the unquoted column name containing the Suffix
#' @param fte_col the unquoted column name containing the FTE
#' @param max_fte_threshold the threshold at which employees exceeding the given
#'   value will be flagged
#' @param write_out_df a boolean indicating if the flagged employees' records
#'   should be output to xlsx
#' @param write_out_fpath the filepath to which the erroneous records will be
#'   writen
#'
#' @return a dataframe containing an extra column, `has_excess_fte`, flagging
#'   employee records who exceed the FTE threshold
#' @export
flag_bad_fte_employees <- function(df,
                                   person_key_col = PIDM,
                                   job_key_col = JOB_KEY,
                                   suffix_col = SUFF,
                                   fte_col = FTE,
                                   job_status_col = JOB_STATUS,
                                   max_fte_threshold = 1,
                                   write_out_df = T,
                                   write_out_fpath = "./") {
  require(tidyverse)

  fte_enquo <- enquo(fte_col)
  job_key_enquo <- enquo(job_key_col)
  person_key_enquo <- enquo(person_key_col)
  suff_enquo <- enquo(suffix_col)
  job_status_enquo <- enquo(job_status_col)

  stopifnot(quo_name(fte_enquo) %in% names(df))
  stopifnot(quo_name(person_key_enquo) %in% names(df))
  stopifnot(quo_name(job_key_enquo) %in% names(df))
  stopifnot(quo_name(suff_enquo) %in% names(df))
  stopifnot(quo_name(job_status_enquo) %in% names(df))

  fte_per_person <- df %>%
    distinct(!!person_key_enquo,
             !!job_key_enquo,
             .keep_all = T) %>%
    group_by(!!person_key_enquo) %>%
    filter(! (!!suff_enquo) %in% c("SD", "OL", "TF", "TM", "TL",
                                   "TR", "T3", "CR", "RF", "OT"),
           !!job_status_enquo == "A",
           ! grepl("4[XDS]", !!job_key_enquo),
           ! grepl("4ADCMP", !!job_key_enquo),
           ! grepl("4IPFRS", !!job_key_enquo),) %>%
    summarize(tot_fte = sum(!!fte_enquo)) %>%
    filter(tot_fte > max_fte_threshold)
  fte_exceeds_pidms <- unlist(fte_per_person[,quo_name(person_key_enquo)])
  message(paste0("Identified ", nrow(fte_per_person), " employees with FTE > 1"))

  df_out <- df %>%
    mutate(has_excess_fte = !!person_key_enquo %in% fte_exceeds_pidms) %>%
    left_join(fte_per_person, by = quo_name(person_key_enquo))

  if(write_out_df == T) {
    list_out <- list()
    list_out$`Excess FTE` <- df_out %>%
      select(GID,
             NAME,
             POSN,
             SUFF,
             DIVISION = HOME_DEPT_DIVISION_L2,
             HOME_DEPT_DESC,
             POSITION_TITLE,
             JOB_TITLE,
             FTE,
             TOTAL_FTE = tot_fte,
             HOURLY_RATE,
             PRIMARY,
             ECLS_JOBS,
             ECLS_PEAM,
             JOB_START_DATE,
             JOB_END_DATE,
             JOB_KEY,
             has_excess_fte) %>%
      filter(has_excess_fte == T) %>%
      distinct(JOB_KEY, .keep_all = T) %>%
      arrange(GID, JOB_KEY)
    date_chr <- Sys.Date() %>% as.character(format = "%Y%m%d")

    fpath_out <- paste0(write_out_fpath, "excess_fte_errors_", date_chr)
    opa::write_list_report(list_out, fpath_out)
    message(paste0("Wrote FTE error report to ", fpath_out, ".xlsx"))

  }

  return(df_out)
}

#' Identify jobs with missing JCAT, CUPA, or SOC codes
#'
#' JCAT, CUPA, and SOC codes are expected to be assigned to appropriate
#' job-types such as Classified, Professional, Grad Asst, etc. This function
#' flags the jobs where they are missing. Codes can be downloaded via the
#' Reportweb All EE Report or via the Argos Datamart History Tables
#'
#' @param df the dataframe containing atleast the following necessary columns
#' @param soc_fed_col the unquoted column name containing the SOC FED code
#' @param soc_mus_col the unquoted column name containing the SOC MUS code
#' @param jcat_col the unquoted column name containing the JCAT code
#' @param cupa_col the unquoted column name containing the CUPA code
#' @param job_type_col the unquoted column name containing the job type supplied
#'   by the `opa::classify_job_by_ecls()` function
#' @param ecls_col the unquoted column name containing the ecls code -- only
#'   necessary if the job_type_col has not yet been added to the dataframe
#' @param write_out_df a boolean flag indicating if the flagged rows should be
#'   written out to a xlsx file
#' @param write_out_fpath the filepath to which the missing row xlsx will be
#'   written
#'
#' @return a dataframe containing four new boolean columns: `missing_jcat_code`,
#'   `missing_soc_mus`, `missing_soc_fed`. amd `missing_cupa_code` which flag
#'   missing code values  for the appropriate job-types. If the Job Type column
#'   was not supplied, there will be an additional column, `JOB_TYPE` as defined
#'   by the `opa::classify_job_by_ecls()` function.
#'
#' @export
flag_missing_soc_cupa_jcat <- function(df,
                                       soc_fed_col = SOC_FED_CODE,
                                       soc_mus_col = SOC_MUS,
                                       jcat_col = JCAT_CODE,
                                       cupa_col = CUPA_CODE,
                                       job_type_col = JOB_TYPE,
                                       ecls_col = ECLS_JOBS,
                                       job_key_col = JOB_KEY,
                                       write_out_df = T,
                                       write_out_fpath = "./") {

  soc_fed_enquo <- enquo(soc_fed_col)
  soc_mus_enquo <- enquo(soc_mus_col)
  jcat_enquo <- enquo(jcat_col)
  cupa_enquo <- enquo(cupa_col)
  job_type_enquo <- enquo(job_type_col)
  ecls_enquo <- enquo(ecls_col)
  job_key_enquo <- enquo(job_key_col)

  if(!quo_name(job_type_enquo) %in% names(df)) {
    stopifnot(quo_name(ecls_enquo) %in% names(df))
    df <- opa::classify_job_by_ecls(df, ecls_col_name = !!ecls_enquo, new_col_name = JOB_TYPE)
  }

  stopifnot(quo_name(soc_fed_enquo) %in% names(df))
  stopifnot(quo_name(soc_mus_enquo) %in% names(df))
  stopifnot(quo_name(jcat_enquo) %in% names(df))
  stopifnot(quo_name(cupa_enquo) %in% names(df))
  stopifnot(quo_name(job_type_enquo) %in% names(df))
  stopifnot(quo_name(job_key_enquo) %in% names(df))

  df_out <- df %>%
    filter(!JOB_TYPE %in% c("Additional Compensation (All Forms)", "Grad Asst", "Temporary"),

           !grepl("4[XDS]", !!job_key_enquo),
           !grepl("4ADCMP", !!job_key_enquo)) %>%
    mutate(missing_jcat_code = is.na(!!jcat_enquo),
           missing_soc_mus = JOB_TYPE == "Classified" & is.na(!!soc_mus_enquo),
           missing_soc_fed = JOB_TYPE == "Classified" & is.na(!!soc_fed_enquo),
           missing_cupa_code = !JOB_TYPE %in% c("Faculty NTT", "Faculty TT/T",
                                                "Grad Asst", "Additional Compensation (All Forms)",
                                                "Temporary", "Retiree") & is.na(JOBS_CUPA_CODE))


  if (write_out_df == T) {

    list_out <- list()

    list_out$`Missing JCAT` <- df_out %>%
      filter(missing_jcat_code == T) %>%
      distinct(JOB_KEY, .keep_all = T) %>%
      arrange(PIDM, JOB_KEY)

    list_out$`Missing CUPA` <- df_out %>%
      filter(missing_cupa_code == T) %>%
      distinct(JOB_KEY, .keep_all = T) %>%
      arrange(PIDM, JOB_KEY)

    list_out$`Missing SOC Fed` <- df_out %>%
      filter(missing_soc_fed == T) %>%
      distinct(JOB_KEY, .keep_all = T) %>%
      arrange(PIDM, JOB_KEY)

    list_out$`Missing SOC MUS` <- df_out %>%
      filter(missing_soc_mus == T) %>%
      distinct(JOB_KEY, .keep_all = T) %>%
      arrange(PIDM, JOB_KEY)

    fpath_out <- paste0(write_out_fpath, "missing_jcat_cupa_soc")
    opa::write_list_report(list_out, fpath_out)
    message(paste0("Wrote FTE error report to ", fpath_out, ".xlsx"))
  }

  return(df_out)

}

#' Flag inconsistent ECLS values across Job, Position, and Person
#'
#' Examine NBRJOBS_ECLS_CODE, NBRPOSN_ECLS_CODE, and PEBEMPL_ECLS_CODE values to
#' flag employees who have a discrepancy between all three values. If a
#' discrepancy is found, filter to all of that employees active jobs. See
#'
#' @param df a snapshot datafrme typically supplied by
#'   \code{\link{get_banner_snapshot}}
#' @param person_key_col the unquoted column name containing the person specific
#'   key, typically `PIDM`
#' @param job_key_col the unquoted column name containing the job specific key,
#'   typically `JOB_KEY`, the concatenation of PIDM, Position Number, and Suffix
#' @param ecls_jobs_col the unquoted column name containing the NBRJOBS Eclass,
#'   typically `ECLS_JOBS`
#' @param ecls_peam_col the unquoted column name containing the PEBEMPL Eclass,
#'   typically `ECLS_PEAM`
#' @param ecls_posn_col the unquoted column name containing the NBRPOSN Eclass,
#'   typically `ECLS_POSN`
#' @param single_job_holders_only a boolean specifying if the function should
#'   only return inconsistencies for single job holders
#' @param write_out_df a boolean flag indicating if the flagged rows should be
#'   written out to a xlsx file
#' @param write_out_fpath the filepath to which the missing row xlsx will be
#'   written
#'
#' @return a dataframe containing all jobs for individuals flagged with atleast
#'   one inconsistent job, position, or person eclass. Not all flagged jobs
#'   require modification. For example, person eclass may differ from job eclass
#'   for non-primary jobs.
#' @export
flag_ecls_inconsistencies <- function(df,
                                      person_key_col = PIDM,
                                      job_key_col = JOB_KEY,
                                      ecls_jobs_col = ECLS_JOBS,
                                      ecls_peam_col = ECLS_PEAM,
                                      ecls_posn_col = ECLS_POSN,
                                      single_job_holders_only = T,
                                      write_out_df = T,
                                      write_out_fpath = "./") {

  person_key_enquo <- enquo(person_key_col)
  job_key_enquo <- enquo(job_key_col)
  ecls_jobs_enquo <- enquo(ecls_jobs_col)
  ecls_peam_enquo <- enquo(ecls_peam_col)
  ecls_posn_enquo <- enquo(ecls_posn_col)

  bad_suffixes_regex <- c("SD","TR", "CR") %>% paste0(collapse = "$|") %>%
    paste0("$")

  bad_posns_regex <- c("4IPFRS", "4ADCMP", "4X") %>% paste0(collapse = "|")

  single_job_holders <- df %>%
    filter(!grepl(bad_suffixes_regex, !!job_key_enquo),
           !grepl(bad_posns_regex, !!job_key_enquo)) %>%
    group_by(!!person_key_enquo) %>%
    summarize(n_jobs = n_distinct(!!job_key_enquo)) %>%
    filter(n_jobs == 1)

  single_job_holder_pidms <- unlist(single_job_holders[,quo_name(person_key_enquo)])

  if (single_job_holders_only == T) {

    df <- filter(df,
                 !!person_key_enquo %in% single_job_holder_pidms,
                 !grepl(bad_suffixes_regex, !!job_key_enquo),
                 !grepl(bad_posns_regex, !!job_key_enquo))

  }

  df_out <- df %>%
    mutate(has_jobs_peam_mismatch = (!!ecls_jobs_enquo != !!ecls_peam_enquo) &
             !grepl(bad_suffixes_regex, !!job_key_enquo) &
             !grepl(bad_posns_regex, !!job_key_enquo),
           has_jobs_posn_mismatch = (!!ecls_jobs_enquo != !!ecls_posn_enquo) &
             !grepl(bad_suffixes_regex, !!job_key_enquo) &
             !grepl(bad_posns_regex, !!job_key_enquo),
           has_peam_posn_mismatch = (!!ecls_peam_enquo != !!ecls_posn_enquo) &
             !grepl(bad_suffixes_regex, !!job_key_enquo) &
             !grepl(bad_posns_regex, !!job_key_enquo),
           has_ecls_inconsistency = has_peam_posn_mismatch | has_jobs_posn_mismatch | has_jobs_peam_mismatch)

  if (write_out_df == T) {
    df_out_temp <- df_out %>%
      filter(has_jobs_peam_mismatch == T |
               has_jobs_posn_mismatch == T |
               has_peam_posn_mismatch == T)

    df_out_temp <- df_out %>%
      filter(!!person_key_enquo %in% unlist(df_out_temp[,quo_name(person_key_enquo)])) %>%
      select(GID,
             NAME,
             POSN,
             SUFF,
             DIVISION = HOME_DEPT_DIVISION_L2,
             HOME_DEPT_DESC,
             POSITION_TITLE,
             JOB_TITLE,
             ECLS_JOBS,
             ECLS_POSN,
             ECLS_PEAM,
             PRIMARY,
             FTE,
             PAYNOS_PER_FY = MONTHS,
             JOB_KEY,
             has_ecls_inconsistency,
             has_jobs_peam_mismatch,
             has_jobs_posn_mismatch,
             has_peam_posn_mismatch)



    list_out <- list()

    list_out$`ECLS Inconsistencies` <- df_out_temp %>%
      distinct(JOB_KEY, .keep_all = T) %>%
      arrange(GID, JOB_KEY)

    date_chr <- Sys.Date() %>% as.character(format = "%Y%m%d")

    fpath_out <- paste0(write_out_fpath, "ecls_inconsistencies_", date_chr)
    opa::write_list_report(list_out, fpath_out)
    message(paste0("Wrote FTE error report to ", fpath_out, ".xlsx"))
  }

    return(df_out)
}

flag_faculty_inconsistencies <- function(df) {

  # flag eskl inconsistency
  # flag pgrp inconsistency
  # flag rank, tenure inconsistency
  # flag position title, job title inconsistencies
  # flag pgrp, pcls inconsistencies
  #
}

flag_library_inconsistencies <- function(df) {

}

flag_bad_classified_titles <- function(df) {

}

flag_nonmus_tttfac <- function(df,
                               job_type_col = job_type,
                               mus_contract_col = MUS_CONTRACT) {

  mus_contract_enquo <- enquo(mus_contract_col)
  job_type_enquo     <- enquo(job_type_col)

  df_out <- filter(df,
                   !!job_type_enquo == "Faculty TT/T",
                   !!mus_contract_enquo == "N")
  return(df_out)
}

flag_bad_tttfac_titles <- function(df) {

  good_titles <- c("Professor", "Associate Professor", "Assistant Professor",
                   "Extension Agent", "Department Head", "Extension Faculty",
                   "Extension Specialist", "ES/AES Head")

  df_out <- filter(df,
                   job_type_enquo == "Faculty TT/T",
                   !position_title_enquo %in% good_titles)


}

highlight_na_vals <- function(df) {
  require(visdat)
  visdat::vis_miss(df)

}
