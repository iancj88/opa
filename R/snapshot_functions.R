#' Calculate the number of unique job keys per PIDM in a data frame
#'
#' Given a data frame with columns representing a unique identifier for a person
#' (PIDM), a unique identifier for each job (job_key), the type of each job
#' (job_type), the position of each job (posn), and a suffix indicating the
#' status of each job (suff), this function calculates the number of unique job
#' keys per PIDM and adds it as a new column to the data frame. It also performs
#' some data validation to ensure that the required columns exist and that
#' certain columns and job types are excluded from the calculation. Any
#' pre-existing "job_count" or "JOB_COUNT" columns in the data frame are dropped
#' before calculating the job counts.
#'
#' @param df A data frame containing columns for PIDM, job_key, job_type, posn,
#'   and suff
#' @param pidm_col_name The name of the PIDM column in the data frame (default:
#'   "pidm")
#' @param job_key_col_name The name of the job_key column in the data frame
#'   (default: "job_key")
#' @param job_type_col_name The name of the job_type column in the data frame
#'   (default: "job_type")
#' @param posn_col_name The name of the posn column in the data frame (default:
#'   "posn")
#' @param suff_col_name The name of the suff column in the data frame (default:
#'   "suff")
#' @param new_col_name The name of the new column to be added to the data frame
#'   with the job counts (default: "job_count")
#'
#' @return A data frame with the same columns as the input data frame, plus a
#'   new column for job counts
#'
#' @importFrom dplyr filter group_by summarize left_join select
#' @importFrom rlang enquo as_name
#' @importFrom tidyverse %>%
#'
#' @examples
#' df <- data.frame(
#'   pidm = c(1, 1, 2, 2, 3, 3),
#'   job_key = c(101, 102, 201, 202, 301, 302),
#'   job_type = c("Type 1", "Type 2", "Type 1", "Type 2", "Type 1", "Type 2"),
#'   posn = c("Posn 1", "Posn 2", "Posn 1", "Posn 2", "Posn 1", "Posn 2"),
#'   suff = c("Suff 1", "Suff 2", "Suff 1", "Suff 2", "Suff 1", "Suff 2")
#' )
#'
#' calc_job_counts(df)
#'
#' @export
calc_job_counts <- function(df,
                            pidm_col_name     = pidm,
                            job_key_col_name  = job_key,
                            job_type_col_name = job_type,
                            posn_col_name     = posn,
                            suff_col_name     = suff,
                            new_col_name      = job_count) {


  # load necessary libraries
  require(tidyverse)
  require(rlang)

  # enquote the column names
  pidm_enq     <- enquo(pidm_col_name)
  job_key_enq  <- enquo(job_key_col_name)
  job_type_enq <- enquo(job_type_col_name)
  posn_col_enq <- enquo(posn_col_name)
  suff_col_enq <- enquo(suff_col_name)
  new_col_enq  <- enquo(new_col_name)


  # Check if the column exists in the data frame
  if (!as_name(pidm_enq) %in% names(df)) {
    stop("Column pidm_col_name does not exist in the data frame.")
  }

  if (!as_name(job_key_enq) %in% names(df)) {
    stop("Column job_key_col_name does not exist in the data frame.")
  }

  if (!as_name(job_type_enq) %in% names(df)) {
    stop("Column job_type_col_name does not exist in the data frame.")
  }

  if (!as_name(posn_col_enq) %in% names(df)) {
    stop("Column posn_col_name does not exist in the data frame.")
  }

  if (!as_name(suff_col_enq) %in% names(df)) {
    stop("Column suff_col_name does not exist in the data frame.")
  }

  # Drop the job_count columns if they already exist
  if ("job_count" %in% names(df)) {df <- select(df, -job_count)}
  if ("JOB_COUNT" %in% names(df)) {df <- select(df, -JOB_COUNT)}

  # filter data frame to exclude certain adcomp job types, positions, and suffixes
  # count the remaining jobs per pidm
  # summarize those counts into a new variable, new_col_enq
  df_job_counts <- filter(df,
                          !!job_type_enq != "Additional Compensation (All Forms)",
                          !(!!suff_col_enq %in% c("SD", "SC", "SE", "GP", "OT", "OL", "TR", "TF", "TL", "AX")),
                          !(!!posn_col_enq %in% c("4ADCMP", "4IFPRS", "4ONEPY", "4TERMS"))) %>%
    group_by(!!pidm_enq) %>%
    summarize(!!new_col_enq := n_distinct(!!job_key_enq))

  # Join the calculated job counts to the original data frame
  df_out <- left_join(df, df_job_counts, by = as_name(pidm_enq))

  return(df_out)
}


#' Calculate full-time/part-time status based on FTE
#'
#' This function calculates full-time/part-time status for each row in a given
#' data frame, based on the value of a column containing the full-time
#' equivalent (FTE) for each position. The function returns the original data
#' frame with additional columns indicating whether the FTE is >= 1.0, >= 0.80,
#' or >= 0.50.
#'
#' @param df A data frame containing columns with position identifiers, job
#'   types, suffixes, and FTEs
#' @param pidm_col_name The name of the column containing position identifiers
#'   (default: pidm)
#' @param job_type_col_name The name of the column containing job types
#'   (default: job_type)
#' @param posn_col_name The name of the column containing position codes
#'   (default: posn)
#' @param suff_col_name The name of the column containing suffixes (default:
#'   suff)
#' @param fte_col_name The name of the column containing FTEs (default: fte)
#'
#' @return A data frame with additional columns indicating full-time/part-time
#'   status based on FTE
#'
#' @importFrom tidyverse select filter group_by summarize left_join mutate
#'   as_name
#' @export
#' @examples
#' join_full_time_part_time(my_df,
#'                          pidm_col_name = PIDM,
#'                          job_type_col_name = JOB_TYPE,
#'                          posn_col_name = POSN,
#'                          suff_col_name = SUFF,
#'                          fte_col_name = FTE)
#'
join_full_time_part_time <- function(df,
                                     pidm_col_name     = pidm,
                                     job_type_col_name = job_type,
                                     posn_col_name     = posn,
                                     suff_col_name     = suff,
                                     fte_col_name      = fte) {
  require(tidyverse)

  # enquote the column names
  pidm_enq     <- enquo(pidm_col_name)
  job_type_enq <- enquo(job_type_col_name)
  posn_col_enq <- enquo(posn_col_name)
  suff_col_enq <- enquo(suff_col_name)
  fte_col_enq  <- enquo(fte_col_name)

  # Drop the fte columns if they already exist
  if ("total_fte" %in% names(df)) {df <- select(df, -total_fte)}
  if ("TOTAL_FTE" %in% names(df)) {df <- select(df, -TOTAL_FTE)}
  if ("is_fulltime_1_00fte" %in% names(df)) {df <- select(df, -is_fulltime_1_00fte)}
  if ("IS_FULLTIME_1_00FTE" %in% names(df)) {df <- select(df, -IS_FULLTIME_1_00FTE)}
  if ("is_fulltime_0_80fte" %in% names(df)) {df <- select(df, -is_fulltime_0_80fte)}
  if ("IS_FULLTIME_0_80FTE" %in% names(df)) {df <- select(df, -IS_FULLTIME_0_80FTE)}
  if ("is_fulltime_0_50fte" %in% names(df)) {df <- select(df, -is_fulltime_0_50fte)}
  if ("IS_FULLTIME_0_50FTE" %in% names(df)) {df <- select(df, -IS_FULLTIME_0_50FTE)}

  tot_fte <- df %>%
    filter(!!job_type_enq != "Additional Compensation (All Forms)",
           !(!!suff_col_enq) %in% c("SD", "SC", "SE", "GP", "OT", "OL", "TR", "TF", "TL", "AX"),
           !(!!posn_col_enq) %in% c("4ADCMP", "4IFPRS", "4ONEPY", "4TERMS")) %>%
    group_by(!!pidm_enq) %>%
    summarize(total_fte = sum(!!fte_col_enq))
  df <- left_join(df, tot_fte, by = as_name(pidm_enq))
  df_out <- df %>%
    mutate(is_fulltime_1_00fte = total_fte >= 1,
           is_fulltime_0_80fte = total_fte >= 0.80,
           is_fulltime_0_50fte = total_fte >= 0.50)

  return(df_out)
}
