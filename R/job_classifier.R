#' Classify Job into aggregated groups
#'
#' Use a combination of Eclass, Suffix, position number, rank, tenure, position
#' title, to group like jobs. Categories include Classified, Professional,
#' Executive, Temporary, Fixed-Term, Faculty NTT, Faculty TT/T and Additional
#' Compensation. Depends on \code{\link{classify_job_detailed}} and
#' \code{\link{classify_job_by_ecls}}.
#'
#' @param df the data frame containing the required variables (PIDM, Position
#'   Number, Suffix, Job Title, Position Title, Eclass, Rank, Tenure) if missing
#'   rank, tenure, or position title, will derive from Banner based on the
#'   supplied PIDM
#' @param pidm_col_name the unquoted name of the column containing PIDM values
#' @param posn_col_name the unquoted name of the column containing Position Number values
#' @param suff_col_name the unquoted name of the column containing Suffix values
#' @param jobtitle_col_name the unquoted name of the column containing Job Title values
#' @param posntitle_col_name the unquoted name of the column containing Position Title values
#' @param date_col_name the column containing the as-of date used for pulling historical rank/tenure info
#' @param opt_rank_records all rank records from PERRANK table use opa::get_rank_records()
#' @param opt_tenure_records all tenure records from banner use opa::get_tenure_records()
#' @param opt_bann_conn an optional banner connection object
#'
#' @return the original df with an additional column 'job_type' containing the new data
#' @export
#' @author Ian C Johnson
#' @seealso classify_job_detailed classify_job_by_ecls
#'
classify_job <- function(df,
                         pidm_col_name = PIDM,
                         posn_col_name = POSN,
                         suff_col_name = SUFF,
                         ecls_col_name = ECLS_JOBS,
                         jobtitle_col_name = JOB_TITLE,
                         posntitle_col_name = POSITION_TITLE,
                         date_col_name = date,
                         opt_rank_records,
                         opt_tenure_records,
                         opt_bann_conn) {


  # TODO: rewrite this function to include more appropriate input parameters
  #
  # check input params ------------------------------------------------------
  suppressPackageStartupMessages({
    require(dplyr)
    require(tictoc)
  })

  # get a banner connection if not supplied. Used to pull PERAPPT data.
  if (missing(opt_bann_conn)) {
    bann_conn <- opa::get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }


  # properly enquote inputs -------------------------------------------------

  pidm_col_enquo <- enquo(pidm_col_name)
  posn_col_enquo <- enquo(posn_col_name)
  suff_col_enquo <- enquo(suff_col_name)
  jobtitle_col_enquo <- enquo(jobtitle_col_name)
  posntitle_col_enquo <- enquo(posntitle_col_name)
  ecls_col_enquo <- enquo(ecls_col_name)

  #if as of date was supplied, look for it in the dataframe
  as_of_col_enquo <- enquo(date_col_name)


  # check for missing columns - pull position title if needed ---------------

  if (!quo_name(as_of_col_enquo) %in% names(df)) {
    stop(
      paste0("'",
        quo_name(as_of_col_enquo),
        "' column not found in dataframe supplied to classify_job function"))
  }
  if (!quo_name(pidm_col_enquo) %in% names(df)) {
    stop(
      paste0(
        quo_name(pidm_col_enquo),
        " not found in dataframe supplied to classify_job function"))
  }
  if (!quo_name(posn_col_enquo) %in% names(df)) {
    stop(
      paste0(
        quo_name(posn_col_enquo),
        " not found in dataframe supplied to classify_job function"))
  }
  if (!quo_name(suff_col_enquo) %in% names(df)) {
    stop(
      paste0(
        quo_name(suff_col_enquo),
        " not found in dataframe supplied to classify_job function"))
  }
  if (!quo_name(jobtitle_col_enquo) %in% names(df)) {
    stop(
      paste0(
        quo_name(jobtitle_col_enquo),
        " not found in dataframe supplied to classify_job function"))
  }
  if (!quo_name(ecls_col_enquo) %in% names(df)) {
    stop(
      paste0(
        quo_name(ecls_col_enquo),
        " not found in dataframe supplied to classify_job function"))
  }
  if (!quo_name(posntitle_col_enquo) %in% names(df)) {
    paste0("could not find position title column, pulling from Banner")
    posn_splt <- opa::split_vec_for_sql(df[,quo_name(posn_col_enquo)])
    df_posn <- lapply(posn_splt, pull_position_title, bann_conn = bann_conn)
    df_posn <- bind_rows(df_posn)
    df_posn <- dplyr::rename(df_posn,
                             !!posn_col_enquo := NBBPOSN_POSN)

    df <- left_join(df,
                    df_posn,
                    by = quo_name(posn_col_enquo))
  }

  # join rank, tenure records -----------------------------------------------
  if (!"RANK_CODE_AGG" %in% names(df)) {
    if (missing(opt_rank_records)) {
      tic("Pull Rank from Banner")
      rank_records <- opa::get_rank_records(return_most_recent = F,
                                               opt_bann_conn = bann_conn)
      toc()
    } else {
      rank_records <- opt_rank_records
    }

    tic("Process rank records")
    rank_records <- select(rank_records,
                           !!pidm_col_enquo := pidm,
                           rank_code_agg,
                           rank_code,
                           rank_code_desc,
                           rank_begin_date) %>%
      mutate(rank_begin_date = as.POSIXct(rank_begin_date)) %>%
      #arrange by date and pidm is critical as it allows the group_by and mutate
      #functions to determine the pidm's next begin date
      arrange(!!pidm_col_enquo, rank_begin_date)

    rank_records <- rank_records %>%
      group_by(!!pidm_col_enquo) %>%
      mutate(next_rank_date = lead(rank_begin_date, 1),
             next_rank_date = if_else(is.na(next_rank_date),
                                     as.POSIXct("2100-01-01"), #placeholder value - must be in future
                                     next_rank_date),
             "NBRJOBS_PIDM" = !!pidm_col_enquo)
    #
    # if_else(!!pidm_col_enquo == pidm_next_row & !is.na(pidm_next_row),
    #         lead(rank_begin_date, 1),
    #         as.POSIXct("2100-01-01")))

    df <- rename(df,
                 "NBRJOBS_PIDM" = !!pidm_col_enquo,
                 "as_of_date" = !!as_of_col_enquo)

    #speed up the function by only running the fuzzy pidm/date join on records
    #with rank records.
    df_with_rank <- filter(df,
                           NBRJOBS_PIDM %in%
                             unlist(rank_records[,quo_name(pidm_col_enquo)]))

    df_no_rank <- filter(df,
                         !(NBRJOBS_PIDM %in%
                             unlist(rank_records[,quo_name(pidm_col_enquo)])))


    tic("fuzzy joining rank with job records ... ")

      df_with_rank <- fuzzyjoin::fuzzy_left_join(x = df_with_rank,
                                                 y = rank_records,
                                                 by =  c("NBRJOBS_PIDM" = "NBRJOBS_PIDM",
                                                         "as_of_date" = "rank_begin_date",
                                                         "as_of_date" = "next_rank_date"),
                                                 match_fun = list(`==`, `>=`, `<`))
    toc()

    df_with_rank <- select(df_with_rank,
                           -NBRJOBS_PIDM.y) %>%
      select(NBRJOBS_PIDM = NBRJOBS_PIDM.x,
             everything())

    df <- bind_rows(df_with_rank, df_no_rank)
    toc()
  }

  if (!"TENURE_CODE" %in% names(df)) {
    if (missing(opt_tenure_records)) {
      tic("Pull Tenure records from Banner")
      tenure_records <- opa::get_tenure_status(return_most_recent = F,
                                                  opt_bann_conn = bann_conn)
      toc()
    } else {
      tenure_records <- opt_tenure_records
    }

    tic("Process tenure records")

    tenure_records <- select(tenure_records,
                             !!pidm_col_enquo := pidm,
                             tenure_code,
                             tenure_eff_date,
                             perappt_eff_flr)

    saveRDS(df, "./tst_df_withRankData.RDS")
    saveRDS(df, "./tst_tenure_records.RDS")

    tenure_records <- tenure_records %>%
      group_by(!!pidm_col_enquo) %>%
      mutate(next_tenure_date = lead(perappt_eff_flr, 1),
             next_tenure_date = if_else(is.na(next_tenure_date),
                                      as.POSIXct("2100-01-01"),
                                      next_tenure_date))

    df_with_tenure <- filter(df, NBRJOBS_PIDM %in%
                               unlist(tenure_records[,quo_name(pidm_col_enquo)]))

    df_no_tenure <- filter(df, !(NBRJOBS_PIDM %in%
                                   unlist(tenure_records[,quo_name(pidm_col_enquo)])))


    # df_with_tenure <- filter(df,
    #                        !!pidm_col_enquo %in% tenure_records$pidm)
    # df_no_tenure <- filter(df,
    #                        !(!!pidm_col_enquo %in% tenure_records$pidm))

    tic("fuzzy joining tenure to job records ...")
    df_with_tenure <- fuzzyjoin::fuzzy_left_join(x = df_with_tenure,
                                                 y = tenure_records,
                                                 by =  c("NBRJOBS_PIDM" = "PIDM",
                                                         "EFF_DATE" = "perappt_eff_flr",
                                                         "EFF_DATE" = "next_tenure_date"),
                                                 match_fun = list(`==`, `>=`, `<`))
    toc()

    df <- bind_rows(df_no_tenure, df_with_tenure)

    toc()
  }


  # use detailed classification function to classify ------------------------
  df_out <- df
  #TODO: pass pidm, posn, suff, title column names to detailed function. Very well may break as is lol!
  df_out <- opa::classify_job_detailed(df_out,
                                  posn_number_col_name = !!posn_col_enquo,
                                  suffix_col_name = !!suff_col_enquo,
                                  posn_title_col_name = !!posntitle_col_enquo,
                                  job_title_col_name = !!jobtitle_col_enquo,
                                  ecls_col_name = !!ecls_col_enquo)

  #df_out <- opa::drop_col(df_out, PIDM)
  df_out <- opa::drop_col(df_out, PIDM.x)
  df_out <- opa::drop_col(df_out, PIDM.y)

  #df_out <- rename(df_out, PIDM = NBRJOBS_PIDM)



  if("as_of_date" %in% names(df_out) & !quo_name(as_of_col_enquo) %in% names(df_out)) {
    df_out <- rename(df_out,
                     !!as_of_col_enquo := as_of_date)
  }

  df_out <- df_out %>%
    arrange(!!pidm_col_enquo,
            !!posn_col_enquo,
            !!suff_col_enquo,
            EFF_DATE)

  return(df_out)
}



#' Classify Job into aggregated categories
#'
#' Group like jobs into common categories such as Classified, Professional,
#' Temporary, Fixed-Term, Faculty TT/T, etc. Supports tidyr quasiquotation. All
#' input parameters default to snapshot names. Uses classify_job_by_ecls as the
#' primary function to assign job-types. Some categories, such as faculty and
#' non-job payments, require additional non-ecls logic found in this function
#'
#' @param df the dataframe containing job rows to be categorized
#' @param posn_number_col_name the unquoted name of the position number column
#' @param suffix_col_name the unquoted name of the suffix number column
#' @param job_title_col_name the unquoted name of the job title column
#' @param posn_title_col_name the unquoted name of the job title column
#' @param ecls_col_name the unquoted name of the ecls column
#' @param rank_col_name the unquoted name of the rank column containing numeric
#'   aggregated ranks (1,2,3,4...)
#' @param eskl_col_name the unquoted name of the column containing e-skill
#' @param new_col_name the unquoted name of the new column to be added
#'
#' @return a dataframe containing an additional column, as specified by the
#'   new_col_name parameter.
#' @author Ian C Johnson
#' @seealso classify_job, classify_job_by_ecls
#' @export
classify_job_detailed <- function(df,
                                  posn_number_col_name = POSN,
                                  suffix_col_name = SUFF,
                                  job_title_col_name = JOB_TITLE,
                                  posn_title_col_name = POSITION_TITLE,
                                  ecls_col_name = ECLS_JOBS,
                                  rank_col_name = RANK_CODE,
                                  tenure_col_name = TENURE_CODE,
                                  new_col_name = job_type) {
  require(tidyverse)

  df_orig <- df

  #given a dataframe df, classify each row as a specific type of employment/job type
  #as definitive classifications are made, the are moved into the df_out dataframe
  #and remmoved from the original df via anti-join.

  # Initialize function and parameters --------------------------------------
  # enquote col names -------------------------------------------------------

  # this is necessary to properly use the dplyr verbs with dynamic column names
  # see a detailed explaination of quosure and programming with dpyr here:
  # https://dplyr.tidyverse.org/articles/programming.html

  posn_col_enquo <- enquo(posn_number_col_name)
  job_title_col_enquo <- enquo(job_title_col_name)
  position_title_col_enquo <- enquo(posn_title_col_name)
  suff_col_enquo <- enquo(suffix_col_name)
  ecls_col_enquo <- enquo(ecls_col_name)
  rank_col_enquo <- enquo(rank_col_name)
  tenure_col_enquo <- enquo(tenure_col_name)
  new_col_enquo <- enquo(new_col_name)



  # Verify columns exist ----------------------------------------------------

  if (!quo_name(posn_col_enquo) %in% names(df)) {
    stop(paste0(quo_name(posn_col_enquo), " not found in dataframe supplied to add_emr_job_type"))
  }
  if (!quo_name(suff_col_enquo) %in% names(df)) {
    stop(paste0(quo_name(suff_col_enquo), " not found in dataframe supplied to add_emr_job_type"))
  }
  if (!quo_name(ecls_col_enquo) %in% names(df)) {
    stop(paste0(quo_name(ecls_col_enquo), " not found in dataframe supplied to add_emr_job_type"))
  }
  if (!quo_name(rank_col_enquo) %in% names(df)) {
    stop(paste0(quo_name(rank_col_enquo), " not found in dataframe supplied to add_emr_job_type"))
  }
  if (!quo_name(tenure_col_enquo) %in% names(df)) {
    stop(paste0(quo_name(tenure_col_enquo), " not found in dataframe supplied to add_emr_job_type"))
  }
  # if (!quo_name(tenure_col_enquo) %in% names(df)) {
  #   stop(paste0(quo_name(tenure_col_enquo), " not found in dataframe supplied to add_emr_job_type"))
  # }
  #

  # Initialize from ECLS ----------------------------------------------------

  # most jobs can be classified by eclass. Faculty and non-job payments are
  # exceptions to this rule
  df <- classify_job_by_ecls(df,
                             ecls_col_name = !!ecls_col_enquo,
                             new_col_name = !!new_col_enquo)
  #debug:
  #df <- mutate(df, key = paste0(PIDM, POSN, SUFF))


  # Additional Comp Rows ----------------------------------------------------
  addcomp_suff <- c("SD", #stipend
                    "SC", #stipend
                    "CR", #car allowance
                    "OT", #overtime
                    "OL", #overload
                    "TF", #temp-foreman
                    "TM", #temp-manager
                    #"LW", #livestock worker? assigned as primary job, not adcomp
                    "TL", #temp-locksmith
                    #"TR", #used for a bunch of different things - temp-refuse ground keeper, temp-replacement, primarily temp positions
                    "RF", #repair foreman
                    #"L3", # used for a temp hourly job, not sure what it means, but not adcomp
                    #"GS", # Grad Stipend -- not an adcomp payment
                    "SE") #stipend (historical, should not be used for new/ongoing job records)
  addcomp_position_numbers <-  c("4ADCMP", #adcomp
                                 "4ONEPY", #one-pay
                                 "4TERMS", #termination payment (leave balances)
                                 "4OEHHD", #4-H one-time payment for teacher program assistance
                                 "4IPFRS") #Incentive Program for Researhers
  addcomp_eclses <- c("FE", "MG", "NP") #Onepay, #adcomp, #??? historical?

  df <- dplyr::mutate(df,
                      !! new_col_enquo := ifelse( (!! suff_col_enquo %in% addcomp_suff             |
                                                     !! posn_col_enquo %in% addcomp_position_numbers |
                                                     !! ecls_col_enquo %in% addcomp_eclses           |
                                                     !! position_title_col_enquo == "Additional Compensation") &
                                                    !! ecls_col_enquo != "1H",
                                                  "Additional Compensation (All Forms)",
                                                  !! new_col_enquo))


  df_out <- filter(df, !!new_col_enquo == "Additional Compensation (All Forms)")

  # Student Rows ------------------------------------------------------------

  # identify and remove students
  student_suff <- "OC"
  student_ecls <- "1H"

  df <- mutate(df,
               !! new_col_enquo := ifelse(!! suff_col_enquo == student_suff |
                                            !! ecls_col_enquo == student_ecls,
                                          "Student",
                                          !! new_col_enquo))

  df_out_temp <- filter(df, !!new_col_enquo == "Student")
  df_out <- bind_rows(df_out, df_out_temp)

  #identify and remove grads
  df <- mutate(df,
               !! new_col_enquo := ifelse(substr(!! position_title_col_enquo, 1, 3) == "GSA" |
                                            substr(!! job_title_col_enquo, 1, 3) == "GTA" |
                                            substr(!! job_title_col_enquo, 1, 3) == "GRA",
                                          "Grad Asst",
                                          !! new_col_enquo))

  df_out_temp <- filter(df, !!new_col_enquo == "Grad Asst")
  df_out <- bind_rows(df_out, df_out_temp)

  suppressMessages(
    df <- anti_join(df,
                    df_out)
  )


  # Classified Rows ---------------------------------------------------------
  # use the ecls derived job type for classifieds
  df_out_temp <- filter(df, !!new_col_enquo == "Classified")
  df_out <- bind_rows(df_out, df_out_temp)


  # Fixed-Term/Temp Rows ----------------------------------------------------

  df_out_temp <- filter(df, !!new_col_enquo %in% c("Temporary", "Fixed-Term"))
  df_out <- bind_rows(df_out, df_out_temp)


  # Professional Rows -------------------------------------------------------

  df_out_temp <- filter(df, !!new_col_enquo == "Professional")
  df_out <- bind_rows(df_out, df_out_temp)


  # All other non-faculty ---------------------------------------------------

  df_out_temp <- filter(df, !!new_col_enquo %in% c("Retiree", "Executive"))
  df_out <- bind_rows(df_out, df_out_temp)

  suppressMessages(
    df <- anti_join(df,
                    df_out)
  )
  # Faculty Rows-------------------------------------------------------------
  ttt_fac_eclses <- c("FA", "FB", "FF", "FS",
                      "FZ")

  ttt_rank <- c("1","2","3", "E1", "E2")
  ttt_ten_code <- c("PT", "CT")

  ntt_fac_eclses <- c("FH", "FJ", "FL", "FM", "FN", "FP")

  #TODO review these rank codes
  ntt_rank <- c("4", "A4", "A3", "A1", "A2", "R1", "R2", "R3", "6",
                "V1", "V2", "V3", "V4")

  ntt_ten_code <- c("NT","NONTENURE")


  df <- mutate(df,
               !! new_col_enquo := ifelse(!! ecls_col_enquo %in% ntt_fac_eclses |
                                            !! rank_col_enquo %in% ntt_rank |
                                            !! tenure_col_enquo %in% ntt_ten_code,
                                          "Faculty NTT",
                                          !! new_col_enquo),
               # manually assign NTT to those people not already assigned an job
               # type and who's title starts with 'Adj' to capture adjuncts
               !! new_col_enquo := ifelse(is.na(!! new_col_enquo) &
                                            substr(!!job_title_col_enquo, 1, 3) == "Adj",
                                          "Faculty NTT",
                                          !! new_col_enquo))
  df <- mutate(df,
               !! new_col_enquo := ifelse(!! rank_col_enquo %in% ttt_rank &
                                            !(is.na(!! rank_col_enquo)) &
                                            !! tenure_col_enquo %in% c("CT", "PT") &
                                            ! (!! ecls_col_enquo %in% ntt_fac_eclses),
                                          "Faculty TT/T",
                                          !! new_col_enquo))

  # handle missing summer session (FS ecls, 4X% posn)
  df <- mutate(df,
               !! new_col_enquo := ifelse(is.na(!! rank_col_enquo) &
                                            is.na(!! tenure_col_enquo) &
                                            !!ecls_col_enquo == "FS",
                                          "Faculty NTT",
                                          !! new_col_enquo))

  # use the eclass for all remaining na records
  df <- mutate(df,
               !! new_col_enquo := ifelse(is.na(!! new_col_enquo) &
                                            !! ecls_col_enquo %in% ttt_fac_eclses,
                                          "Faculty TT/T",
                                          !! new_col_enquo))
  df <- mutate(df,
               !! new_col_enquo := ifelse(is.na(!! new_col_enquo) &
                                            !! ecls_col_enquo %in% ntt_fac_eclses,
                                          "Faculty NTT",
                                          !! new_col_enquo))

  df <- mutate(df,
               !! new_col_enquo := ifelse(substr(!! posn_col_enquo,1,2) == "4X",
                                          "Summer Session",
                                          !! new_col_enquo))

  df_out_temp <- df
  df_out <- bind_rows(df_out, df_out_temp)

  return(df_out)
}



#' classify the type of job based on it's eclass
#'
#' using the elcass, determine the type of job. Does not handle situations where
#' the job type is not properly contained in th eclass. examples includes NTT vs
#' TT/T and some types of non-jobpayment.
#'
#' @param df the dataframe containing the eclass and to which the new column
#'   will be appended
#' @param ecls_col_name the unquoted name of the eclass column
#' @param new_col_name the unquoted name of the new column to be added
#'
#' @return the original datafram with a 'job-type' column added
#' @export
classify_job_by_ecls <- function(df,
                                 ecls_col_name,
                                 new_col_name = job_type) {

  ecls_col_enquo <- enquo(ecls_col_name)
  new_col_enquo <- enquo(new_col_name)

  if (!quo_name(ecls_col_enquo) %in% names(df)) {
    stop(
      paste0(
        quo_name(ecls_col_enquo),
        " not found in dataframe supplied to classify_job_by_ecls function"))
  }

  if (quo_name(new_col_enquo) %in% names(df)) {
    warning(
      paste0("new column name, ",
             quo_name(new_col_enquo),
             ", already exists in dataframe. Overwriting column."))
  }

  # clear any data that currently exists in the column storing the data
  df <- mutate(df,
               !! new_col_enquo := NA)


  # Classified Jobs ---------------------------------------------------------

  classified_eclses <- c("HF", "HP", "HV", "SE", "SF",
                         "SN", "SP", "SY")
  df <- mutate(df,
               !! new_col_enquo := ifelse(!! ecls_col_enquo %in% classified_eclses,
                                          "Classified",
                                          !! new_col_enquo))

  # Fixed Term Jobs ---------------------------------------------------------


  fixed_term_eclses <- c("TM", "TS")
  df <- mutate(df,
               !! new_col_enquo := ifelse(!! ecls_col_enquo %in% fixed_term_eclses,
                                          "Fixed-Term",
                                          !! new_col_enquo))


  # Executive Jobs ----------------------------------------------------------


  executive_ecls <- c("EX")
  df <- mutate(df,
               !! new_col_enquo := ifelse(!! ecls_col_enquo %in% executive_ecls,
                                          "Executive",
                                          !! new_col_enquo))

  # Student and Grad Jobs ---------------------------------------------------


  student_eclses <- c("1H")
  grad_eclses <- c("1S")

  df <- mutate(df,
               !! new_col_enquo := ifelse(!! ecls_col_enquo %in% student_eclses,
                                          "Student",
                                          !! new_col_enquo))
  df <- mutate(df,
               !! new_col_enquo := ifelse(!! ecls_col_enquo %in% grad_eclses,
                                          "Grad Asst",
                                          !! new_col_enquo))


  # Temp Jobs ---------------------------------------------------------------


  temp_eclses <- c("TH", "PH")
  df <- mutate(df,
               !! new_col_enquo := ifelse(!! ecls_col_enquo %in% temp_eclses,
                                          "Temporary",
                                          !! new_col_enquo))

  # Retiree Jobs ------------------------------------------------------------


  retiree_ecls <- c("RE")
  df <- mutate(df,
               !! new_col_enquo := ifelse(!! ecls_col_enquo %in% retiree_ecls,
                                          "Retiree",
                                          !! new_col_enquo))

  # Professional Jobs -------------------------------------------------------

  pro_eclses <- c("AF", "AP", "HC", "PF",
                  "PP", "PS", "PY", "PZ", "PX")
  df <- mutate(df,
               !! new_col_enquo := ifelse(!! ecls_col_enquo %in% pro_eclses,
                                          "Professional",
                                          !! new_col_enquo))


  # TT/T Faculty Jobs -------------------------------------------------------


  ttt_fac_eclses <- c("FA", "FB", "FF", "FS",
                      "FZ")
  df <- mutate(df,
               !! new_col_enquo := ifelse(!! ecls_col_enquo %in% ttt_fac_eclses,
                                          "Faculty TT/T",
                                          !! new_col_enquo))


  # NTT Faculty Jobs --------------------------------------------------------

  ntt_fac_eclses <- c("FH", "FJ", "FL", "FM", "FN", "FP")

  df <- mutate(df,
               !! new_col_enquo := ifelse(!! ecls_col_enquo %in% ntt_fac_eclses,
                                          "Faculty NTT",
                                          !! new_col_enquo))


  # Additional Compensation (All forms) records --------------------------------

  nonjob_eclses <- c("FE", "MG", "NP")
  df <- mutate(df,
               !! new_col_enquo := ifelse(!! ecls_col_enquo %in% nonjob_eclses,
                                          "Additional Compensation (All Forms)",
                                          !! new_col_enquo))
}


