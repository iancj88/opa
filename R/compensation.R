#' Calculate Longevity Salary Bonus Percent
#'
#' Given a column containing current hire date and an as_of date, calculate the
#' appropriate percent bonus
#'
#' @param df the dataframe containing employee records
#' @param curr_hire_col the column containing current hire dates as POSIXct
#' @param as_of_date_col the column containing the as_of_date as POSIXct
#' @param job_group_col the column containing the row type, i.e. classified,
#'   proessional, faculty, etc. see msupa::classify_job_detailed
#'
#' @return a dataframe with an additional longevity_perc_bonus column
#' @export
add_longevity_perc <- function(df,
                               curr_hire_col = CURRENT_HIRE_DATE,
                               as_of_date_col = date,
                               job_group_col) {

  #check that the specified input parameter columns exist in the supplied
  #dataframe
  curr_hire_enquo <- enquo(curr_hire_col)
  as_of_date_enquo <- enquo(as_of_date_col)
  job_group_enquo <- enquo(job_group_col)

  if (!quo_name(curr_hire_enquo) %in% names(df)) {
    stop(paste0(quo_name(curr_hire_enquo), " not found in dataframe supplied to calc_longevity_perc"))
  }
  if (!quo_name(as_of_date_enquo) %in% names(df)) {
    stop(paste0(quo_name(as_of_date_enquo), " not found in dataframe supplied to calc_longevity_perc"))
  }
  if (!quo_name(job_group_enquo) %in% names(df)) {
    stop(paste0(quo_name(job_group_enquo), " not found in dataframe supplied to calc_longevity_perc"))
  }

  # default set to UTC when pulling dates from banner. This sets the environment
  # so that the DBI package properly handles date conversions.
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  long_rates <- msuopa::longevity_rates

  df_out <- mutate(df,
                   curr_hire_flr = lubridate::floor_date(!!curr_hire_enquo,
                                                         unit = "months"),
                   long_interv = lubridate::interval(curr_hire_flr, !!as_of_date_enquo),
                   long_years = floor(long_interv / lubridate::dyears(x = 1))) %>%
    left_join(msuopa::longevity_rates,
              by = c("long_years" = "YearsOfService")) %>%
    rename(long_percent = PercentToBase,
           longevity_years = long_years) %>%
    select(-long_interv, -curr_hire_flr) %>%
    mutate(longevity_perc_bonus := if_else(!!job_group_enquo == "Classified",
                                           long_percent,
                                           0)) %>%
    select(-long_percent)

  return(df_out)
}

#' Add a job's FLSA exemption status to a dataframe
#'
#' Using a choosen Ecls column, map elcs to FLSA overtime exemption status.
#' Depends on the `msuopa::ecls_flsa_exmpt_tbl` dataframe being accessible to
#' the package
#'
#' @param df the dataframe containing the ecls column and to which the new
#'   column will be added.
#' @param ecls_col_name The string vector containing the name of the eclass
#'   column to which the FLSA status will be mapped.
#'
#' @return The original dataframe with a new column named \code{FLSA OT Exempt}.
#' @export
#'
add_flsa_exmpt_status <- function(df, ecls_col_name) {
  stopifnot(ecls_col_name %in% names(df))
  df_out <- df
  df_out$ThisIsAnAbsurdColumnNamePlaceHolder123459 <- df_out[,ecls_col_name]
  #TODO: convert to properly enquoted ecls col name
  #TODO: move flsa exemption table to opa package
  df_out <- dplyr::left_join(df_out,
                             msuopa::ecls_flsa_exmpt_tbl,
                             by = c("ThisIsAnAbsurdColumnNamePlaceHolder123459" = "Ecls Code"))
  df_out <- dplyr::select(df_out,
                          -ThisIsAnAbsurdColumnNamePlaceHolder123459)

  return(df_out)
}

#' Compile total amount paid via adcomp and stipend payments per person per fy
#'
#' determine adcomp and stipend amount group by job, person, dept or some other
#' variable. parameters should be passed unquoted. All input parameters use
#' quasiquotation.
#'
#' @param df the dataframe containing job records with adcomp/stipend records
#' @param unique_id_field the grouping of the adcomp and stipend amounts.
#'   Typically gid/pidm, posn, or some dept
#' @param posn_field_name the name of the field used to store the position
#'   number. Complies with tidyr programming best-practice. Should be passed
#'   unquoted
#' @param suff_field_name the name of the field used to store the suffix.
#'   Complies with tidyr programming best-practice. Should be passed unquoted
#'
#' @return a dataframe containing one row per distinct grouping variable which
#'   contains a non-zero adcomp or stipend sum.
#' @export
compile_adcomp_stipend_totals <- function(df,
                                          unique_id_field = PIDM,
                                          posn_field_name = POSN,
                                          suff_field_name = SUFF,
                                          wage_month_field_name = MONTHLY_RATE,
                                          months_field_name = MONTHS) {
  suppressPackageStartupMessages({
    require(dplyr)
  })

  # default stipend and adcomp values
  stipend_suffixes <- c("SD", "SE")
  adcomp_posn <- "4ADCMP"

  #enquo allows the field supplied to the input parameters to be used in dplyr
  #verbs to filter, select, group by etc. These all represent column names.
  id_enquo <- enquo(unique_id_field)
  posn_enquo <- enquo(posn_field_name)
  suff_enquo <- enquo(posn_field_name)
  wage_enquo <- enquo(wage_month_field_name)
  months_enquo <- enquou(months_field_name)

  # make sure the filter and grouping columns are in the supplied dataframe
  # first if statment allows collapse of all verification if statements.
  if (TRUE == TRUE) {
    if (!quo_name(id_enquo) %in% names(df)) {
      stop(paste0(quo_name(id_enquo),
                  " not found in dataframe supplied to compile_adcomp_stipend_totals"))
    }
    if (!quo_name(posn_enquo) %in% names(df)) {
      stop(paste0(quo_name(posn_enquo),
                  " not found in dataframe supplied to compile_adcomp_stipend_totals"))
    }
    if (!quo_name(suff_enquo) %in% names(df)) {
      stop(paste0(quo_name(suff_enquo),
                  " not found in dataframe supplied to compile_adcomp_stipend_totals"))
    }
    if (!quo_name(wage_enquo) %in% names(df)) {
      stop(paste0(quo_name(wage_enquo),
                  " not found in dataframe supplied to compile_adcomp_stipend_totals"))
    }
    if (!quo_name(months_enquo) %in% names(df)) {
      stop(paste0(quo_name(months_enquo),
                  " not found in dataframe supplied to compile_adcomp_stipend_totals"))
    }
  }

  # compute stipend amounts
  stipend_amnt <- df %>%
    group_by(!! id_enquo) %>%
    filter(!! suff_enquo %in% stipend_suffixes) %>%
    summarize(stipend_month = sum(!! wage_enquo))

  adcomp_amnt <- df %>%
    group_by(!! id_enquo) %>%
    filter(!! posn_enquo %in% adcomp_posn) %>%
    summarize(adcomp_month = sum(!! wage_enquo))

  # join the monthly adcomp and stipends back to the orignial dataframe
  # compute the annual amount by extrapolating duration from employment months (9,10,12)
  df <- left_join(df,
                  stipend_amnt,
                  by = quo_name(id_enquo)) %>%
    left_join(adcomp_amnt,
              by = quo_name(id_enquo)) %>%
    mutate(stipend_annual = stipend_month * !! months_enquo,
           adcomp_annual = adcomp_month * !! months_enquo)

  return(df)
}

#' Classify Additional Compensation into categories
#'
#' Use eclass, position number, suffix and eclass to classify adcomp into one of
#' the following categories: 'Adcomp', 'Stipend', 'One-time Payment',
#' Overload/Overtime and 'Car Allowance/Other'
#'
#' @param df the dataframe whose adcomp  rows will be categorized
#' @param posn_col_name the unquoted column name containing position numbers
#' @param suff_col_name the unquoted column name containing job suffix
#' @param job_title_col_name the unquoted column name containing job titles
#' @param ecls_col_name the unquoted column name containing job or person eclass
#'
#' @return a dataframe with an additional `adcomp_type` column specifying the
#'   type
#' @export
classify_adcomp_type <- function(df,
                                 posn_col_name = POSN,
                                 suff_col_name = SUFF,
                                 job_title_col_name = JOB_TITLE,
                                 ecls_col_name = ECLS_JOBS) {

  suppressPackageStartupMessages({
    require(dplyr)
  })

  posn_enquo <- enquo(posn_col_name)
  suff_enquo <- enquo(suff_col_name)
  job_title_enquo <- enquo(job_title_col_name)
  ecls_enquo <- enquo(ecls_col_name)

  df_out <- mutate(df,
                   adcomp_type = case_when(!!posn_enquo == "4ADCMP"                ~ "AdComp",
                                           !!suff_enquo %in% c("SD", "SC", "SE")   ~ "Stipend",
                                           !!posn_enquo %in% c("4ONEPY", "4OEHHD") ~ "One-Time Payment",
                                           !!suff_enquo %in% c("OL")               ~ "Overload",
                                           !!suff_enquo == "CR"                    ~ "Car Allowance",
                                           !!ecls_enquo == "FS" | substr(!!posn_enquo, 1,2) == "4X" ~ "Summer Session",
                                           !!suff_enquo == "OT"                    ~"Overtime",
                                           T ~ as.character(NA))

  return(df_out)


}


#' Load BLS salary data
#'
#' loads BLS data for MSA, State, and National aggregations
#'
#' @param year the dataset's benchmark year typically published in the following
#'   year
#' @param add_year_key a field combining the SOC code and the year of the
#'   dataset. Useful for joining to datasets when working with multiple years of
#'   data
#'
#' @return a wide dataframe containing median and mean aggregates on the msa,
#'   state, and national levels.
#' @export
get_bls_salary <- function(year,
                           add_year_key = F) {

  suppressPackageStartupMessages({
    require(readxl)
    require(dplyr)
    require(stringr)
    require(magrittr)
  })

  fpath <- "X:/Employees/EMR Report (production)/Wage Benchmark Data/BLS/"

  msa_path <- paste0(fpath, "MSA/BOS_M", year, "_dl_MT_SW_only.xlsx")
  bls_msa <- readxl::read_xlsx(msa_path)
  names(bls_msa) <- stringr::str_to_upper(names(bls_msa))
  bls_msa$SCOPE <- "MSA"
  bls_msa <- dplyr::select(bls_msa,
                           -PRIM_STATE,
                           -AREA,
                           -AREA_NAME,
                           -JOBS_1000,
                           -EMP_PRSE,
                           -MEAN_PRSE)


  state_path <- paste0(fpath, "State/state_M", year, "_dl_MT_only.xlsx")
  bls_state <- readxl::read_xlsx(state_path)
  names(bls_state) <- stringr::str_to_upper(names(bls_state))
  bls_state$SCOPE <- "STATE"
  bls_state <- dplyr::select(bls_state,
                             -AREA,
                             -ST,
                             -STATE,
                             -JOBS_1000,
                             -EMP_PRSE,
                             -MEAN_PRSE)

  national_path <- paste0(fpath, "National/national_M", year,"_dl.xlsx")
  bls_national <- readxl::read_xlsx(national_path)
  names(bls_national) <- stringr::str_to_upper(names(bls_national))
  bls_national$SCOPE <- "NATIONAL"
  bls_national <- dplyr::select(bls_national,
                                -TOT_EMP,
                                -EMP_PRSE,
                                -MEAN_PRSE)

  bls_salaries_long <- dplyr::bind_rows(bls_msa, bls_state, bls_national)


  msa_to_join <- dplyr::select(bls_msa,
                               OCC_CODE,
                               OCC_TITLE,
                               H_MEAN,
                               H_MEDIAN,
                               A_MEAN,
                               A_MEDIAN)
  #append MSA to all column names to avoid duplicates when joining all three datasets
  names(msa_to_join) <- sapply(names(msa_to_join), paste0, "_MSA")

  state_to_join <- dplyr::select(bls_state,
                                 OCC_CODE,
                                 OCC_TITLE,
                                 H_MEAN,
                                 H_MEDIAN,
                                 A_MEAN,
                                 A_MEDIAN)
  names(state_to_join) <- sapply(names(state_to_join), paste0, "_ST")

  national_to_join <- dplyr::select(bls_national,
                                    OCC_CODE,
                                    OCC_TITLE,
                                    H_MEAN,
                                    H_MEDIAN,
                                    A_MEAN,
                                    A_MEDIAN)
  names(national_to_join) <- sapply(names(national_to_join), paste0, "_NAT")

  bls_salaries_wide <- dplyr::left_join(national_to_join,
                                        state_to_join,
                                        by = c("OCC_CODE_NAT" = "OCC_CODE_ST"))

  bls_salaries_wide <- dplyr::left_join(bls_salaries_wide,
                                        msa_to_join,
                                        by = c("OCC_CODE_NAT" = "OCC_CODE_MSA"))

  bls_salaries_wide <- mutate(bls_salaries_wide,
                              year = year)

  if (add_year_key == T) {
    bls_salaries_wide <- mutate(bls_salaries_wide,
                                key = paste0(OCC_CODE_NAT, "_", year))
  }

  return(bls_salaries_wide)
}

#' Pull Year of OSU Faculty Salaries
#'
#' OSU Faculty salaries provide annual faculty salaries based on 9/10 month
#' contracts. If comparing to FY contract faculty such as Department Heads, be
#' sure to convert to AY by multiplying Annual Salary by 9/11. Data aggregated
#' by CIP code. Assumes that the Faculty type has been included in the Avg Sal
#' and N columns in the format FacType - Average Salary aor FacType - N
#'
#' @param year The year corresponding to the salary benchmark
#' @param pivot_long original datasource stores faculty Rank in column names.
#'   Use pivot longer to convert to a tidy dataset with Rank stored in an
#'   independent column
#'
#' @return a dataframe containing faculty AY salary benchmarks and number of
#'   surveyed jobs contributing to the benchmark.
#' @export
get_osu_salary <- function(year = 2019, pivot_long = T) {
  suppressPackageStartupMessages({
    require(tidyverse)
    require(magrittr)
    require(readxl)
  })

  fpath <- "X:/Employees/EMR Report (production)/Wage Benchmark Data/OSU/"

  osu_salaries_wide <- read_excel(paste0(fpath, "OSU_MSU_", substr(year, 3,4), "F_post.xlsx"),
                                 skip = 6)

  osu_out <- osu_salaries_wide %>%
    mutate(cip = substr(`CIP, Discipline`, 1, 6),
           discipline = substr(`CIP, Discipline`, 8, nchar(`CIP, Discipline`)),
           year = year)

  #pivot Rank held in column name to it's own variable
  if (pivot_long == T) {

    #Expected Column Name Format
    #Rank - Average Salary,	Rank - N	 ...

    osu_out <- osu_out %>%
      pivot_longer(cols = ends_with(" - Average Salary"),
                   names_to = "Rank",
                   names_pattern = "(.*) - Average Salary",
                   values_to = "avg_salary") %>%
      pivot_longer(cols = ends_with(" - N"),
                   names_to = "Rank2",
                   names_pattern = "(.*) - N",
                   values_to = "n") %>%
      filter(Rank == Rank2) %>%
      select(-Rank2) %>%
      mutate(key = paste0(cip, "_", Rank, "_", year))
  } else {
    osu_out <- osu_out %>%
      mutate(key = paste0(cip, "_", year))
  }

  return(osu_out)

}

#' Build OSU CIP hierarchy
#'
#' For each CIP-Rank combination in the OSU salary datafile, compile the 2-digit,
#' four-digit cip code associated with each cip
#'
#' @param year the year (fall data) corresponding to the osu salary survey. 2019
#'   refers to data submitted in fall of 2019.
#'
#' @return a dataframe containing the cip code n-counts and average salary for
#'   the cip-rank combination at the 2, 4, 6 digit cip codes.
#'
#' @export
build_osu_hierarchy <- function(year = 2019) {
  require(stringr)
  osu_data <- opa::get_osu_salary(year = year)

  osu_data <- osu_data %>%
    mutate(cip_2d = str_pad(substr(cip, 1, 2), 6, "right", "0"),
           cip_4d = str_pad(substr(cip, 1, 4), 6, "right", "0"),
           cip_6d = cip,
           cip_2d_key = paste0(cip_2d, "_", Rank),
           cip_4d_key = paste0(cip_4d, "_", Rank),
           cip_6d_key = paste0(cip, "_", Rank)) %>%
    rename(cip6 = cip)

  osu_new_asst <- filter(osu_data,
                         Rank == "New Assistant")

  osu_data <- filter(osu_data,
                     Rank != "New Assistant")

  osu_2d <- filter(osu_data,
                   cip_2d == cip6) %>%
    select(cip2_title = discipline,
           cip2_n = n,
           cip2_avg_sal = avg_salary,
           cip_2d_key)

  osu_4d <- filter(osu_data,
                   cip_4d == cip6) %>%
    filter(Rank != "New Assistant") %>%
    select(cip4_title = discipline,
           cip4_n = n,
           cip4_avg_sal = avg_salary,
           cip_4d_key)


  osu_out <- osu_data %>%
    mutate(cip6_key = paste0(cip6, Rank)) %>%
    select(cip6,
           rank = Rank,
           cip6_title = discipline,
           cip6_sal = avg_salary,
           cip6_n = n,
           cip6_key,
           cip_2d_key,
           cip_4d_key) %>%
    left_join(osu_2d, by = c("cip_2d_key")) %>%
    left_join(osu_4d, by = c("cip_4d_key")) %>%
    select(-cip_2d_key,
           -cip_4d_key)

  return(osu_out)

}


#' Load CUPA Salary data
#'
#' Load CUPA salary benchmark data from Helene.
#'
#' @param ay_year the academic year for which salary data is pulled. 2016 refers
#'   to 2016-2017, etc.
#'
#' @return a single dataframe containing admin, professional, and staff cupa
#'   salaries for Land Grant institutions
#' @export
get_cupa_salary <- function(ay_year = 2018) {

  require(tidyverse)

  if(is.character(ay_year)) {
    ay_year <- as.numeric(ay_year)
  }

  fpath_abbreviation <- paste0(substr(ay_year, 3,4),
                              "-",
                              as.numeric(substr(ay_year, 3, 4)) + 1)

  fpath <- "X:/Employees/EMR Report (production)/Wage Benchmark Data/CUPA/"
  fpath <- paste0(fpath, ay_year, "-", ay_year + 1, "/")

  cupa_admin <- readr::read_csv(paste0(fpath, paste0("CUPA_Admin_", fpath_abbreviation, ".csv")))
  cupa_pro <- readr::read_csv(paste0(fpath, paste0("CUPA_Professional_", fpath_abbreviation, ".csv")))
  cupa_staff <- readr::read_csv(paste0(fpath, paste0("CUPA_Staff_", fpath_abbreviation, ".csv")))

  cupa_salaries <- dplyr::bind_rows(cupa_admin,
                                    cupa_pro,
                                    cupa_staff,
                                    .id = "job_type")

  cupa_salaries <- mutate(cupa_salaries,
                          year_span = paste0(ay_year, "-", ay_year + 1),
                          year = ay_year)

  return(cupa_salaries)
}

#' Convert the raw OSU DATAFEED file into a semi-formatted dataframe with
#' appropriate column names. Optionally, write this 'long-format' df to the
#' FacSal2 access db.
#'
#' @param fpath the filepath including filename and extension pointing of the
#'   fixed-width DATAFEED file
#' @param write_to_access a boolean specifying whether the dataframe should be
#'   written to the FacSal2 db.
#'
#' @return a dataframe containing the DATAFEED file with appropriate column names
#' @export
import_raw_osu_data <- function(fpath, write_to_access) {
  require(tidyverse)
  require(DBI)
  require(tictoc)

  # expects a fixed width
  df_raw <- read_fwf(fpath, col_positions = fwf_widths(widths = c(6,6,1,8,8,8,6,7,7,7,71,9),
                                                       col_names = c("INST",
                                                                     "CIP",
                                                                     "RANK",
                                                                     "LOW",
                                                                     "HIGH",
                                                                     "AVG",
                                                                     "N",
                                                                     "MIX_PCT",
                                                                     "NI",
                                                                     "SAL_FAC",
                                                                     "CIP_DESC",
                                                                     "AY")))

  if(write_to_access == TRUE) {
    facsal_db <- opa::get_access_conn("X:/IRCommon/FacSal/FacSal2.accdb")
    ay <- unique(df_raw$AY)
    ay_abbr <- paste0(substr(ay, 3,4), substr(ay, 8,9))

    new_table_name <- paste0("OSU", ay_abbr, "Report")

    tic("... Completed table update")
    message(paste0("Writing ", new_table_name, " to ", "X:/IRCommon/FacSal/FacSal2.accdb database..."))
    # This is pretty slow since it has to be written one row at a time. Not sure
    # why it's required here, but not when writing out the Employees snapshot
    DBI::dbWriteTable(facsal_db, new_table_name, value = df_raw, overwrite = T, batch_rows = 1)
    toc()
  }

  return(df_raw)
}

#' Compile the OSU-MSU salary report with appropriate formatting. Requires use
#' of an excel template prior to publication. See OSU_MSU_post_19F.xlsx
#'
#' @param raw_osu_df the dataframe of unformatted/unfiltered DATAFEED data
#'   supplied by `import_raw_osu_data` function
#' @param msu_cips_only a boolean to include if an additional dataset of CIPs
#'   related to MSU cips should be included
#' @param fpathname_out the excel document name to export.
#'
#' @return a list containing the formatted OSU data for all cips, optionally the
#'   formatted OSU data for MSU related cips, and the raw unformatted OSU
#'   dataset
#' @export
compile_osu_msu_pivot <- function(raw_osu_df,
                            msu_cips_only = F,
                            fpathname_out = "./OSU_MSU_20F_post_all_cips") {

  df_out <- raw_osu_df %>%
    opa::trim_ws_from_df() %>%
    filter(INST == "RU/VH",
           !RANK %in% c(9)) %>%
        group_by(CIP, CIP_DESC, RANK) %>%
        summarize(avg_sal = AVG,
                  n = N) %>%
    pivot_wider(id_cols = c(CIP, CIP_DESC), names_from = RANK, values_from = c(avg_sal, n)) %>%
    mutate(CIP = paste0(CIP, " ", CIP_DESC)) %>%
    ungroup() %>%
    select("CIP, Discipline" = CIP,
           "Professor - Average Salary" = avg_sal_1,
           "Professor - N" = n_1,
           "Associate - Average Salary" = avg_sal_2,
           "Associate - N" = n_2,
           "Assistant - Average Salary" = avg_sal_3,
           "Assistant - N" = n_3,
           "New Assistant - Average Salary" = avg_sal_4,
           "New Assistant - N" = n_4,
           "Instructor - Average Salary" = avg_sal_5,
           "Instructor - N" = n_5)

  list_out <- list()


  if(msu_cips_only == TRUE) {
    #academic year field assumed to have the format 2019-2020.
    #
    #Pull the 3rd and 4th characters to filter to pull the appropriate
    #employee snapshot
    academic_year <- unique(raw_osu_df$AY)
    year_abbr <- substr(academic_year, 3, 4)
    snapshot_tbl_name <- paste0("Employees", year_abbr, "F")

    opa_snapshot <- opa::get_access_conn()
    snapshot <- tbl(opa_snapshot, snapshot_tbl_name) %>% collect()

    # facsal <- opa::get_access_conn("X:/IRCommon/FacSal/FacSal2.accdb")
    # OSUFac19F <- tbl(facsal, "OSUFac19F") %>% collect()

    # these are all the valid cip codes to post to our website. They contain a row
    # for each cip assigned to a faculty as well as each of these cip's roll up
    # grouping i.e. those that end in 00, and 0000
    curr_ee_cip_combos <- distinct(OSUFac19F, CIP) %>%
      mutate(cip2d = paste0(substr(CIP, 1, 2), "0000"),
             cip4d = paste0(substr(CIP, 1, 4), "00"))

    all_cip_combos <- c(curr_ee_cip_combos$CIP, curr_ee_cip_combos$cip2d, curr_ee_cip_combos$cip4d) %>% unique()

    df_out_msu_only <- filter(df_out, substr(CIP,1,6) %in% all_cip_combos)
    list_out$"OSU_Post" <- df_out_msu_only

  }


  list_out$"OSU_DataCompiled" <- df_out
  list_out$"OSU_DataRaw" <- raw_osu_df


  opa::write_list_report(list_out, fpathname_out)

  return(list_out)
}

#' Get Stipend Comments for a given pidm from the PPRCCMT table
#'
#' @param pidms the vector of unquoted pidms, not to exceed length of 1000
#' @param opt_bann_conn the optional banenr connection supplied by opa::get_banner_conn()
#'
#' @return a dataframe containing all Stipend comments for the given pidms with both PIDM and the fiscal year in which they were entered.
#' @export
get_stipend_comments <- function(pidms, opt_bann_conn) {
  require(dbplyr)

  stopifnot(length(pidms) <= 1000)

  pprccmt <- tbl(bann_conn, "PPRCCMT") %>%
    filter(PPRCCMT_PIDM %in% pidms,
           PPRCCMT_CMTY_CODE == "STI") %>%
    collect() %>%
    mutate(fy = opa::compute_fiscal_year(PPRCCMT_ACTIVITY_DATE)) %>%
    select(pidm = PPRCCMT_PIDM,
           fy,
           stipend_comment = PPRCCMT_TEXT)

  return(pprccmt)
}


#' Summarize total Stipend payment per individual over a fiscal year.
#'
#' Determines stipend recipients and payments from payroll data. Links to home
#' department and non-stipend job titles using Banner snapshots taken on a
#' monthly basis covering entire fiscal year. Summarizes all non-stipend
#' payments and stipend payments as percent of non-stipend payments.
#'
#'
#' @param fy the numeric fiscal year in question
#' @param write_to_file a boolean indicating if the output should be written to
#'   file
#' @param opt_snapshot_df an optional bound set of snapshots from the fiscal
#'   year in question
#' @param opt_fpathname if write_to_file is TRUE, set the file path and name
#'   here
#' @param opt_bann_conn an optional banner connection supplied from
#'   opa::get_banner_conn()
#'
#' @return
#' @export
summarize_stipend_fy <- function(fy_in,
                                  write_to_file = F,
                                  opt_snapshot_df,
                                  opt_fpathname,
                                  opt_bann_conn) {

  require(magrittr)
  require(dplyr)
  require(lubridate)

  if(!missing(opt_snapshot_df)) {
    snap_all <- opt_snapshot_df
  }

  if(write_to_file == TRUE) {
    if(missing(opt_fpathname)) {stop("Must supply compile_stipend_data with fpathname if write_to_file set to TRUE")}
  }

  fy_start <- paste0(fy_in - 1, "-07-01", fy_in - 1) %>% as.Date()
  fy_end <- paste0(fy_in, "-06-01") %>% as.Date()

  # get a banner connection if not supplied. Used to pull PERAPPT data.
  if (missing(opt_bann_conn)) {
    bann_conn <- msuopa::get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }

  # # Pull Snapshots from Banner ----------------------------------------------
  if(missing(opt_snapshot_df)) {
    date_seq <- seq.Date(from = fy_start,
                         to = fy_end,
                         by = "months") %>% as.POSIXct()
    snap_seq <- lapply(date_seq,
                       FUN = opa::get_banner_snapshot,
                       opt_bann_conn = bann_conn,
                       max_fund_only = T,
                       remove_eclses = c("SE", "TS", "TH", "TM", "SF", "1H", "1S"))

    snap_all <- bind_rows(snap_seq)
  }


  snap_all <- snap_all %>%
    mutate(gid_job_key = paste0(GID, POSN, SUFF),
           fy = opa::compute_fiscal_year(date))

  snap_all <- filter(snap_all,
                     fy == fy_in)

  # Snap Stipend Records
  snap_stipend_recs <- snap_all %>%
    select(-starts_with("FUNDING")) %>%
    distinct() %>%
    filter(SUFF %in% c("SE", "SD",
                       "SF", "SC") |
             grepl("Stipend", POSITION_TITLE) |
             grepl("Stipend", JOB_TITLE),
           !ECLS_JOBS == "1H") %>%
    mutate(gid_fy_key = paste0(GID, fy),
           job_fy_key = paste0(job_key, fy))

  # Records for all other Jobs held by those recieving stipends
  snap_nonstipend_recs <- snap_all %>%
    filter(!SUFF %in% c("SE", "SD",
                        "SF", "SC"),
           !grepl("Stipend", POSITION_TITLE),
           !grepl("Stipend", JOB_TITLE),
           !ECLS_JOBS == "1H") %>%
    mutate(gid_fy_key = paste0(GID, fy)) %>%
    filter(gid_fy_key %in% snap_stipend_recs$gid_fy_key)

  # Get the non-stipend Titles to join to the amount of money paid via stipend jobs
  org_hier <- opa::build_org_hierarchy_lu(opt_bann_conn = bann_conn)
  org_hier_names <- select(org_hier,
                           dept_number = seed,
                           dept_name = seed_desc)

  snap_titles_depts <- snap_nonstipend_recs %>%
    left_join(org_hier_names, by = c("HOME_DEPT_CODE" = "dept_number")) %>%
    group_by(gid_fy_key) %>%
    summarize(gid = paste0(unique(GID), collapse = ", "),
              name = paste0(unique(NAME), collapse = ", "),
              titles = paste0(unique(JOB_TITLE), collapse = ", "),
              depts = paste0(unique(dept_name), collapse =  ", "),
              posns_held = paste0(unique(POSN), collapse = ", "))

  min_date <- min(snap_all$date) %>% as.POSIXct()
  max_date <- max(snap_all$date) %>% as.POSIXct()

  pr_earnings <- opa::get_payroll_data(most_recent_only = F,
                                       opt_start_date = min_date,
                                       opt_end_date = max_date,
                                       opt_bann_conn = bann_conn)

  pr_earnings <- opa::trim_ws_from_df(pr_earnings)
  pr_earnings <- mutate(pr_earnings,
                        job_key = paste0(pidm, posn, suff),
                        gid_fy_key = paste0(gid, fy),
                        job_fname_key = paste0(job_key, fname)) %>%
    filter(!substr(posn,1,2) %in% c("4M", "4N", "4C", "4S", "4D"))

  pr_earnings <- mutate(pr_earnings,
                        pay_type = case_when(suff %in% c("SE", "SC", "SD", "SF") ~ "Stipend",
                                             (posn %in% c("4ADCMP") & earn_code != "GRT" & suff != "FR") ~ "Adcomp",
                                             substr(posn, 1, 2) == "4X" ~ "SummerSession",
                                             (earn_code == "GRT" | suff == "FR" | posn == "4IFPRS") ~ "IPR",
                                             T ~ "Base/Other"))

  pr_earnings_summary <- group_by(pr_earnings,
                                  gid_fy_key,
                                  pay_type) %>%
    summarize(total_pay = sum(amount, na.rm = T)) %>%
    pivot_wider(names_from = pay_type, values_from = total_pay, names_prefix = "pay_") %>%
    filter(!is.na(pay_Stipend),
           !is.na(`pay_Base/Other`))




  # Pull Payroll Stipend Comments -------------------------------------------
  # Hasn't been updated Since June 2016 -- Business Practice changed?
  #stipend_comments <- opa::get_stipend_comments(unique(snap_stipend_recs$PIDM), bann_conn)

  # sum the total paid via stipend and join the non-stipend titles held by each employee
  pr_earnings_summary <- pr_earnings_summary %>%
    left_join(snap_titles_depts,
              by = "gid_fy_key") %>%
    mutate(stipend_percent = pay_Stipend/`pay_Base/Other`,
           total_payment = sum(`pay_Base/Other`, pay_Stipend, pay_Adcomp, pay_SummerSession, pay_IPR, na.rm = T),
           fy = fy_in)

  pr_summary_out <- select(pr_earnings_summary,
                           fy,
                           GID = gid,
                           Name = name,
                           Job_Titles_Held = titles,
                           `Home Dept(s)` = depts,
                           Posn_Held = posns_held,
                           total_stipend_pay = pay_Stipend,
                           total_adcomp_pay = pay_Adcomp,
                           total_summersession_pay = pay_SummerSession,
                           total_ipr_pay = pay_IPR,
                           `total_base/other` = `pay_Base/Other`,
                           total_pay_all = total_payment,
                           stipend_percent)

  if(write_to_file == TRUE) {
    opa::write_report(pr_summary_out, fpath = "./stipends", fname = "stipend_summary", sheetname = fy_in)
  }

  return(pr_summary_out)
}
