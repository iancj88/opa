#' Filter a dataframe to only include rows holding a maximum value for each
#' key-value.
#'
#' a utility function that filters to only the records for each key-value which
#' contain the . This assumes that the key values are duplicated over rows with
#' given numeric or date column differentiating their values.
#'
#' @param df the dataframe to be filtered
#' @param key_col_name the name of the column containing the keys to use as
#'   grouping variables. Uses quasi-quotation so supply the column name
#'   unquoted. See https://dplyr.tidyverse.org/articles/programming.html
#' @param col_to_max_name the name fo the column containing a numeric or date
#'   value. only the row containing the max value in this column will be kept.
#'   Uses quasi-quotation so supply the column name unquoted. See
#'   https://dplyr.tidyverse.org/articles/programming.html
#'
#' @return the original dataframe filtered to only contain the max-dated
#'   records. All records with this max date per grouping variable will be
#'   returned.
#' @export
#' @author Ian C Johnson
filter_by_max_per_key <- function(df,
                                  key_col_name,
                                  col_to_max_name) {
  suppressPackageStartupMessages({
    require(dplyr)
  })

  # Verify Key and Max Val columns ---------------------------------------------

  key_col_enquo <- enquo(key_col_name)
  col_to_max_enquo <- enquo(col_to_max_name)

  #check that df contains the key and to max columns named by input parameters
  if (!quo_name(key_col_enquo) %in% names(df)) {
    stop(
      paste0(
        quo_name(key_col_enquo),
        " not found in dataframe supplied to filter_by_max_date_per_key"))
  }
  if (!quo_name(col_to_max_enquo) %in% names(df)) {
    stop(
      paste0(
        quo_name(col_to_max_name),
        " not found in dataframe supplied to filter_by_max_date_per_key"))
  }


# Group by Date col to find max date per key-----------------------------------

  # add new date-key column for filtering
  df  <- mutate(df,
                to_max_key = paste0(!!key_col_enquo, !!col_to_max_enquo))

  #use key col as grouping variable
  max_value_per_key <- group_by(df,
                               !!key_col_enquo)

  #get the max value for each key
  max_value_per_key <- summarize(max_value_per_key,
                                 max_vol_value = max(!!col_to_max_enquo))

  #make a key-maxval variable to use as a filter against newly created value-key
  #column in the original dataframe
  max_value_per_key <- mutate(max_value_per_key,
                              max_value_key = paste0(!!key_col_enquo,
                                                   max_vol_value))


# Filter orig df to max date rows per key ---------------------------------

  # filter the original dataframe
  df_out <- filter(df,
                   to_max_key %in% max_value_per_key$max_value_key)

  return(df_out)
}


#' Source all R Files contained in a given folder
#'
#' @description Source all files in a given folder for the current R session. Any file
#' ending with *.R will be sourced. implemented in Base R
#'
#' @param folder_path The folder path containing the R files. By default, uses
#' the ./R/ folder contained in the working directory
#'
#' @return Success message will be printed to terminal
#' @export
#' @author Ian C Johnson
source_folder_files <- function(folder_path = "./R/") {
  # Load up the functions stored in the ./R/ folder
  file.sources <- list.files(path = folder_path,
                             pattern = "\\.R$",
                             full.names = TRUE)
  #if zero files found i.e. if length = 0,
  if (length(file.sources) == 0) {
    stop(simpleError(sprintf('No R Source files found')))
  }

  src <- invisible(lapply(file.sources, source))

  for (i in 1:length(file.sources)) {
    message(sprintf('sourced %s', file.sources[i]))
  }

  message(sprintf('%s files successfully sourced.', length(src)))
}


#' Compute the Fiscal Year given a date
#'
#' @description Computes the fiscal year of a vector of dates based on the Montana State
#' fiscal calendar (July 1 - June 30).
#'
#' @param date the vector of dates from which to compute fiscal year
#'
#' @return the vector of year integers
#' @export
#'
#' @examples
#' dte_1 <- as.Date("2018-01-01")
#' dte_2 <- as.Date("2018-08-01")
#' compute_fiscal_year(dte_1)
#' compute_fiscal_year(dte_2)
#' @author Ian C Johnson
compute_fiscal_year <- function(date) {
  #handle NA values in the date vector
  qrtr <- data.table::quarter(date)
  qrtr[is.na(qrtr)] <- 0
  third_fourth_quarter <- (qrtr > 2)

  date <- data.table::year(date)
  date[is.na(date)] <- 1900

  date[third_fourth_quarter] <- date[third_fourth_quarter] + 1

  return(date)
}

#' Load and format a SQL query script from text/file source.
#'
#' load a properly formatted sql query from a plain text file to be sent to an
#' oracle db via ROracle::dbSendQuery or an access db via DBI::dbSendQuery
#'
#' @param file_path the full folder path containing the file
#' @param file_name the full name of the file with extension
#'
#' @seealso `DBI::dbSendQuery()` and `ROracle::dbSendQuery()`
#' @return a string containing the sql query
#' @export
#' @author Ian C Johnson
load_sql_qry <- function(file_path, file_name) {
  full_path <- paste0(file_path, file_name)
  qry <- paste(readLines(full_path), collapse = '\n')
  return(qry)
}


#' allee_dates_from_fnames
#'
#' Extracts and formats the dates contained in the csv file names. The csv files
#' must be in the form "YYYYMMDD All Employees.txt".
#'
#' @param file_list a character vector containing the names of all the txt
#' files in the folder containing csv
#'
#' @return list of POSIXct dates contained in the input filenames
allee_dates_from_fnames <- function(file_list) {
  date_from_file_name <- gsub(" All Employees.txt", "", file_list)
  date_from_file_name <-  as.POSIXct(date_from_file_name, format = "%Y%m%d")
  return(date_from_file_name)
}

#' convert_allee_txt_rds
#'
#' Convert a folder path containing .txt semi-colon delimited all employees
#' report(s) from Report Web into RDS files.
#'
#' @param folder_path the folder containing .txt all employees reports
#'
#' @return called for it's side-effects, NULL return value.
#' @export
convert_allee_txt_rds <- function(folder_path, opt_output_path) {
  if(missing(opt_output_path)) {
    output_path <- "X:/Employees/All EEs Reports/rds_src/"
  } else {
    output_path <- opt_output_path
  }

  #ensure working with only txt files
  file_names_only <- list.files(folder_path, full.names = FALSE)
  full_path <- list.files(folder_path, full.names = TRUE)

  if (sum(grepl(".txt", file_names_only)) > 0) {
    full_path <- full_path[grepl(".txt", file_names_only)]
    file_names_only <- file_names_only[grepl(".txt", file_names_only)]

# read each txt All EE file -----------------------------------------------

    dfs <- mapply(fread_allee_csv,
                  path = full_path,
                  name = file_names_only,
                  SIMPLIFY = FALSE)

    output_paths <- sapply(file_names_only,
                           function(x) paste0(output_path,
                                              x))

    output_paths <- stringr::str_replace(output_paths, ".txt", ".rds")

# clean all ee data -------------------------------------------------------

    # Format the date columns out of their screwy dd-MMM-yy format
    loaded_data <- lapply(dfs, format_allEE_dates)
    #loaded_data <- lapply(loaded_data, msuopa::fix_native_org_names)
    #loaded_data <- lapply(loaded_data, supplement_all_ee)


# write all ee data to RDS  -----------------------------------------------

    mapply(saveRDS, object = loaded_data, file = output_paths)

    invisible(lapply(full_path, file.remove))
  }
  return(invisible(NULL))
}

#' All Employees Report Column Types
#'
#' A named list of vectors of column names specifying column types as numeric or
#' character. Additional date formating is necessary after loading. This column
#' specification ensures that leading zeros are never dropped from certain
#' fields such as GID or Zip Code.
#'
#' @param date the date on which the All Employees report was run from Banner.
#' This is necessary because the columns were expanded on 12/15/2017
#'
#' @return a named list of vectors assigning each column to a class by column
#' name.
all_ee_col_types <- function(date) {

# column documentation ----------------------------------------------------

  # Current column names in all ee as of 2017/12/27:

  # [1] "GID"                    "Last Name"
  # [3] "First Name"             "Home Street 1"
  # [5] "Home Street 2"          "Home Street 3"
  # [7] "City"                   "State"
  # [9] "Zip"                    "Campus"
  # [11] "Pict Code"              "Department"
  # [13] "Home Orgn Number"       "Budget Org."
  # [15] "Budget Org. Long Desc." "Org. Heirarchy"
  # [17] "Job Title"              "Status"
  # [19] "PEAEMPL ECLS"           "ECLS Description"
  # [21] "MUS"                    "Position Number"
  # [23] "Suffix"                 "Position Title"
  # [25] "FTE"                    "Job Type"
  # [27] "Pays"                   "Current Hire Date"
  # [29] "Campus Orig. Hire"      "Longevity Date"
  # [31] "Annual Lv Accrual"      "Anniversary Date"
  # [33] "Last Work Date"         "Job Begin Date"
  # [35] "Employee Group"         "Hourly Rate"
  # [37] "Annual Salary"          "Assgn Salary"
  # [39] "Retirement"             "Union"
  # [41] "Union Deduction"        "BCAT"
  # [43] "Leave Category"         "Sex"
  # [45] "Race 1"                 "Birth Date"
  # [47] "SOC Code"               "SOC Description"
  # [49] "Email"                  "Phone"
  # [51] "Index"                  "Fund"
  # [53] "Orgn"                   "Account"
  # [55] "Program"                "Percent"
  # [57] "date"                   "CUPA Code"
  # [59] "CUPA Desc."             "FED SOC Code"
  # [61] "FED SOC Code Desc."     "MUS SOC Code"
  # [63] "MUS SOC Code Desc."     "JCAT Code"
  # [65] "JCAT Desc."

  # column names in all ee as prior to 2017/12/15:

  # [1] "GID"                    "Last Name"
  # [3] "First Name"             "Home Street 1"
  # [5] "Home Street 2"          "Home Street 3"
  # [7] "City"                   "State"
  # [9] "Zip"                    "Campus"
  # [11] "Pict Code"              "Department"
  # [13] "Home Orgn Number"       "Budget Org."
  # [15] "Budget Org. Long Desc." "Org. Heirarchy"
  # [17] "Job Title"              "Status"
  # [19] "PEAEMPL ECLS"           "ECLS Description"
  # [21] "MUS"                    "Position Number"
  # [23] "Suffix"                 "Position Title"
  # [25] "FTE"                    "Job Type"
  # [27] "Pays"                   "Current Hire Date"
  # [29] "Campus Orig. Hire"      "Longevity Date"
  # [31] "Annual Lv Accrual"      "Anniversary Date"
  # [33] "Last Work Date"         "Job Begin Date"
  # [35] "Employee Group"         "Hourly Rate"
  # [37] "Annual Salary"          "Assgn Salary"
  # [39] "Retirement"             "Union"
  # [41] "Union Deduction"        "BCAT"
  # [43] "Leave Category"         "Sex"
  # [45] "Race 1"                 "Birth Date"
  # [47] "SOC Code"               "SOC Description"
  # [49] "Email"                  "Phone"
  # [51] "Index"                  "Fund"
  # [53] "Orgn"                   "Account"
  # [55] "Program"                "Percent"
  # [57] "date"

  all_ee_v2_date <- as.POSIXct("2017/12/15")

# column specs by date -----------------------------------------------------

  if (date < all_ee_v2_date) { # This is the older version
    col_types <- list(character = c("GID",
                                    "Last Name",
                                    "First Name",
                                    "Home Street 1",
                                    "Home Street 2",
                                    "Home Street 3",
                                    "City",
                                    "State",
                                    "Zip",
                                    "Campus",
                                    "Pict Code",
                                    "Department",
                                    "Home Orgn Number",
                                    "Budget Org.",
                                    "Budget Org. Long Desc.",
                                    "Org. Heirarchy",
                                    "Job Title",
                                    "Status",
                                    "PEAEMPL ECLS",
                                    "ECLS Description",
                                    "MUS",
                                    "Position Number",
                                    "Suffix",
                                    "Position Title",
                                    "Job Type",
                                    "Current Hire Date",
                                    "Campus Orig. Hire",
                                    "Longevity Date",
                                    "Annual Lv Accrual",
                                    "Anniversary Date",
                                    "Last Work Date",
                                    "Job Begin Date",
                                    "Employee Group",
                                    "Retirement",
                                    "Union",
                                    "Union Deduction",
                                    "BCAT",
                                    "Leave Category",
                                    "Sex",
                                    "Race 1",
                                    "Birth Date",
                                    "SOC Code",
                                    "SOC Description",
                                    "Email",
                                    "Phone",
                                    "Index",
                                    "Fund",
                                    "Orgn",
                                    "Account",
                                    "Program"),
                      numeric = c("FTE",
                                  "Pays",
                                  "Hourly Rate",
                                  "Annual Salary",
                                  "Assgn Salary",
                                  "Percent"))
  } else {
    # This is the newer all ee version with removed SOC Code,
    # SOC Description columns and added CUPA, JCAT, SOC FED, and SOC
    # MUS columns specified
    if (TRUE == TRUE) {
      col_types <- list(character = c("GID",
                                      "Last Name",
                                      "First Name",
                                      "Home Street 1",
                                      "Home Street 2",
                                      "Home Street 3",
                                      "City",
                                      "State",
                                      "Zip",
                                      "Campus",
                                      "Pict Code",
                                      "Department",
                                      "Home Orgn Number",
                                      "Budget Org.",
                                      "Budget Org. Long Desc.",
                                      "Org. Heirarchy",
                                      "Job Title",
                                      "Status",
                                      "PEAEMPL ECLS",
                                      "ECLS Description",
                                      "MUS",
                                      "Position Number",
                                      "Suffix",
                                      "Position Title",
                                      "Job Type",
                                      "Current Hire Date",
                                      "Campus Orig. Hire",
                                      "Longevity Date",
                                      "Annual Lv Accrual",
                                      "Anniversary Date",
                                      "Last Work Date",
                                      "Job Begin Date",
                                      "Employee Group",
                                      "Retirement",
                                      "Union",
                                      "Union Deduction",
                                      "BCAT",
                                      "Leave Category",
                                      "Sex",
                                      "Race 1",
                                      "Birth Date",
                                      "Email",
                                      "Phone",
                                      "Index",
                                      "Fund",
                                      "Orgn",
                                      "Account",
                                      "Program",
                                      "CUPA Code",
                                      "CUPA Desc.",
                                      "FED SOC Code",
                                      "FED SOC Code Desc.",
                                      "MUS SOC Code",
                                      "MUS SOC Code Desc.",
                                      "JCAT Code",
                                      "JCAT Desc."),
                        numeric = c("FTE",
                                    "Pays",
                                    "Hourly Rate",
                                    "Annual Salary",
                                    "Assgn Salary",
                                    "Percent"))
    }

  }

  return(col_types)
}

#' Format All EE Report Dates
#'
#' Properly format dates using the ISO YYYY-MM-DD standard. All Employees report
#' formats them as character type in the DD-MMM-YYYY format.
#'
#' @param df dataframe containing the all employees data
#'
#' @return the input dataframe with revised date formats
format_allEE_dates <- function(df) {

  date_cols <- c("Current Hire Date",
                 "Campus Orig. Hire",
                 "Longevity Date",
                 "Annual Leave Accrual",
                 "Anniversary Date",
                 "Last Work Date",
                 "Job Begin Date",
                 "Birth Date")

  date_cols_indx <- which(names(df) %in% date_cols)

  # now sort through the ones with bad date formats
  #date_cols_indx <- which(typeof(df[1,date_cols_indx]) == "POSIXcT/POSIClT")

  for (col in date_cols_indx) {
    if (!class(df[, col])[1] == "POSIXct") {
      df[,col] <- lubridate::parse_date_time2(df[,col], "%d-%b-%y")

      # the year is stored as a two digit number making it difficult to parse
      # properly. '80' may be interpreted as 1980 or 2080. if the date was
      # parsed as the future, subtract 100 from it.
      misread_years <- which(lubridate::year(df[,col]) > lubridate::year(Sys.Date()))
      if (length(misread_years) > 0) {
        lubridate::year(df[misread_years, col]) <- lubridate::year(df[misread_years, col]) - 100
        misread_years <- NULL
      }
    }
  }

  return(df)
}

#' Add supplmental data to All Employees Report
#'
#' @description A wrapper for several functions that add additional columns to the all
#' employees report. Adds Job Type, Longevity Bonus, Full Name,
#' Job Key, Job Date Key, and fiscal year.
#'
#' @param df the all employees report with unaltered column header names.
#'
#' @return the original input dataframe with the additional columns
#' @seealso add_emr_job_type, add_emr_orgs, add_longevity_bonus
#' @author Ian C Johnson
#' @export
#'
supplement_all_ee <- function(df) {
  # Drop the previously existing data to ensure that there are no duplicate
  # columns after joining
  cols_to_drop <- c("EMROrg", "VPOrg", "EMROrg.x",
                    "EMROrg.y", "VPOrg.x", "VPOrg.y")
  df_out <- df[, !names(df) %in% cols_to_drop]

  # df_out <- add_emr_job_type(df_out,
  #                            position_number_col_name = "Position Number",
  #                            suffix_col_name = "Suffix",
  #                            mus_col_name = "MUS")
  # df_out <- add_emr_orgs(df_out,
  #                        dept_number_col_name = "Budget Org.",
  #                        new_emrorg_col_name = "EMROrg_Budget")
  # df_out <- add_emr_orgs(df_out,
  #                        dept_number_col_name = "Home Orgn Number",
  #                        new_emrorg_col_name = "EMROrg_Home")

  df_out <- add_longevity_bonus(df_out,
                                longevity_date_col = "Longevity Date",
                                hr_rate_col = "Hourly Rate",
                                assgn_rate_col = "Assgn Salary",
                                annual_rate_col = "Annual Salary")

  df_out$FullName <- paste0(df_out$`Last Name`,
                            ", ",
                            df_out$`First Name`)

  df_out$job_key <- paste0(df_out$GID,
                           df_out$`Position Number`,
                           df_out$Suffix)
  df_out$job_date_key <- paste0(df_out$GID,
                                df_out$`Position Number`,
                                df_out$Suffix,
                                df_out$date)

  df_out$FY <- compute_fiscal_year(date = df_out$date)

  return(df_out)
}


#' Split Vector into list of vectors each containing a maximum number of values
#'
#' @description Take a vector of values and split it into a list of vectors each containing,
#' at most, the number of items specified in max_size. Useful for constructing
#' plsql queries against Banner using the `%in%` operator.
#'
#' @param vector the vector of values to be split
#' @param max_size the maximum items to place in each new vector. defaults to
#' the plsql 1000 item limit
#'
#' @return a list containing the split vectors
#' @author Ian C Johnson
#' @export
split_vec_for_sql <- function(vector, max_size = 1000, all_distinct = T) {
  if (all_distinct == TRUE) {vector <- unique(vector)}
  output <- split(vector, ceiling(seq_along(vector)/max_size))
  return(output)
}

#' Drop a column from a df by enquoted name if it exists.
#'
#' @description If the column is not contained in the supplied dataframe, the
#'   dataframe is returned unmodified. Useful for removing potentially
#'   duplicated columns.
#'
#' @param df the dataframe containing the column to drop
#' @param col_name the unquoted name of the column to drop
#'
#' @return the original dataframe minus the specified column
#' @author Ian C Johnson
#' @export
drop_col <- function(df, col_name) {
  col_name_enquo <- enquo(col_name)

  if (quo_name(col_name_enquo) %in% names(df)) {
    df_out <- select(df, -!!col_name_enquo)
  } else {
    df_out <- df
  }

  return(df_out)
}

#' Rename a column in a dataframe
#'
#' Quickly rename a column based on it's current name rather than location. This
#' is helpful in certain instances when a name of a column cannot be determined
#' in advance.
#'
#' @param df the dataframe containing columns to be renamed
#' @param old_name a string containing the name of the column to be renamed
#' @param new_name a string containing the new name.
#'
#' @return the same dataset with a renamed column
#' @author Ian C Johnson
#' @export
rename_col <- function(df, old_name, new_name) {
  if (new_name %in% colnames(df)) {
    warning("!!! New column name ",
            new_name,
            " already exists in the dataframe, ignoring.")
    return(df)
  }

  if (!old_name %in% colnames(df)) {
    warning("!!! Old column name ",
            old_name,
            " does not exist in dataframe, ignoring.")
    return(df)

  }

  colnames(df)[which(names(df) == old_name)] <- new_name
  return(df)
}


#' Time Whitespace from all character columns in dataframe
#'
#' remove whitespace surrounding values stored in character type dataframe
#' columns. Commonly needed when pulling data from Access
#'
#' @param df the dataframe to clean of whitespace
#'
#' @return the original dataframe with whitespace removed from character column
#'   values
#' @export
trim_ws_from_df <- function(df) {

  for (i in names(df)) {
    if (inherits(df[, i], c("factor", "character"))) {
      df[, i] <- trimws(df[, i])
    }
  }

  return(df)
}


#' Pad Gid Values for Dropped preceeding Zeros
#'
#' GID values take the character form `-xxxxxxxx` where `x` is a numeric
#' character. By default, most applications erroneously assing these values to
#' numeric data types causing the preceding zeros to be droped. There may be
#' issues if only some of the values are proper lengths
#'
#' @param gid_vec the vector containing gid vlue
#'
#' @return a character value where each GID volue is the proper 9-character
#'   length with preceeding zeros included.
#' @export
pad_gid <- function(gid_vec) {
  gid_length <- 9

  tmp_df <- tibble("gid_values" = gid_vec,
                   "gid_length" = gid_length) %>%
    mutate(n_missing_zero = gid_length - nchar(gid_vec),
           missing_values = if_else(!is.na(n_missing_zero),
                                    stringi::stri_dup("0", n_missing_zero),
                                    as.character(NA)),
           trimmed_gid = gsub("-", "", gid_vec),
           missing_values = if_else(!is.na(gid_vec),
                                    paste0("-", missing_values, trimmed_gid),
                                    as.character(NA)))

  tmp_out <- tmp_df$missing_values

  return(tmp_out)
}


fix_cruzado <- function(df, opt_col_name) {
  suppressPackageStartupMessages({
    require(dplyr)

  })

  # if (!missing(opt_col_name)) {
  #   col_name_enquo <- enquo(opt_col_name)
  #
  #   df_out <- mutate(df,
  #                    col_name_enquo = gsub("Cruzado-Salas", "Cruzado", col_name_enquo))
  # } else {
    for (i in 1:ncol(df)) {
    df[,i] <- gsub("Cruzado-Salas", "Cruzado", df[,i])
    df_out <- df
    }
  # }

  return(df_out)

}


#' Reorder columns by alphabetical order
#'
#' @param df the dataframe whose columns will be reordered
#'
#' @return a dataframe containing columns ordered alphabetically descending
#'   order
#' @export
sort_cols_by_name <- function(df) {

  df_out <- df[, order(names(df))]

  return(df_out)
}


#' Transpose a dataframe using tidyr functions
#'
#' Transpose a dataframe using pivot_longer and pivot_wider. The names_to_str
#' specifies the column names into which the previous column names will be
#' added. names_from is the current column of values which will be transposed
#' into the new columns. If a prefix to the column name values is needed, it can
#' be supplied via names_out_prefix_str.
#'
#' @param df the dataframe to be transposed
#' @param names_to_str the name of the new column containing the values
#'   previously stored in the column names
#' @param names_from the unquoted column whose values will be used as new column
#'   names
#' @param names_out_prefix_str an optional prefix to be used in the new column
#'   names
#'
#' @return a transposed dataframe from the original df
#' @export
transpose_tidyr <- function(df, names_to_str, names_from, names_out_prefix_str) {
  require(tidyr)
  require(dplyr)
  require(magrittr)

  names_from_enquo <- enquo(names_from)

  if (missing(names_out_prefix_str)) {
    names_out_prefix_str <- ""
  }

  df_out <- df %>%
    pivot_longer(! (!!names_from_enquo),
                 names_to = names_to_str) %>%
    pivot_wider(id_cols = names_to_str,
                names_from = !!names_from_enquo,
                names_prefix = names_out_prefix_str)

  return(df_out)
}

#' remove_na_cols
#'
#' remove columns from dataframe if they contain \emph{only} NA values
#'
#' @param df the dataframe or datatable from which to remove columns
#'
#' @return a datatable with NA columns removed
#' @export
#'
remove_na_cols <- function(df) {
  dt <- data.table::as.data.table(df)
  dt <- dt[,which(unlist(purrr::map(dt, function(x)!all(is.na(x))))), with = F]
  return(dt)
}

#' remove_na_rows
#'
#' remove columns from dataframe if they contain \emph{only} NA values
#'
#' @param df the dataframe or datatable from which to remove columns
#'
#' @return a datatable with NA columns removed
#' @export
#'
remove_na_rows <- function(df) {
  dt <- data.table::as.data.table(df)
  dt <- dt[rowSums(!is.na(dt)) > 0,]
  return(dt)
}


#' Add factor level to a specified dataframe column
#'
#'
#' @param x_col the column of data in which the factor level will be added. If
#'   not already a factor, will return the column unaltered
#' @param new_level the character value of the new factor level. Commonly NA or
#'   "".
#'
#' @return a column of data with teh new factor level added if the supplied
#'   vector of data was a factor
#' @export
add_factor_levels <- function(x_col, new_level = "") {
  if(is.factor(x_col)) return(factor(x_col, levels=c(levels(x_col), new_level)))
  return(x_col)
}


#' Convert Date to proper Banner date format
#'
#' Accept Character, Date, or POSIXt class dates and return the proper
#' 'dd-mmm-yyyy' date format to be used in Banner sql filters.
#'
#' @param date_in a Date, character, or POSIXt class vector to be converted
#' @param opt_date_in_format if a character class date input is supplied,
#'   specify the `strptime` format
#'
#' @return a character vector of the format 'dd-mmm-yyyy' ('%d-%b-%Y')
#' @export
date_banner_convert <- function(date_in,
                                opt_date_in_format = "%d-%b-%Y") {


  # set to UTC time zone to ensure compatibility with Banner dates
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  if (any(class(date_in) %in% c("Date", "POSIXct", "POSIXt"))) {
    date_out <- as.character(date_in, format = "%d-%b-%Y") %>% toupper()
  } else if (class(date_in) == "character") {
    date_out <- as.character(as.POSIXct(date_in,
                                        format = opt_date_in_format),
                             format = "%d-%b-%Y") %>% toupper()
  }

  return(date_out)
}