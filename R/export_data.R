
#' write_report
#'
#' Export a dataframe to an excel and/or csv file. Typically used to share
#' aggregated datasets.
#'
#' @param df the dataframe to be output
#' @param fpath the full name of the path to which the file will be written
#' @param fname the file name to be used for the output. Do not include .csv or
#'   .xlsx or any other filetype specifier
#' @param sheetName the name of the sheet and table
#' @param include_xlsx an optional parameter which defaults to TRUE. Set to
#'   FALSE to only output an a flat csv file. Ideal when writing  a large number
#'   of individual files
#'
#' @return A success message will be returned
#' @import data.table
#' @import openxlsx
#' @export
#'
write_report <- function(df,
                         fpath,
                         fname,
                         sheetname,
                         include_xlsx = TRUE) {

  require(tidyverse)
  require(magrittr)

  if (missing(sheetname)) {sheetname <- deparse(substitute(df))}

  #check that there are not duplicated case-insensitive column names
  names_df_upper_unique <- names(df) %>%
    toupper() %>%
    unique()

  if (length(names_df_upper_unique) != length(names(df))) {
    stop("Column names in df must be case-insensitive unique")
  }

  if(!dir.exists(fpath)) {stop(paste0(fpath, " Not found on system"))}
  if (!grepl("/$" , fpath)) {
    fpath <- paste0(fpath, "/")
  }

  fpathname <- paste0(fpath, fname)

  # this writes the csv file
  data.table::fwrite(x = df,
                     file = paste0(fpathname, ".csv"),
                     append = FALSE,
                     sep = ",",
                     col.names = TRUE)

  # this write the excel document
  if (include_xlsx == TRUE) {

    #check that the number of rows, columns will fit in excel
    if (include_xlsx == TRUE) {
      if (nrow(df) >  1048576) {
        stop(paste0("df contains ", nrow(df), " rows. Exceeds Excel max by ", nrow(df) - 1048576))
      }
      if (ncol(df) >  16384) {
        stop(paste0("df contains ", ncol(df), "cols. Exceeds Excel max by ", ncol(df) - 16384))
      }
    }

    header_row <- 2 # skip a row to manually add a title
    data_sht <- 1
    wb_active <- openxlsx::createWorkbook()

    #options("openxlsx.borderColour" = "#4F80BD")
    #options("openxlsx.borderStyle" = "thin")
    #options("openxlsx.dateFormat" = "mm/dd/yyyy")

    options("openxlsx.datetimeFormat" = "mm/dd/yyyy")
    openxlsx::modifyBaseFont(wb_active,
                             fontSize = 10,
                             fontName = "Segoe UI")
    openxlsx::addWorksheet(wb_active,
                           sheetName = sheetname,
                           zoom = 85,
                           header = c("Montana State University - Bozeman",
                                      sheetname,
                                      paste0("Compiled on ", Sys.Date())),
                           orientation = "landscape")

    openxlsx::freezePane(wb_active,
                         sheet = data_sht,
                         firstActiveRow = header_row + 1,
                         firstActiveCol = 2)

    openxlsx::writeDataTable(wb_active,
                             sheet = data_sht,
                             x = df,
                             tableName = sheetname,
                             colNames = TRUE,
                             rowNames = FALSE,
                             tableStyle = "TableStyleLight1",
                             startRow = header_row,
                             withFilter = TRUE,
                             bandedRows = TRUE)

    # format the header
    header_style <- openxlsx::createStyle(textDecoration = "bold",
                                          wrapText = TRUE,
                                          border = "bottom")
    openxlsx::setRowHeights(wb_active,
                            sheet = data_sht,
                            rows = header_row,
                            heights = 12.75 * 3)

    openxlsx::setColWidths(wb_active,
                           sheet = data_sht,
                           cols = 1:ncol(df),
                           widths = 16)

    openxlsx::addStyle(wb_active,
                       sheet = data_sht,
                       style = header_style,
                       rows = header_row,
                       cols = 1:ncol(df))

    openxlsx::setColWidths(wb_active,
                           sheet = data_sht,
                           cols = 1:ncol(df),
                           widths = "auto")

    openxlsx::saveWorkbook(wb_active,
                           file = paste0(fpathname, ".xlsx"),
                           overwrite = TRUE)

  }

  #write the full working directory to remove ambiguity
  if(stringr::str_detect(fpath, stringr::fixed("."))) {
    stringr::str_replace(fpath, stringr::fixed("."), getwd())
  }
  message(paste0(fname, " successfully written to ", fpath))

}



#' write_list_report
#'
#' Export a list of dataframes to an excel workbook. Each sheet contains a
#' dataframe in the list. The list should be named. If not, will be given
#' defaults df_1, df_2, ... df_n.
#'
#' @param df_list the list of dataframes
#' @param output_name_path the full name of the folder and file name to be
#'   exported
#'
#' @return NULL
#' @import openxlsx
#' @export
write_list_report <- function(df_list,
                              output_name_path) {

  if (missing(output_name_path)) {
    output_name_path <- paste0(rstudioapi::selectDirectory(caption = "Select folder for output",
                                                           label = "Select",
                                                           path = "./"),
                               "/",
                               rstudioapi::showPrompt("Enter File Name",
                                                      "Enter File Name",
                                                      default = "default"))

  }

  if (is.null(names(df_list))) {
    warning("Unnamed list written to report.\n",
            "  Assign names to be used for Sheetnames\n",
            "  Using sheetnames df1, df2, ... dfn")
    for (i in seq(length(df_list))) {
      names(df_list)[i] <- paste0("df_", i)}
  }



  wb_active <- openxlsx::createWorkbook()
  #options("openxlsx.borderColour" = "#4F80BD")
  #options("openxlsx.borderStyle" = "thin")
  #options("openxlsx.dateFormat" = "mm/dd/yyyy")

  options("openxlsx.datetimeFormat" = "mm/dd/yyyy")

  openxlsx::modifyBaseFont(wb_active,
                           fontSize = 10,
                           fontName = "Segoe UI")

  # add names to the dataframes in the list if there are none already assigned
  # give a warning to the user that the names were automatically added. these
  # names are used to determine sheetnames and table names
  if (is.null(names(df_list))) {
    n_items <- length(df_list)
    names(df_list) <- letters[1:n_items]
  }
  names(df_list) <- format_sht_names(names(df_list))

  # loop through the dataframes and write them into the file
  # df, wb_active, df_name, opt_header_row
  if (length(df_list) > 1) {
    mapply(create_sheets,
           df = df_list,
           df_name = names(df_list),
           MoreArgs = list(wb_active = wb_active))
  } else {# does anything different need to be done if there is only one df in
    # the list???
    mapply(create_sheets,
           df = df_list,
           df_name = names(df_list),
           MoreArgs = list(wb_active = wb_active))
  }

  openxlsx::saveWorkbook(wb_active,
                         file = paste0(output_name_path, ".xlsx"),
                         overwrite = TRUE)
}

#' format_sht_names ensures that the name to be used for an execl table or
#' worksheet is properly formatted
#'
#' Requries the length to be less than or equal to 31 (sheet max) and replaces
#' all spaces with underscores (table requirement)
#'
#' @param name_vec a string to be used as a worksheet name and/or table name
#'
#' @return the properly formatted string. if no formatting is needed then return
#' the input parameter string.
format_sht_names <- function(name_vec) {
  # the names will be used for the datatables within the workbook. as such, they
  # cannot contain spaces
  modified_names <- gsub("\\s", "_", name_vec)

  #they can also not be greater than 31 characters due to excel limitations
  #if any are the case, throw a warning messsage and truncate that result

  name_lengths <- nchar(modified_names)
  too_long_names_indx <- name_lengths > 31
  if (sum(too_long_names_indx) > 0) {
    warning("Worksheet Name(s) too long. ",
            "Values will be truncated to 31 characters. ",
            "Bad name(s):  \\n", modified_names[too_long_names_indx])
  }

  modified_names[too_long_names_indx] <- substr(modified_names[too_long_names_indx],
                                                start = 1,
                                                stop = 30)
  # remove invalid characters
  replacement_str <- "_"


  # modified_names <- mapply(gsub,
  #                          pattern = regex_bad_strings,
  #                          replacement = replacement_str,
  #                          x = modified_names)
  # this is ugly, but required to remove invalid characters from sheet names
  modified_names <- gsub("\\?",replacement_str, modified_names)
  modified_names <- gsub("\\[",replacement_str, modified_names)
  modified_names <- gsub("\\]",replacement_str, modified_names)
  modified_names <- gsub("\\*",replacement_str, modified_names)
  modified_names <- gsub("\\\\",replacement_str, modified_names)
  modified_names <- gsub("\\/",replacement_str, modified_names)


  return(modified_names)
}

#' check_fix_dupe_name checks a string vector for the existence of a particular
#' string. If found, it modifies the query string so that it does not match an
#' existing string int he vector. It does this be appending an underscore and
#' numeric value.
#'
#' @param name the single string name that will be searched for and modified if
#' necessary
#' @param curr_names the vector of strings that will be searched for the single
#' string 'name'.
#'
#' @return if necessary, a modified string that is not duplicated in the input
#' vector. otherwise, the input name parameter
check_fix_dupe_name <- function(name, curr_names) {
  # check to make sure that the dataframe name isn't already being used as the
  # name of a worksheet. If it is, append a '_x' where x is a number to name.
  # Recheck the length and shorten if necessary.
  while (name %in% curr_names) {
    number_suffix <- 1
    while (paste0(name, "_", number_suffix) %in% curr_names) {
      number_suffix <- number_suffix + 1
    }
    name <- paste0(name, "_", number_suffix)
    if (nchar(name) > 31) {
      suffix_len <- nchar(number_suffix) + 1
      name <- substr(name, start = 1, stop = 31 - suffix_len)
      name <- paste0(name, "_", number_suffix)
    }
  }

  return(name)
}


#' create_sheets inserts a new sheet and data to the input workbook. Contains
#' the code for formatting and styles.
#'
#' @param df the dataframe to be added
#' @param wb_active the workbook into which it will be added
#' @param df_name the name of the worksheet
#' @param opt_header_row an optional header row, default = 2
#'
#' @return NULL as the activeworkbook is being operated on by reference
create_sheets <- function(df, wb_active, df_name, opt_header_row) {
  if (missing(opt_header_row)) {
    header_row <- 2
  } else {
    header_row <- opt_header_row
  }

  curr_sht_names <- names(wb_active)
  if (length(curr_sht_names) > 0) {
    curr_tbl_names <- mapply(openxlsx::getTables,
                             sheet = curr_sht_names,
                             MoreArgs = list(wb = wb_active))
  } else {curr_tbl_names <- c("")}

  df_name <- format_sht_names(df_name)

  df_name <- check_fix_dupe_name(df_name, curr_sht_names)
  df_name <- check_fix_dupe_name(df_name, curr_tbl_names)


  openxlsx::addWorksheet(wb_active,
                         sheetName = df_name,
                         zoom = 85,
                         header = c("Montana State University - Bozeman - ",
                                    df_name,
                                    paste0("Compiled on ", Sys.Date())),
                         orientation = "landscape")

  openxlsx::freezePane(wb_active,
                       sheet = df_name,
                       firstActiveRow = header_row + 1,
                       firstActiveCol = 2)

  openxlsx::writeDataTable(wb_active,
                           sheet = df_name,
                           x = df,
                           tableName = paste0(df_name,"_tbl"),
                           colNames = TRUE,
                           rowNames = FALSE,
                           tableStyle = "TableStyleLight1",
                           startRow = header_row,
                           withFilter = TRUE,
                           bandedRows = TRUE)

  # format the header
  header_style <- openxlsx::createStyle(textDecoration = "bold",
                                        wrapText = TRUE,
                                        border = "bottom")
  openxlsx::setRowHeights(wb_active,
                          sheet = df_name,
                          rows = header_row,
                          heights = 12.75 * 3)
  openxlsx::setColWidths(wb_active,
                         sheet = df_name,
                         cols = 1:ncol(df),
                         widths = "auto")

  openxlsx::addStyle(wb_active,
                     sheet = df_name,
                     style = header_style,
                     rows = header_row,
                     cols = 1:ncol(df))
  return(NULL)
}

