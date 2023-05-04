
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
#' @param copy_to_onedrive_tableau a boolean indicating whether the output
#'   file(s) should be copied to the local onedrive folder for upload to the
#'   cloud
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
                         include_xlsx = TRUE,
                         copy_to_onedrive_tableau = FALSE) {

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
    check_excel_df_dims(df)


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

  if (copy_to_onedrive_tableau == TRUE) {
    copy_report_to_onedrive(fpath_from = fpath, fname_from = fname)
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

#' save_report_to_onedrive
#'
#' Copies file to local OneDrive Folder for upload to the cloud. Expects to find
#' the onedrive location at "C://Users/%USERNAME%//OneDrive - Montana State
#' University//"
#'
#' @param data_to_save the list or dataframe to write to onedrive
#' @param onedrive_folder the onedrive subfolder where the file will be saved.
#'   Defaults to Tableau Extract Source folder. Use NA or an empty string to
#'   save to the onedrive root.
#' @param fname_in the name of the file to save.
#' @param include_xlsx a boolean parameter specificy if an xlsx file should be
#'   written in addition to a flat csv file. Defaults to TRUE.
#' @param create_file_backup a boolean parameter specifying if the function
#'   should save a backup of the existing file if it exists in the specified
#'   onedrive folder. Will save the backup with a name of fname_backup
#' @export
#' @import fs
save_report_to_onedrive <- function(data_to_save,
                                    onedrive_folder = "Tableau Extract Source",
                                    fname_in,
                                    include_xlsx_file = T,
                                    create_file_backup = T) {
  require(fs)
  require(dplyr)

  #if a character vector supplied ot the function, check and see if it already
  #exists in teh calling function's working directory.
  if(is.character(data_to_save)) {
    existing_fname_path <- paste0(getwd(), "/", data_to_save)
    if (!file.exists(existing_fname_path)) {
      stop("supplied character vector to data_to_save parameter but file not
           found in working directory")
    }
  }
  # if it's not a character string representing an already existing file,
  # make sure that it is a datatype that can be written to disk such as
  # a data.frame or a list object
  else if (!is.list(data_to_save) & !is.data.frame(data_to_save)) {
    bad_class_type <- class(data_to_save)
    stop(paste0("input data_to_save parameter must be a data frame or list.
                data_to_save has a ",
                bad_class_type, " class"))
  }

  windows_username <- Sys.getenv("USERNAME")
  message(paste0("Identified windows username as ", windows_username, "\n"))

  #build path to local onedrive location
  fpath_onedrive <- paste0("C:/Users/", windows_username, "/OneDrive - Montana State University/")

  #Check that the folders specified by the input parameters are accessible to R
  if (!dir.exists(fpath_onedrive)) {
    stop(paste0("Could not find OneDrive folderpath at ", fpath_onedrive))
  } else {
    message("Identifid OneDrive folder path as: \n", fpath_onedrive, "\n")
  }

  if (!is.na(onedrive_folder) & !onedrive_folder == "") {
    fpath_onedrive_full <- paste0(fpath_onedrive, onedrive_folder, "/")
    if (!dir.exists(fpath_onedrive_full)) {
      stop(paste0("Could not find OneDrive folderpath at ", fpath_onedrive_full))
    }
  } else {
    fpath_onedrive_full <- fpath_onedrive
  }

  full_path_name <- paste0(fpath_onedrive_full, fname_in)

  # check if a backup needs to be made
  if (create_file_backup == T) {
    if (file_exists(paste0(full_path_name, ".csv"))) {
      file_copy(paste0(full_path_name, ".csv"),
                paste0(full_path_name, "_backup.csv"),
                overwrite = T)
    }
    if (file_exists(paste0(full_path_name, ".xlsx"))) {
      file_copy(paste0(full_path_name, ".xlsx"),
                paste0(full_path_name, "_backup.xlsx"),
                overwrite = T)
    }
  }

  if (is.data.frame(data_to_save)) {
    opa::write_report(data_to_save,
                      fpath = fpath_onedrive_full,
                      fname = fname_in,
                      sheetname = "df",
                      include_xlsx = include_xlsx_file)
  } else if (is.list(data_to_save)){
    opa::write_list_report(data_to_save,
                           full_path_name)
  }

  message(paste0("Wrote ", class(data_to_save), " to ", full_path_name, "\n\n"))
  return(invisible(NULL))
}


#' Email a report
#'
#' @param email_recipient_address standard email address
#' @param email_subject character subject line
#' @param email_body character body content
#' @param email_attachment_fpath file to be attached prior to sending
#' @param send_or_display set to 'send' to automatically send the email. set to
#'   anything else to display the email prior to sending.
#'
#' @return NULL
#' @export
email_report <- function(email_recipient_address,
                         email_subject,
                         email_body,
                         email_attachment_fpath,
                         send_or_display = "send") {

  # https://stackoverflow.com/questions/26811679/sending-email-in-r-via-outlook
  #
  # https://docs.microsoft.com/en-us/office/vba/api/Outlook.MailItem
  #
  require(RDCOMClient)

  ## init com api
  OutApp <- COMCreate("Outlook.Application")
  ## create an email
  outMail = OutApp$CreateItem(0)
  ## configure  email parameter
  outMail[["To"]] = email_recipient_address
  outMail[["subject"]] = email_subject
  outMail[["body"]] = email_body
  outMail[["Attachments"]]$Add(email_attachment_fpath)

  if (send_or_save == "send") {
    ## send it
    email_result <- outMail$Send()
    if (email_result == TRUE) {
      msg("Suceesfully emailed report")
    }
  } else {
    outMail$Display()
  }
  return(NULL)
}

#' write out a properly formatted salary report
#'
#' @param requestor_name the nanme of the requestor
#' @param data_source a text descriptor of the data source
#' @param opt_sal_report a prebuilt salary report. will pull automatically if
#'   not provided
#' @param opt_fy an optional fiscal year if the sal report is not provided
#' @param opt_dw_conn an optional datawarehouse connection containing snapshots
#'
#' @return
#' @export
write_sal_report <- function(requestor_name,
                             data_source = "Annual Employee Snapshot - Oct 15, ",
                             opt_sal_report,
                             opt_fy,
                             opt_dw_conn) {
  require(lubridate)
  require(tidyverse)

  if (missing(opt_sal_report)) {
    if (missing(opt_dw_conn)) {
      dw_conn <- opa::get_postgres_conn()
    } else {
      dw_conn <- opt_dw_conn
    }

    if (missing(opt_fy)) {
      sysdate <- lubridate::today()
      fy <- opa::calc_fiscal_year(sysdate)
    } else {
      fy <- opt_fy
    }

    sal_report <- opa::pull_dw_salary(dw_conn = dw_conn,
                                      fy = fy)
  } else {
    sal_report <- opt_sal_report
  }


  wb_active <- openxlsx::createWorkbook()
  options("openxlsx.datetimeFormat" = "mm/dd/yyyy")






}

#' Check that dataframe dimensions do not exceed excels row/column limitations
#'
#' @param df the dataframe to be checked
#'
#' @return nothing if runs successfully
#' @export
check_excel_df_dims <- function(df) {
  #check that the number of rows, columns will fit in excel
    excel_max_row <- 1048576
    excel_max_col <- 16384

    if (nrow(df) >  excel_max_row) {
      stop(paste0("df contains ", nrow(df), " rows. Exceeds Excel max by ", nrow(df) - 1048576))
    }
    if (ncol(df) >  excel_max_col) {
      stop(paste0("df contains ", ncol(df), "cols. Exceeds Excel max by ", ncol(df) - 16384))
    }

}

#' Work in progress - do not use
#'
#' @return NULL
#' @export
write_detailed_report <- function() {

  # build xlsx workbook
  wb_active <- openxlsx::createWorkbook()

  # build workbook styles
  header_style <- openxlsx::createStyle(textDecoration = "bold",
                                        wrapText = F,
                                        border = "BottomLeftRight",
                                        borderStyle = "medium")



}


#' Write a dataframe to a database. Checks to ensure that sql key-words are not
#' used as column names.
#'
#' @param df the dataframe to write to the database
#' @param tbl_name the character table name used in teh database
#' @param append a boolean value specifying if the dataset should be appended to
#'   already existing data in the database table
#' @param overwrite a boolean value specifying if the dataset should overwrite
#'   already existing data in teh database table
#' @param pg_conn the database connection. Typically a postgres connection to
#'   the local datawarehouse. See `opa::get_postgress_conn()` function.
#'
#' @return NULL
#' @export
write_df_to_postgres <- function(df, tbl_name, append = F, overwrite= F, pg_conn) {
  require(DBI)
  require(tictoc)

  # Ensure that both append and overwrite are not set to TRUE
  if(append == T & overwrite == T) {
    stop("Both append and overwrite are set to TRUE. Only one may be TRUE")
  }

  # Ensure that one of append or overwrite is set to TRUE if a table already exists
  # in the database
  tbls_in_db <- DBI::dbListTables(dw_conn)
  if(tbl_name %in% tbls_in_db & append == F & overwrite == F) {
    stop(paste0(tbl_name, " exists in database and both append and overwrite set to FALSE"))
  }

  # load the reserved sql reserved keywords table
  tic("Pulled SQL keywords from https://www.postgresql.org/docs/current/sql-keywords-appendix.html")
  sql_keywords <- load_sql_reserved_keywords()
  toc()

  # stop if the dataframe contains key words. This uses a broad approach and stops any sql key word from being used.
  # the sql_keywords dataframe includes reserved and non-reserved status for each word in varying sql implementations.
  if(any(names(df) %in% sql_keywords$`Key Word`)) {
    bad_names <- names(df)[names_df %in% sql_keywords$`Key Words`]
    stop(cat(paste("The following column names are SQL key-words: ",
                   paste(bad_names, collapse = "\n"),
                   sep = "\n")))
  }

  # Explicitly tell the user how the function is interacting with the database
  if(!tbl_name %in% tbls_in_db) {
    tic("Wrote df to new ", tbl_name, " table in database")
  } else if(append == T) {
    tic("Appended df to ", tbl_name)
  } else if(overwrite == T) {
    tic("Overwrote ", tbl_name, " with df")
  }

  dbWriteTable(pg_conn, tbl_name, df,append = append, overwrite = overwrite)
  toc()

  return(invisible())
}

load_sql_reserved_keywords <- function() {
  require(rvest)
  require(lubridate)

  # URL of the website containing the table
  url <- "https://www.postgresql.org/docs/current/sql-keywords-appendix.html"

  # Read the HTML content of the website
  page <- read_html(url)

  table_data <- page %>%
    html_nodes("#KEYWORDS-TABLE table") %>%
    html_table()

  df <- table_data[[1]]
  df <- replace(df, df == "", NA)

  # Convert the HTML table to a data frame
  df <- table_data[[1]]

  names(df) <-

  # Print the first 10 rows of the data frame
  return(df)
}
