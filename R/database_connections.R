#' Get a Banner database connection object
#'
#' @description Connects to the MSU Production Oracle Banner Database. Requires
#'   valid Banner username and password credentials. Requires appropriately
#'   configured tnsnames.ora file. Depends on ROracle to create Oracle database
#'   driver (aka OraDriver).
#'
#' @param opt_pword WARNING !!! password stored as plaintext until completion of
#'   connection attempt.
#'
#' @return a connection object that can be used to exectute operations on the db
#' @export
#' @author Ian C Johnson
get_banner_conn <- function(opt_pword,
                            verbose = T) {
  # default set to UTC when pulling dates from banner. This sets the environment
  # so that the DBI package properly handles date conversions.
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")


  if(verbose == TRUE) {
    username <- rstudioapi::showPrompt(title = "BANNER User Name",
                                       message = "Enter user name here: ",
                                       default = "IJOHNSON")
  } else {
    username <- "IJOHNSON"
  }

  if (missing(opt_pword)) {
    pwd <- rstudioapi::askForPassword(prompt = "Enter BANNER password here: ")
  } else {
    pwd <- opt_pword
  }


  ora_conn_drvr <- ROracle::Oracle()
  # db_conn <- ROracle::dbConnect(ora_conn_drvr,
  #                               username,
  #                               pwd,
  #                               dbname = "PROD")

  db_name <- "odaprod-scan.msu.montana.edu:1521/PROD.MSU"
  db_conn <- ROracle::dbConnect(ora_conn_drvr,
                                username,
                                pwd,
                                dbname = db_name)

  #See https://docs.oracle.com/database/121/ADMQS/GUID-1A15D322-B3AC-426A-86A1-EB7590930687.htm#ADMQS045
  #"host[:port][/service_name][:server][/instance_name]"

  pwd <- ""
  rm(pwd)
  return(db_conn)
}

#' Get an Access database connection object
#'
#' @description Get a connection to local or network stored Access db. The
#'   connection allows for basic operations to be made on the access database.
#'
#' @param db_file_path A full file path to the access database including the
#'   file name. Default value should be updated to reflect the most recent
#'   Employees annual snapshot file
#'
#' @return a connection object that can be used to exectute operations on the db
#' @export
#' @author Ian C Johnson
get_access_conn <- function(db_file_path = "X:/Employees/EmployeesFY21.accdb") {
  suppressPackageStartupMessages({
      require(DBI)
    })

  # default set to UTC when pulling dates from banner. This sets the environment
  # so that the DBI package properly handles date conversions.
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  # make sure that the file exists before attempting to connect
  if (!file.exists(db_file_path)) {
    stop("DB file does not exist at ", db_file_path)
  }

  # Assemble connection strings
  dbq_string <- paste0("DBQ=", db_file_path)
  driver_string <- "Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
  db_connect_string <- paste0(driver_string, dbq_string)

  myconn <- dbConnect(odbc::odbc(),
                      .connection_string = db_connect_string)
  return(myconn)
}

#' Get all table names from a given database
#'
#' @description Currently built to return Access (DBI connector) and Oracle (ROracle)
#' database table names.
#'
#' @param db_conn the database connection object
#'
#' @return a text character vector of table names
#' @export
#' @author Ian C Johnson
get_db_table_names <- function(db_conn) {
  suppressPackageStartupMessages({
    require(DBI)
    require(ROracle)
  })
  if (inherits(db_conn, "ACCESS")) {
    names <- DBI::dbListTables(db_conn)
  } else if (inherits(db_conn, "OraConnection")) {
    names <- ROracle::dbListTables(bann_conn, all = T)
  }

  return(names)
}

#' Get the names of all columns in a given database table.
#'
#' @description Given a database table which exists in said database, get the
#'   column names. This is particularly useful when exploring less familiar
#'   tables and databases.
#'
#' @param tbl_name a character string containing the database table name. Case
#'   specific.
#' @param db_conn an Access or Oracle database connection object.
#'
#' @return a character vector containing the names of the fields
#' @seealso get_db_table_names
#' @export
#' @author Ian C Johnson
get_db_table_col_names <- function(tbl_name, db_conn) {
  suppressPackageStartupMessages({
    require(dplyr)
  })

  # must be an Access connection or an oracle database connection
  stopifnot(inherits(db_conn, "ACCESS") | inherits(db_conn, "OraConnection"))

  #ensure that the table names input parameter exists in the database
  names <- tbl(db_conn, tbl_name) %>% colnames()

  return(names)
}
