#' Get a Banner database connection object
#'
#' @description Connects to the MSU Production Oracle Banner Database. Requires
#'   valid Banner username and password credentials. Requires appropriately
#'   configured tnsnames.ora file. Depends on ROracle to create Oracle database
#'   driver (aka OraDriver). ROracle must be installed from:
#'   `https://www.oracle.com/database/technologies/appdev/roracle.html` See also
#'   `https://stackoverflow.com/questions/61935988/alternative-to-odbc-package-for-oracle-r`
#'
#'
#'
#'
#' @param opt_pword WARNING !!! password stored as plaintext until completion of
#'   connection attempt.
#'
#' @param verbose boolean specifying if the script should use the rstudio api to
#'   launch a username popup dialogue
#'
#' @return a connection object that can be used to execute operations on the db
#' @export
#' @author Ian C Johnson
get_banner_conn <- function(opt_pword,
                            verbose = T) {

  require(keyringr)

  # default set to UTC when pulling dates from banner. This sets the environment
  # so that the DBI package properly handles date conversions.
  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")


  if (verbose == TRUE) {
    username <- rstudioapi::showPrompt(title = "BANNER User Name",
                                       message = "Enter user name here: ",
                                       default = "IJOHNSON")
  } else {
    username <- "IJOHNSON"
  }

  # See https://github.com/cran/keyringr
  # https://stackoverflow.com/questions/58409378/safely-use-passwords-in-r-files-prevent-them-to-be-stored-as-plain-text
  credential_label <- paste0("BANNER_", username)
  # credential_path <- paste0(Sys.getenv("USERPROFILE"),
  #                           "\\Documents\\DPAPI\\passwords\\",
  #                           Sys.info()["nodename"],
  #                           "\\", credential_label, ".txt")

  credential_path <- paste0("\\\\helene\\opa$\\icj_dts\\DPAPI\\passwords\\",
                            Sys.info()["nodename"],
                            "\\", credential_label, ".txt")

  if (missing(opt_pword)) {
    #pwd <- rstudioapi::askForPassword(prompt = "Enter BANNER password here: ")
    pwd <- keyringr::decrypt_dpapi_pw(credential_path)
  } else {
    pwd <- opt_pword
  }

#
#   ora_conn_drvr <- ROracle::Oracle()
#   # db_conn <- ROracle::dbConnect(ora_conn_drvr,
#   #                               username,
#   #                               pwd,
#   #                               dbname = "PROD")
#
#   #db_name <- "odaprod-scan.msu.montana.edu:1521/PROD.MSU"
#   #db_name <- "prod.db.msuprodnet.oraclevcn.com:1521/PROD"
#
#   #new db_name for oracle cloud transition
#   db_name <- "10.210.1.11:1521/PROD"
#   db_conn <- ROracle::dbConnect(ora_conn_drvr,
#                                 user = username,
#                                 password = pwd,
#                                 host = "proddbs.db.msuprodnet.oraclevcn.com",
#                                 port =  1521,
#                                 dbname = "PROD",
#                                 service = "prod.db.msuprodnet.oraclevcn.com")
#
#   # conn <- ROracle::dbConnect(ora_conn_drvr,
#   #                   username = username,
#   #                   password = pwd,
#   #                   dbname = "PROD",
#   #                   host = "proddbs.db.msuprodnet.oraclevcn.com",
#   #                   port = 1521,
#   #                   service = "prod.db.msuprodnet.oraclevcn.com")
#   #
#   #
#   #

  # See Sys.getenv("TNS_ADMIN") for location of the tnsnames.ora file that
  # defines the location of the PROD database
  ora_conn_drvr <- ROracle::Oracle()
  db_conn       <- ROracle::dbConnect(ora_conn_drvr,
                                      user = username,
                                      password = pwd,
                                      dbname = "PROD")



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
#' @return a connection object that can be used to execute operations on the db
#' @export
#' @author Ian C Johnson
get_access_conn <- function(db_file_path = "X:/Employees/EmployeesFY22.accdb") {
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

#' Connect to a PostgreSQL db. Defaults to localhost ee_dw database.
#'
#' Currently only installed locally. Hardcoded connection to the localhost and
#' db named `ee_dw`. Used to prototype datawarehouse designs.
#'
#' @param host_in The tns name or ip address of the database. defaults to
#'   `localhost`
#' @param dbname_in The database name. defaults to `ee_dw`
#' @param schema_in an optional character string specifying any schema to add to
#'   the db search path
#' @param username_in A valid username setup in the database
#' @param verbose a boolean determining if the function will prompt the user to
#'   enter a username and password. defaults to false and uses the defaul
#'   username and password stored in teh local keyringr DPAPI folder source1
#' @param opt_pword An optional password string value. Will prompt for password
#'   if none is supplied to functio
#'
#' @return a connectoin object to the locally hosted postgresql db
#' @export
get_postgres_conn <- function(host_in = "localhost",
                              dbname_in = "ee_dw",
                              schema_in = "hr",
                              username_in = "postgres",
                              verbose = F,
                              opt_pword) {


  require(RPostgres)
  require(DBI)
  require(keyringr)

  if (verbose == TRUE) {
    username <- rstudioapi::showPrompt(title = "BANNER User Name",
                                       message = "Enter user name here: ",
                                       default = "postgres")
  } else {
    username <- "postgres"
  }

  # See https://github.com/cran/keyringr
  # https://stackoverflow.com/questions/58409378/safely-use-passwords-in-r-files-prevent-them-to-be-stored-as-plain-text
  credential_label <- paste0("LOCALDB_", username)
  # credential_path <- paste0(Sys.getenv("USERPROFILE"),
  #                           "\\Documents\\DPAPI\\passwords\\",
  #                           Sys.info()["nodename"],
  #                           "\\", credential_label, ".txt")

  credential_path <- paste0("C:\\Users\\iancj\\Documents\\DPAPI\\passwords\\",
                            Sys.info()["nodename"],
                            "\\", credential_label, ".txt")

  if (missing(opt_pword)) {
    #pwd <- rstudioapi::askForPassword(prompt = "Enter BANNER password here: ")
    pwd <- keyringr::decrypt_dpapi_pw(credential_path)
  } else {
    pwd <- opt_pword
  }

  con <- dbConnect(RPostgres::Postgres(),
                   host = host_in,
                   port = 5432,
                   dbname = dbname_in,
                   user = username,
                   password = pwd,
                   options = paste0("-c search_path=", schema_in, ",public"),
                   timezone = "UTC",
                   timezone_out = "UTC")

  return(con)


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

get_oche_dw_conn <- function() {

 user         <- "IJOHNSON"
 host_name    <- "ochedwp.mus.edu"
 port         <- 1521
 service_name <- "msusdwp.db.msuprodnet.oraclevcn.com"
 driver       <- "Oracle in OraClient11g_home1"

# Assemble connection strings
#
 db_connect_string <- paste0(driver_string, dbq_string)

   db_name <- paste0(service_name, ":", port)

 db_conn <- ROracle::dbConnect(ora_conn_drvr,
                               username,
                               pwd,
                               dbname = db_name)


 # RODBC::odbcDriverConnect()
 # "DRIVER=Oracle in OraClient11g_home1;UID=ijohnson;PWD=PASSWORD;DBQ=//HOSTNAME:PORT/ORACLE_SID;
 #
 #
 # tst <- RODBC::odbcDriverConnect()
}


password_encryption_tst <- function() {
  library(keyringr)
  credential_label <- "BANNER_IJOHNSON"
  credential_path <- paste0( "C:\\Users\\d66x816\\Documents\\DPAPI\\passwords\\", Sys.info()["nodename"], "\\", credential_label, ".txt")
  my_pwd <- decrypt_dpapi_pw(credential_path)
  print(my_pwd)
}