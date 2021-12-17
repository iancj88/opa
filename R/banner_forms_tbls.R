#' Banner Forms-Tables Crosswalk dataset
#'
#' A list of all table datasources used to populate a given banner form.
#'
#' @return a dataframe comprised of FORM NAME, FORM DESC, TABLE ACCESSED and
#'   TABLE MODULE
#' @export
bann_form_tbl_xwalk <- function() {
  #load banner doc forms
  #
  #
  #
  require(readxl)
  require(tictoc)
  require(tidyverse)

  shts <- readxl::excel_sheets("X:/icj_dts/Documentation/BannerFormsFields/Banner_Forms_Tables.xls")
  tic("load data from worksheets")
  bann_forms_tbls <- mapply(read_excel,
                            sheet = shts,
                            MoreArgs = list( path = "X:/icj_dts/Documentation/BannerFormsFields/Banner_Forms_Tables.xls"),
                            SIMPLIFY = F)
  toc()
  names(bann_forms_tbls) <- shts

  bann_forms_tbls <- bind_rows(bann_forms_tbls, .id = "sht_name")

  tic("loop through rows to carry Form name/descriptions forward")
  # loop through each row carrying the last non-NA value of the form forward.
  for (i in 1:nrow(bann_forms_tbls)) {
    if(!is.na(bann_forms_tbls[i, "FORM"])) {
      curr_form_name <- bann_forms_tbls[i, "FORM"]
    } else {
      bann_forms_tbls[i, "FORM"] <- curr_form_name
    }
  }

  toc()
  # currently the form name and desc is held in a single value of the form
  # FORM NAME (FORM DESCRIPTION)
  # Form names are always 6 characters
  # Form descriptions are always in a () bracket
  tic("separate Form name and description")
  bann_forms_tbls <- mutate(bann_forms_tbls,
                            FORM_NAME = substr(FORM, 1, 7),
                            FORM_DESC = substr(FORM, 10, nchar(FORM)-1)) %>%
    relocate(FORM_NAME, FORM_DESC, .before = FORM) %>%
    select(-FORM)

  toc()



  return(bann_forms_tbls)

}

#' Banner Tables-Forms Crosswalk dataset
#'
#' A list of all forms using a given table as a datasource
#'
#' @return a dataframe comprised of TABLE, FORM NAME, and FORM DESC
#' @export
bann_tbl_form_xwalk <- function() {
  require(readxl)
  require(tictoc)

  tic("load data from worksheets")
  shts <- readxl::excel_sheets("X:/icj_dts/Documentation/BannerFormsFields/Banner_Table_Forms.xls")
  bann_forms_tbls <- mapply(read_excel,
                            sheet = shts,
                            MoreArgs = list( path = "X:/icj_dts/Documentation/BannerFormsFields/Banner_Table_Forms.xls"),
                            SIMPLIFY = F)


  toc()
  names(bann_forms_tbls) <- shts

  bann_forms_tbls <- bind_rows(bann_forms_tbls, .id = "sht_name")

  tic("loop through rows to carry Tble name/descriptions forward")
  # loop through each row carrying the last non-NA value of the form forward.
  for (i in 1:nrow(bann_forms_tbls)) {
    if(!is.na(bann_forms_tbls[i, "TABLE"])) {
      curr_form_name <- bann_forms_tbls[i, "TABLE"]
    } else {
      bann_forms_tbls[i, "TABLE"] <- curr_form_name
    }
  }

  toc()


  tic("separate Form name and description")
  bann_forms_tbls <- rename(bann_forms_tbls,
                            FORM = `ACCESSED BY FORM`) %>%
    mutate(FORM_NAME = substr(FORM, 1, 7),
           FORM_DESC = substr(FORM, 10, nchar(FORM)-1)) %>%
    relocate(FORM_NAME, FORM_DESC, .before = FORM) %>%
    select(-FORM) %>%
    filter(!is.na(FORM_NAME))

  toc()

  return(bann_forms_tbls)
}