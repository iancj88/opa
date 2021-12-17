#' Supplement ATS EEO-6 Dataset
#'
#' @description determine veteran and disability status for each applicant from
#'   the ATS EEO-6 report. Does not error check for missing columns. This is
#'   necessary because, historically, different fields have been used to store
#'   teh same attributes. Furthermore, the field names do not indicate which is
#'   'correct' or even in current use.
#'
#' @param ats_data the dataset from the ATS reporting system. Custom report
#'   titled 'EEO Applicant Details Report'.
#'
#' @return a dataframe containing input ats dataset with modified column names
#'   to standardize modifications to ATS (ada_1,ada_2... instead of full
#'   sentences)
#' @export
supplement_ats <- function(ats_data) {

  ats_data_out <- ats_data %>%
    rename(ada_1 = `Will You Require A Reasonable Accommodation To Perform The Duties Of The Position You Are Applying For?`,
           ada_2 = `Will You Require A Reasonable Accommodation To Perform The Duties Of The Position You Are Applying For?_1`,
           ada_3 = `Please Select One_1`,
           ada_4 = `Please Select One`,
           vet_1 = `Lookup Veteran Status`,
           vet_2 = `Protected Veteran Self Identification`)


  ats_data_out <- ats_data_out %>%
    mutate(ada_out = mapply(determine_ada_status,
                            ats_data_out$ada_1,
                            ats_data_out$ada_2,
                            ats_data_out$ada_3,
                            ats_data_out$ada_4),
           vet_out = mapply(determine_vet_status,
                            ats_data_out$vet_1,
                            ats_data_out$vet_2))
  return(ats_data_out)
}

#' Determine ADA status from ATS applicant record
#'
#' @description helper function for the exported `supplement_ats` function
#'
#' @param f1 ada field one name
#' @param f2 ada field two name
#' @param f3 ada field three name
#' @param f4 ada field four name
#'
#' @return a character 'status' string of 'Y', 'N', or 'Non-Response'.
determine_ada_status <- function(f1, #field one
                                 f2, #field two...
                                 f3,
                                 f4) {
  yes_responses <- c("Yes", "Yes, I have a disability (or previously had a disability)")
  no_responses <- c("No", "No, I don't have a disability")
  na_responses <- c(NA, "I don't wish to answer" )

  #look through each field for a yes response. if non found, look for no
  #response. Lastly, if still nothing, assign unknown
  status <- ifelse(f1 %in% yes_responses |
                     f2 %in% yes_responses |
                     f3 %in% yes_responses |
                     f4 %in% yes_responses,
                   "Y",
                   ifelse(f1 %in% no_responses |
                            f2 %in% no_responses |
                            f3 %in% no_responses |
                            f4 %in% no_responses,
                          "N",
                          "No Response"))
  return(status)
}

#' Determine Vet status from ATS applicant record
#'
#' @description helper function for the exported `supplement_ats` function
#'
#' @param f1 ?
#' @param f2 ?
#'
#' @return a character 'status' string of 'Y', 'N', or 'Non-Response'.
determine_vet_status <- function(f1,
                                 f2) {

  yes_responses <- c("I identify as one or more of the classifications of protected veteran listed above.")
  no_responses <- c("I am not a protected veteran.")

  #look through each field for a yes response. if non found, look for no
  #response. Lastly, if still nothing, assign unknown
  status <- ifelse(f1 %in% yes_responses |
                     f2 %in% yes_responses,
                   "Y",
                   ifelse(f1 %in% no_responses |
                            f2 %in% no_responses,
                          "N",
                          "No Response"))

  return(status)
}
