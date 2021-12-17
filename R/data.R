#' Longevity pay percent bonus by years of service
#'
#' A dataset containing the percent bonus for each year of service (time since
#' PEBEMPL_CURRENTY_HIRE_DATE) rounded to the previous payroll date.
#'
#' @format A data frame with 101 rows and 2 variables: \describe{
#'   \item{YearsOfService}{integer years of service calculated from Current Hire
#'   Date} \item{PercentToBase}{the percent bonus ranging from 0 to .21} ... }
#' @source \url{https://www.montana.edu/policy/hr_policies/longevity_allowance.html}
"longevity_rates"

#' Deptartment head names with associated colleges
#'
#' A data set containing the proper names of each department head with their
#' associated department and college.
#'
#' @format A data frame with 38 rows and 4 variables: \describe{
#'   \item{College}{character vector containing formal college name}
#'   \item{Dept}{character vector containing the formal Dept name}
#'   \item{Name}{the employee name currently holding the dept head position}
#'   \item{as_of_date}{the as-of-date for the dataset. Frequently changes as
#'   individuals move into or out of the Dept Head role} ... }
#' @source
#' \url{https://www.montana.edu/opa/facts/admin.html}
"dept_head_names"


#' Term Dates
#'
#' A daily date sequence containing the term-code, term-description, term start
#' and term term end dates associated with a particular date. Due to date
#' periods not contained in a term, the assigned term is carried forward until
#' the next term starts. To remove these carried values, filter by
#' `!as_of_date %within% term_date_int`. `%within%` found in the lubridate
#' package.
#'
#' @format A data frame with 9483 rows and 6 variables:
#' \describe{
#'   \item{as_of_date}{the date of the sequence. Serves as the primary key}
#'   \item{term_code}{character vector containing the term code in the format
#'   YYYY30, YYYY50, and YYYY70 associated with spring, summer, and fall
#'   respectively}
#'   \item{term_start_date}{POSIXct vector containing the term start date}
#'   \item{term_end_date}{POSIXct vector containing the term end dates}
#'   \item{term_date_int}{date interval vector defined by the term start and
#'   end date}
#'   ...
#'}
"term_dates"