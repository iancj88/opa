
#' Build the FTVORGN Hierarchy for each unique Organization Code/Number
#'
#' @description Build a lookup table storing each unique org code's hierarchy wihtin the BZ
#' organization. BZ campus filters are currently hardcoded. Will break if the
#' org hierarchy depth ever exceeds 8
#'
#' @param as_of_date the date used to filter the FTVORGN data to ensure that
#'   historical or future records are not used. Defaults to Sys.Date()
#' @param opt_bann_conn an optional banner connection object
#' @param opt_ftvorgn_data an optional ftvorgn dataframe in the format supplied
#'   by msoupa::get_ftvorgn_data. If no supplied, will pull.
#' @param include_names a boolean specifying whether to join org titles to the
#'   output dataframe
#' @param new_col_name a predicate to use on teh new column names. Defaults to
#'   'Org' creating columns titled 'Org1', 'Org2',... 'Org7'
#'
#' @return a dataframe containing a 'seed' column containing the original org
#'   code used to build the hierarchy. Hierarchy is specified from least to most
#'   granular. Org1 is always 400000, i.e. "montana state university
#'   -bozeman".Org2 then represents the Division, Org3 college, Org4 dept, etc.
#'   If titles are included, each column includes an additional Orgx_desc,
#'   seed_desc, ... column containing the unit's name
#' @export
build_org_hierarchy_lu <- function(as_of_date = Sys.Date(),
                                   opt_bann_conn,
                                   opt_ftvorgn_data,
                                   include_names = T,
                                   new_col_name = "Org",
                                   detailed_col_names = F) {

# initialize script -------------------------------------------------------
  require(tictoc)
  require(pkgcond)

  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")

  if (missing(opt_bann_conn)) {
    bann_conn <- get_banner_conn()
  } else {
    bann_conn <- opt_bann_conn
  }

  if (missing(opt_ftvorgn_data)) {
    ftvorgn_data <- opa::get_ftvorgn_data(opt_bann_conn = bann_conn)
  } else {
    ftvorgn_data <- opt_ftvorgn_data
  }

  # If no as_of_date supplied, use curent Sys date,
  # else ensure that the date is in the POSIXct class
  #
  # either this will work beautifully or crash spectacularly.
  if (missing(as_of_date)) {
    as_of_date <- as.POSIXct(Sys.Date())
  } else {
    as_of_date <- as.POSIXct(as_of_date)
  }

  tic_msg <-  paste0("Building org LU table as of ", as_of_date)
  tic(tic_msg)
# Build Unformatted Hierarchy and Name LU ---------------------------------

  ftv_dated <- ftvorgn_date_filter(as_of_date, ftvorgn_data)

  unformatted_hier <- pkgcond::suppress_warnings(build_org_hierarchy(ftv_dated,
                                                                     new_col_name = new_col_name),
                                                 pattern = "parent org number of 62")

  code_org_name_lu <- get_code_title_lu(ftv_dated)


# Format Hierarchy for export ---------------------------------------------

  formatted_hier <- format_org_df(unformatted_hier,
                                  include_names,
                                  ftvorgn_code_title = code_org_name_lu)

  formatted_hier$date <- as_of_date


  if (detailed_col_names == T) {
    formatted_hier <- formatted_hier %>%
      arrange(Org1, Org2, Org3, Org4, Org5, Org6, Org7) %>%
      select(seed_org_number = seed,
             seed_org_desc = seed_desc,
             Campus = Org1_desc,
             Division = Org2_desc,
             `College/Unit` = Org3_desc,
             Dept = Org4_desc,
             `Sub-Dept` = Org5_desc,
             `Sub-Dept2` = Org6_desc,
             `Sub-Dept3` = Org7_desc)

  }
  toc()

  return(formatted_hier)
}


#' Filter FTVORGN table by effective date.
#'
#' @description Use to exclude future and historical data as specified by
#' the as_of_date param.
#'
#' @param as_of_date the date used to determine historical, current, and future
#'   records
#' @param ftvorgn_data the table of FTVORGN data as pulled by SELECT * FROM
#'   FTVORGN
#'
#' @return a dataframe containing a single row for each unique org code.
#'   Additional rows would indicate that historical or future data was
#'   erroneously included.
#'
ftvorgn_date_filter <- function(as_of_date_in,
                                ftvorgn_data) {
  require(dplyr)
  # check that the necessary columns exist in the dataset
  # Returns boolean 0,1 values for each column name that is or is not in the
  # dataframe column name vector. Multiplying them together reveals if any returned
  # FALSE or 0.
  #
  stopifnot(prod(c("FTVORGN_EFF_DATE",
                   "FTVORGN_NCHG_DATE",
                   "FTVORGN_ORGN_CODE",
                   "FTVORGN_TERM_DATE") %in% names(ftvorgn_data)) > 0)

  Sys.setenv(TZ = "UTC")
  Sys.setenv(ORA_SDTZ = "UTC")


  as_of_date <- as.POSIXct(as_of_date_in)

  stopifnot("POSIXct" %in% class(as_of_date_in))

  orgn_out <- dplyr::filter(ftvorgn_data,
                            as.POSIXct(FTVORGN_EFF_DATE) <= as.POSIXct(as_of_date_in),
                            as.POSIXct(FTVORGN_TERM_DATE) > as.POSIXct(as_of_date_in) | is.na(FTVORGN_TERM_DATE))

  orgn_out_max_dates <- orgn_out %>%
    group_by(FTVORGN_ORGN_CODE) %>%
    filter(as.POSIXct(FTVORGN_EFF_DATE) <= as.POSIXct(as_of_date_in)) %>%
    summarize(max_eff_date = max(FTVORGN_EFF_DATE)) %>%
    mutate(org_eff_key = paste0(FTVORGN_ORGN_CODE, max_eff_date))

  orgn_out <- dplyr::mutate(orgn_out,
                            org_eff_key = paste0(FTVORGN_ORGN_CODE, FTVORGN_EFF_DATE))

  orgn_out <- dplyr::filter(orgn_out,
                            as.POSIXct(FTVORGN_EFF_DATE) <= as.POSIXct(as_of_date_in),
                            org_eff_key %in% orgn_out_max_dates$org_eff_key,
                            #as.POSIXct(FTVORGN_NCHG_DATE) > as.POSIXct(as_of_date_in),
                            as.POSIXct(FTVORGN_TERM_DATE) > as.POSIXct(as_of_date_in) | is.na(FTVORGN_TERM_DATE))



  orgn_out <- dplyr::mutate(orgn_out, "date" = as_of_date_in)

  if (!dplyr::n_distinct(orgn_out) == nrow(orgn_out)) {
    warning(paste0("Too many rows returned for ftvorgn using as_of_date = ", as_of_date_in))
  }

  return(orgn_out)
}


#' Build an unstandardized Org Hierarchy
#'
#' The ftvorgn table uses FTVORGN_ORGN_CODE and FTVORGN_PRED_CODE to form
#' hierarchical relationship between organization numbers. For each unique org
#' code, i.e. row, follow the predecessor codes until it reaches the top of the
#' organization. Requires the format_org_df to be useable outside of the
#' package.
#'
#'
#' @param ftvorgn_data the ftvorgn corresponding to a single as-of date
#' @param new_col_name the predicate for the new columns i.e. org, orgn, etc...
#'
#' @return the unformatted hierarchy with each row including a seed value and
#'   the corresponding org codes leading to the top of hte hierarchy as
#'   determined by the PRED code.
build_org_hierarchy <- function(ftvorgn_data,
                                new_col_name) {

  require(dplyr)

  #for each unique dept org number, a hierarchy will be recursively determined.
  # Using a list of each uniqe dept
  #Only look at BZ orgs that start with a '4'
  dept_orig <- as.character(unique(ftvorgn_data$FTVORGN_ORGN_CODE))

  CoN_org_titles <- c("4M6110", "4M6109", "4M6107") #college of nursing

  non_standard_bz_orgs <- c("AUXADM",
                            "RLHAL",
                            "ST UN",
                            "RLDINS",
                            "FMLHS",
                            "SPOFAC",
                            "TICAUX",
                            "FDSVC",
                            "CONCS",
                            "CONFSV",
                            "STUHL",
                            "BUDGET")

  dept_unique <- dept_orig[#(substr(dept_orig,1,1) == "4" | dept_orig %in% non_standard_bz_orgs) & #look only at BZ orgs
                             !(substr(dept_orig,1,2) == "4M" & !dept_orig %in% CoN_org_titles) & #don't include 4MAIL, do include 4M CoN orgs
                             !substr(dept_orig,1,4) %in% c("4PAY", "4TIM")] #don't include 4PAY* or 4TIME orgs


  dept_out_df <- build_new_df(key_vector = dept_unique, sort_keys = F, predicate_char = new_col_name)


  # make an empty dataframe to store each org's hierarchy
  # dept_out_df <- data.frame("seed" = dept_orig, stringsAsFactors = FALSE)
  # dept_out_df <- mutate(dept_out_df,
  #                       "Org1" = NA,
  #                       "Org2" = NA,
  #                       "Org3" = NA,
  #                       "Org4" = NA,
  #                       "Org5" = NA,
  #                       "Org6" = NA,
  #                       "Org7" = NA,
  #                       "Org8" = NA,
  #                       "Org9" = NA,
  #                       "max_org_depth" = NA)



  # loop through each unique organization and recurisively assemble the code via
  # the FTVORGN_ORGN_CODE_PRED and FTVORGN_ORGN_CODE relationship
  row_indx <- 0
  for (dept in dept_unique) {
    row_indx <- row_indx + 1

    #Null rows cause non-desc errors in the loop. ensure that they are not included
    #  !!! may indicate issues with the supplied FTVORGN data !!!
    if (is.null(dept)) {
      message(paste0("Null Dept value supplied to loop at index ", row_indx))
      break
    }

    #initialize the seed dept and the 'predecessor' dept variables
    seed_dept <- dept # this is a static variable used to store
    # the original dept from which the hieraarchy is created
    dept_pred <- dept # this is a dynamic variable that changes for each step in the while loop.
    # it stores the org code that is next higher in it's org list

    # n and max_org_depth is used to count how many level deep the seed is in
    # the hierarchy. this number is used to place the organization in the
    # appropriate column and determine the maximum depth that a particular org #
    # exists in the hierarchy.
    n <- 1



    max_org_depth <- 1

    # keep looping up the hierarchy until it reaches the top of the org i.e.
    # org # 400000 or an NA dept_pred value
    while (!is.na(dept_pred) &&
           (!dept == "400000")) {

      if(dept_pred == "2") {
        browser()
        message("Stop for debug")
      }

      #determine the appropriate column to place new value
      column_name <- paste0("Org", n)

      #store the previous predecessor code while the next one if found
      dept_pred_prev <- dept_pred

      dept_pred <- ftvorgn_data[ftvorgn_data$FTVORGN_ORGN_CODE == dept_pred,
                                "FTVORGN_ORGN_CODE_PRED"]

      # if the predecessor code doesn't have a parent, stop the loop and throw a warning.
      # Example: Billings used org code '626' with a predecessor of '62'. '62' has no predecessor.
      if (!length(dept_pred) == 1) {
        warn_message <- paste0("issue pulling parent org for ",
                               seed_dept,
                               " with a parent org number of ",
                               dept_pred_prev)
        warning(warn_message)

        dept_out_df <- filter(dept_out_df, seed != seed_dept)

        break
      } else {
        dept_out_df[dept_out_df$seed == seed_dept, column_name] <- dept_pred
        dept_out_df[dept_out_df$seed == seed_dept, "max_depth"] <- max_org_depth
      }

      #check that the pred is appropriate
      # if (nchar(dept_pred) != 6) {
      #   message("Found an odd one")
      # }
      #put org code into new df and update max_depth column


      n <- n + 1
      max_org_depth <- max_org_depth + 1
    }
  }

  return(dept_out_df)
}


#' Build a dataframe to store the orgn hierarchy
#'
#' @param key_vector the vector to use as the key values for hte new dataframe.
#'   This will be renamed 'seed' column.
#' @param n_cols the number of columns to be added
#' @param predicate_char the predicate for the column names
#' @param include_max_depth a boolean specifying if a column should be added
#'   with the name 'max_depth' as used in the build_org_hierarchy function
#' @param sort_keys a boolean specifying if the seed column should be sorted
#'   before return
#'
#' @return a dataframe containing a column of key seed values with empty columns
#'   specified by n_cols, predicate char, and include_max_depth
build_new_df <- function(key_vector,
                         n_cols = 9,
                         predicate_char = "Org",
                         include_max_depth = T,
                         sort_keys = T) {
  stopifnot(n_cols > 0)

  new_df <- data.frame("seed" = key_vector,
                       stringsAsFactors = FALSE)
  #sort if necessary
  if (sort_keys == TRUE) {
    new_df <- dplyr::arrange(new_df,
                             seed)
  }
  if (sort_keys == "desc") {
    new_df <- dplyr::arrange(new_df,
                             seed,
                             desc = T)
  }

  # #create new columns
  # new_col_name_vec <- paste0(predicate_char, 1:n_cols)
  # new_df[,2:1+n_cols] <- NA

  for (i in 1:n_cols) {
    new_col_name <- paste0(predicate_char, i)
    new_df[[new_col_name]] <- NA
  }

  #max_depth is used in certian other function ?
  if (include_max_depth) {
    new_df[["max_depth"]] <- NA
  }

  return(new_df)
}

#' Get a lookup table defining the relationship between org number and org title
#'
#' @param ftvorgn_data the ftvorgn data specific to a single as-of date.
#'
#' @return a dataframe containing FTVORGN_TITLE and FTVORGN_ORGN_CODE
get_code_title_lu <- function(ftvorgn_data) {

  if (missing(ftvorgn_data)) {
    ftvorgn_data <- opa::get_ftvorgn_data()
    ftvorgn_data <- ftvorgn_date_filter(ftvorgn_data, as_of_date = Sys.Date())
  }

  ftvorgn_code_titles <- dplyr::select(ftvorgn_data, FTVORGN_ORGN_CODE, FTVORGN_TITLE)
  ftvorgn_code_titles <- dplyr::distinct(ftvorgn_code_titles)

  if (!nrow(ftvorgn_code_titles) == dplyr::n_distinct(ftvorgn_data$FTVORGN_ORGN_CODE)) {

    duplicated_org_rows <- dept_name_lu %>%
      dplyr::group_by(FTVORGN_TITLE) %>%
      dplyr::summarize(n = n()) %>%
      dplyr::filter(!n == 1)

    warning("duplicated org rows in the code-title lu")
  }

  return(ftvorgn_code_titles)
}

#' Format the org hierarchy dataframe returned by build-org_hierarchy to be
#' standardized with column one being the highest level of the org hierarchy,
#' col 2 being the next lower, etc. Effectively reverses the org hierarchy as
#' previously stored.
#'
#' @param df the dataframe supplied by build_org_hierarchy function
#' @param include_names boolean to include names
#' @param ftvorgn_code_title an orgcode-title lookup table as supplied by the
#'   get_code_title_lu function
#'
#' @return a dataframe with a seed org value followed by it's hierachy in the
#'   least to most granular order. One column per org level with 1 being highest
#'   and 8 being the lowest/most-specific
format_org_df <- function(df,
                          include_names = T,
                          ftvorgn_code_title) {
  #if include_names is true, ensure that ftvorgn_code_title (lookup table) is included
  if (missing(ftvorgn_code_title) & include_names == T) {
    stop("must supply ftvorgn_code_title when requiring names added")
  }

  # remove unneeded columns. by default 9 org columns are produced, but the max
  # org depth is only 5,6 depending on as-of date.
  df <- opa::remove_na_cols(df)

  # the order of the orgs in the hierarchy should be reversed to move from
  # broader i.e. org 400000 to increasingly specific/deep
  # Requires the max_org_depth to appropriaptly swap.
  df <- reverse_org_rows(df)
  df <- dplyr::filter(df, !is.na(Org1))

  # if the include_names input param is TRUE, then join the org names to the
  # output dataframe as appended columns
  if (include_names == TRUE) {
    #use this to join names to each column of org codes
    ftvorgn_code_names <- dplyr::select(ftvorgn_code_title,
                                        org_code = FTVORGN_ORGN_CODE,
                                        org_name = FTVORGN_TITLE)
    ftvorgn_code_names <- dplyr::distinct(ftvorgn_code_names)

    # loop through each column and add the org title if appropriate
    for (col in 1:ncol(df)) {
      # for each org code column, append the org titles. Skip if the column is the
      # 'max-depth' daata
      if (!names(df)[col] == "max_depth") { #skip the max_depth column

        col_name <- names(df)[col]

        temp_single_col <- df[col]

        temp_single_col <- setNames(temp_single_col,
                                    "org_code")

        temp_joined_cols <- dplyr::left_join(temp_single_col,
                                             ftvorgn_code_names,
                                             by = "org_code")
        #revert the names
        new_desc_col_name <- paste0(col_name, "_desc")
        temp_joined_cols <- setNames(temp_joined_cols,
                                     c(col_name, new_desc_col_name))

        #initialize new dataframe for output or append the columns data
        #to the already existing dataframe
        if (!exists("df_out_joined")) {
          df_out_joined <- temp_joined_cols
        } else {
          df_out_joined <- dplyr::bind_cols(df_out_joined, temp_joined_cols)
        }

      } else {
        # this triggers when on the "max_org_depth" column of dept_out_df
        df_out_joined <- dplyr::bind_cols(df_out_joined, df[col])
      }
    }
  } else {
    df_out_joined <- df
  }


  df_out_joined <- dplyr::arrange(df_out_joined, seed)

  return(df_out_joined)
}


#' reverse_org_rows
#'
#' used to reverse the org hierarchy in built by the get_org_hierarchy function.
#'
#' TODO: make into a loop
#'
#' @param df the dataframe produced by get_org_hierarchy's loop. Requires a
#'   max_org_depth column to know how to place the
reverse_org_rows <- function(df) {
  df <- lapply(df, as.character)
  df <- data.frame(df, stringsAsFactors = FALSE)

  df_names <- names(df)

  df_1org <- dplyr::filter(df, max_depth == 1)

  # Rearrange the columns depending on the depth of the seed in the hierarchy
  ##2-org levels
  df_2org <- dplyr::filter(df, max_depth == 2)
  df_2org <- df_2org[c(1,2:1)]
  names(df_2org) <- df_names[c(1:3)]

  ##3-org levels
  df_3org <- dplyr::filter(df, max_depth == 3)
  df_3org <- df_3org[c(1,3:1)]
  names(df_3org) <- df_names[c(1:4)]

  ##4-org levels
  df_4org <- dplyr::filter(df, max_depth == 4)
  df_4org <- df_4org[c(1,4:1)]
  names(df_4org) <- df_names[c(1:5)]

  ##5-org levels
  df_5org <- dplyr::filter(df, max_depth == 5)
  df_5org <- df_5org[c(1,5:1)]
  names(df_5org) <- df_names[c(1:6)]

  ##6-org levels
  df_6org <- dplyr::filter(df, max_depth == 6)
  df_6org <- df_6org[c(1,6:1)]
  names(df_6org) <- df_names[c(1:7)]

  ##7-org levels
  df_7org <- dplyr::filter(df, max_depth == 7)
  df_7org <- df_7org[c(1,7:1)]
  names(df_7org) <- df_names[c(1:8)]

  ##8-org levels
  df_out <- dplyr::bind_rows(df_1org, df_2org, df_3org,
                             df_4org, df_5org, df_6org, df_7org)
  ## There is at most, 6 levels of the hierarchy requiring 8 columns when including the seed and max
  df_names <- c("seed", "Org1", "Org2", "Org3", "Org4", "Org5", "Org6", "Org7")
  names(df_out) <- df_names


  return(df_out)

}
