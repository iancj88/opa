#' build the CIP hierarchy of 6-digit, 4-digit, and 2-digit values
#'
#' Draft - requires debugging
#'
#' @param fpath the path to the raw CIPCode values.
#'
#' @return the cip hierarchy with each 6 digit cip including it's 4 and 2 digit
#'   rollups. Defaults to a wide dataset
#' @export
build_cip_hier <- function(fpath = "X:/icj_dts/opaDataEE/raw/CIPCode2010.csv") {

  require(readr)
  require(magrittr)
  #built for 2010 version of CIP code files
  #see https://nces.ed.gov/ipeds/cipcode/resources.aspx?y=55
  #to be updated in 2020 calendar year
  cip_raw <- readr::read_csv(file = fpath,
                             col_types = "cccccccc")

  cip_raw <- mutate(cip_raw,
                    CIPCode = str_replace_all(CIPCode, "[=\"\\.]", ""),
                    CIPFamily = str_replace_all(CIPFamily, "[=\"\\.]", ""),
                    cip_2d = substr(CIPCode, 1, 2),
                    cip_4d = substr(CIPCode, 1, 4),
                    CIPCode_filled = str_pad(CIPCode,6,"right", pad = "0"))

  cip_2d_df <- filter(cip_raw,
                   nchar(CIPCode) == 2) %>%
    select(CIPCode, CIPTitle_2d = CIPTitle)

  cip_4d_df <- filter(cip_raw,
                   nchar(CIPCode) == 4) %>%
    select(CIPCode, CIPTitle_4d = CIPTitle)

  cip_raw <- filter(cip_raw, !nchar(CIPCode) %in% c(2, 4))

  cip_raw <- left_join(cip_raw,
                       cip_2d_df,
                       by = c("cip_2d" ="CIPCode" )) %>%
    left_join(cip_4d_df,
              by = c("cip_4d" ="CIPCode" ))

  cip_out <- cip_raw %>%
    filter(!CIPCode %in% cip_2d_df$CIPCode,
           !CIPCode %in% cip_4d_df$CIPCode) %>%
    select(cip = CIPCode,
           cip_title = CIPTitle,
           cip_2d,
           cip_title_2d = CIPTitle_2d,
           cip_4d,
           cip_title_4d = CIPTitle_4d)



  return(cip_out)
}

#' pull_deleted_modified_cips
#'
#' get a datafrmae containing moved or deleted cip codes from the 2010 to 2020
#' update
#'
#' @return a dataframe specifying cip codes that have been deleted, had cip
#'   codes merged into them, or been merged into a new cip
#' @export
pull_deleted_modified_cips <- function() {
  require(readr)
  require(dplyr)
  require(stringr)

  cip_deleted <- readr::read_csv("X:/icj_dts/opaDataEE/raw/CIPCode2020_deleted.csv",
                                 col_types = cols(`Text Changed` = col_skip(),
                                                  `Cross References` = col_skip(),
                                                  `Moved To/Report Under` = col_skip(),
                                                  `Moved From` = col_skip()))

  cip_moved <- read_csv("X:/icj_dts/opaDataEE/raw/CIPCode2020_moved.csv",
                        col_types = cols(`Text Changed` = col_skip(),
                                         `Cross References` = col_character()))

  cip_move_delete <- bind_rows(cip_moved, cip_deleted)

  cip_move_delete <- cip_move_delete %>%
    mutate(cip = str_replace_all(`CIP Code`, "[=\"\\.]", ""),
           cip_title = `Title & Definition`,
           cip_2d = substr(cip, 1, 2),
           cip_4d = substr(cip, 1, 4),
           cip_filled = str_pad(cip,6,"right", pad = "0"),
           cip_action = Action,
           cip_moved_from = str_replace_all(`Moved From`, "[=\"\\.]", ""),
           cip_moved_from = str_pad(cip_moved_from,6,"right", pad = "0"),
           cip_moved_to = str_replace_all(`Moved To/Report Under`, "[=\"\\.]", ""),
           cip_moved_to = str_pad(cip_moved_to,6,"right", pad = "0")) %>%
    select(starts_with("cip"), -`CIP Code`)



  return(cip_move_delete)
}