build_acct_hier <- function(acct_data, as_of_date, opt_bann_conn) {
  require(tidyverse)
  require(magrittr)
  require(tictoc)

  bann_conn <- opt_bann_conn
  if(missing(acct_data)) {
    tic("PULLED FTCACCI Data")
    ftvacci_all <- tbl(bann_conn, "FTVACCI") %>% collect()
    toc()
  } else {
    ftvacci_all <- acct_data
  }


  ftvacci_as_of <- filter(ftvacci_all,
                          FTVACCI_EFF_DATE < as_of_date)

  ftvacci_as_of <- ftvacci_as_of %>%
    group_by(FTVACCI_ACCI_CODE) %>%
    filter(FTVACCI_EFF_DATE == max(FTVACCI_EFF_DATE))

  #determine leaf nodes
  ftvacci_as_of <- ftvacci_as_of %>%
    mutate(is_leaf = !FTVACCI_ACCI_CODE %in% FTVACCI_ORGN_CODE,
           is_root = is.na(FTVACCI_ORGN_CODE),
           is_parent = FTVACCI_ACCI_CODE %in% FTVACCI_ORGN_CODE,
           is_child = !is_root)
  ftvacci_children <- ftvacci_as_of %>%
    group_by(FTVACCI_ACCI_CODE) %>%
    summarize(child_acci = unique())


  return(ftvacci_as_of)
}