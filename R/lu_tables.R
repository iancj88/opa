#TODO: Build the Affirmative Action Plan Job Group Code and Job Group Code Desc LU tables into the OPA package

#TODO: Build job termination code lookup table from msuopa

#
# fname <- "./lu_tables/Standardized Job Titles Department Names.xlsx"
# job_title_group_lu <- read.xlsx(fname, sheet = 1)
# job_title_group_lu <- select(job_title_group_lu,
#                              job_title_new = Revised_Title_Final,
#                              job_title_orig = `Job.Title.Original`,
#                              job_group = `Job.group.code`,
#                              job_group_desc = `job.group.desc`)
#
# #this works best if the position titles have already been cleaned using the
# #"Standardized Job Titles Department Names.xlsx" file
# eeo_applicant_report <- eeo_applicant_report %>%
#   left_join(job_title_group_lu,
#             by = c("job_title" = "job_title_orig"))