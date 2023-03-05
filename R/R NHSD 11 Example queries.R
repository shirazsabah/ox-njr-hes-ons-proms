# R NHSD 11 Example queries

########################################## Set up directories ##########################################

# You must source 'R NHSD 1 Set-up directories.R' when you begin



########################################## Load packages ##########################################

pacman::p_load(pacman, duckdb, DBI, data.table, arrow, tidyverse, logger)



########################################## Open DuckDB con ##########################################

# Read only is set to TRUE
con = dbConnect(duckdb::duckdb(), dbdir=paste0(data_dir,"SQL/ss.duckdb"), read_only=TRUE)

# Threads...
# NB: dbExecute always returns a scalar of the number of rows affected, which will be 0 here
dbExecute(con, "PRAGMA threads=10")

# Check the expected tables are present
dbListTables(con)



########################################## Example queries ##########################################

# View 20 top rows from linked primary knee replacement table
dbGetQuery(con, "SELECT * 
                 FROM pk_ons_hes_proms
                 LIMIT 20")


# View fields from linked primary knee replacement table
dbListFields(con, "pk_ons_hes_proms")


# Remove limit and assign to bring primary knee replacement table into R environment
pk <- dbGetQuery(con, "SELECT * 
                 FROM pk_ons_hes_proms")


# Note that not all HES fields have been joined, so to reopen the HES table
source(paste0(script_dir, "R NHSD 3 Set schema for HES.R"))


# The entire HES table can now be queried using Arrow
# Note that not all dplyr verbs are available
hes %>% head() %>% collect()


# row_id and FYEAR are a unique combination, so these can be used to merge on additional variables
# This example adds on the CCG fields
addon <- hes %>% select(row_id, FYEAR, CCG_RESIDENCE, CCG_RESPONSIBILITY, CCG_TREATMENT) %>% collect()
newpk <- left_join(pk, addon, by = c("row_id", "FYEAR"))
newpk %>% janitor::tabyl(CCG_RESIDENCE)


# Perhaps more commonly one might want to Window over the diagnosis fields
# e.g. to calculate a new comorbidity score
diagcols <- grep("DIAG*", names(hes), value= TRUE)
diagcols[diagcols != "DIAG_COUNT"]
addon <- hes %>% select(row_id, FYEAR, all_of(diagcols)) %>% collect()


# I would write this table to DuckDB and Window using SQL (as per 'R NHSD 4 Create HES tables.R')
# The connection must be set to read_only=FALSE to do this
# dbWriteTable(con, "addon_table", addon)



########################################## Export DuckDB tables to CSV ##########################################

# Note that COPY seems to lose data
# dbGetQuery(con, "COPY ph_ons_hes_proms TO 'ph_ons_hes_proms.csv' (HEADER, DELIMITER ',');")

## Primary knees
pk <- dbGetQuery(con, "SELECT * from pk_ons_hes_proms")

pk <- pk %>% mutate(mic01 = case_when(ch>=7 & cca==1 ~ "Yes",
                                            ch<7 & cca==1 ~ "No",
                                            TRUE ~ NA_character_))

pk$mic01 <- factor(pk$mic01, levels=c("Yes", "No"), ordered=TRUE, labels = c("Yes", "No")) %>% forcats::fct_explicit_na()

write.csv(pk,file=paste0(data_dir,"Stata/pk_ons_hes_proms.csv"))


## Revision knees
rk <- dbGetQuery(con, "SELECT * from rk_ons_hes_proms")
write.csv(rk,file=paste0(data_dir,"Stata/rk_ons_hes_proms.csv"))


## Primary hips
ph <- dbGetQuery(con, "SELECT * from ph_ons_hes_proms")

ph <- ph %>% mutate(mic01 = case_when(ch>=8 & cca==1 ~ "Yes",
                                      ch<8 & cca==1 ~ "No",
                                      TRUE ~ NA_character_))

ph$mic01 <- factor(ph$mic01, levels=c("Yes", "No"), ordered=TRUE, labels = c("Yes", "No")) %>% forcats::fct_explicit_na()

write.csv(ph,file=paste0(data_dir,"Stata/ph_ons_hes_proms.csv"))


## Revision hips
rh <- dbGetQuery(con, "SELECT * from rh_ons_hes_proms")
write.csv(rh,file=paste0(data_dir,"Stata/rh_ons_hes_proms.csv"))



########################################## Shutdown DuckDB con ##########################################

dbDisconnect(con, shutdown=TRUE)
