# R NHSD 6 Create PROMs table


########################################## Create PROMs table ##########################################

p_unload(all)
pacman::p_load(pacman, data.table, rio, tidyverse, janitor, duckdb, DBI, logger)
setwd(data_dir)

log_info('Create PROMs table')

#############################################       Import    #############################################

proms <- import(paste0(data_dir, "PROMS/NIC380650_PROMs.txt"), quote="")



#############################################       Data prep        #############################################

## Clean names
proms <- clean_names(proms)

# Generate fields for 'complete' PROM data (defined as valid & complete OHS/OKS & anchor question for perceived success)

ohs_cols <- c("hr_q1_pain", 
              "hr_q1_sudden_pain",
              "hr_q1_night_pain",
              "hr_q1_washing",
              "hr_q1_transport",
              "hr_q1_dressing",
              "hr_q1_shopping",
              "hr_q1_walking",
              "hr_q1_limping",
              "hr_q1_stairs",
              "hr_q1_standing",
              "hr_q1_work",
              "hr_q2_pain",
              "hr_q2_sudden_pain",
              "hr_q2_night_pain",
              "hr_q2_washing",
              "hr_q2_transport",
              "hr_q2_dressing",
              "hr_q2_shopping",
              "hr_q2_walking",
              "hr_q2_limping",
              "hr_q2_stairs",
              "hr_q2_standing",
              "hr_q2_work")

oks_cols <- c("kr_q1_pain",
              "kr_q1_night_pain", 
              "kr_q1_washing", 
              "kr_q1_transport", 
              "kr_q1_walking", 
              "kr_q1_standing", 
              "kr_q1_limping", 
              "kr_q1_kneeling", 
              "kr_q1_work", 
              "kr_q1_confidence", 
              "kr_q1_shopping", 
              "kr_q1_stairs",
              "kr_q2_pain", 
              "kr_q2_night_pain", 
              "kr_q2_washing", 
              "kr_q2_transport", 
              "kr_q2_walking", 
              "kr_q2_standing", 
              "kr_q2_limping", 
              "kr_q2_kneeling", 
              "kr_q2_work", 
              "kr_q2_confidence", 
              "kr_q2_shopping", 
              "kr_q2_stairs")

proms <- proms %>%                                          
  mutate(comp_ohs = c("1", "0")[1 + (if_any(all_of(ohs_cols), `==`, "9"))]) %>% 
  mutate(comp_oks = c("1", "0")[1 + (if_any(all_of(oks_cols), `==`, "9"))])

proms <- proms %>% 
  mutate(cca = case_when(
    proms_proc_code == "HR" & comp_ohs ==1 & q2_success!=9 ~ 1,
    proms_proc_code == "KR" & comp_oks ==1 & q2_success!=9 ~ 1,
    TRUE ~ 0))

# Generate change in OHS/OKS
proms$ch <- ifelse(proms$proms_proc_code == "HR", (proms$hr_q2_score - proms$hr_q1_score), (proms$kr_q2_score - proms$kr_q1_score))

# Responders
proms <- proms %>% mutate(mic01 = case_when(ch>=6 & cca==1 ~ "Yes",
                                            ch<6 & cca==1 ~ "No",
                                            TRUE ~ NA_character_))

proms %>% janitor::tabyl(mic01)

proms$mic01 <- factor(proms$mic01, levels=c("Yes", "No"), ordered=TRUE, labels = c("Yes", "No")) %>% forcats::fct_explicit_na()
levels(proms$mic01)

# Satisfaction
proms %>% janitor::tabyl(q2_satisfaction)

proms <-
  proms %>% mutate(sat = case_when(q2_satisfaction <4 ~ 1, 
                                   q2_satisfaction ==4 | q2_satisfaction ==5 ~ 0,
                                   TRUE ~ NA_real_))

proms$sat <- factor(proms$sat, levels=c(1,0), ordered=TRUE, labels = c("Yes", "No")) %>% forcats::fct_explicit_na()
levels(proms$sat)
proms %>% janitor::tabyl(sat)

proms$q2_satisfaction <- 
  factor(proms$q2_satisfaction, levels = c(1,2,3,4,5,9), ordered = TRUE, labels = c("Excellent", "Very Good", "Good", "Fair", "Poor", "(Missing)"))  %>% forcats::fct_explicit_na()


# Success
proms %>% janitor::tabyl(q2_success)
proms <-
  proms %>% mutate(suc = case_when(q2_success <3 ~ 1, 
                                   q2_success ==3 | q2_success ==4 | q2_success ==5 ~ 0,
                                   TRUE ~ NA_real_))

proms$suc <- factor(proms$suc, levels=c(1,0), ordered=TRUE, labels = c("Yes", "No")) %>% forcats::fct_explicit_na()
levels(proms$suc)
proms %>% janitor::tabyl(suc)

proms$q2_success <- factor(proms$q2_success, levels = c(1,2,3,4,5,9), ordered = TRUE,
                           labels = c("Much better", "A little better", "About the same", "A little worse", "Much worse", "(Missing)"))


#############################################       Register table with DuckDB    #############################################

# proms <-
#   proms %>% filter(!is.na(epikey))

# Remove some columns
proms <-
  proms %>%
  select(-c(q1_scan_date, q2_scan_date, status_date, modified_date, complete, eq5d_health_scale_expected_final_model3,
            eq5d_index_expected_final_model3, hr_score_expected_final_model3, kr_score_expected_final_model3, modified_date, q1_form_version,
            q1_language))


# Sort out date cols
setDT(proms)[, q1_received_date := fifelse(q1_received_date < as.IDate("2009-01-01") |
                                      q1_received_date > as.IDate("2022-07-01"), as.Date(NA_integer_), as.Date(q1_received_date))]

proms[, q1_completed_date := fifelse(q1_completed_date < as.IDate("2009-01-01") |
                                       q1_completed_date > as.IDate("2022-07-01"), as.Date(NA_integer_), as.Date(q1_completed_date))]

proms[, q2_completed_date := fifelse(q2_completed_date < as.IDate("2009-01-01") |
                                      q2_completed_date > as.IDate("2022-07-01"), as.Date(NA_integer_), as.Date(q2_completed_date))]

proms[, q2_received_date := fifelse(q2_received_date < as.IDate("2009-01-01") |
                                      q2_received_date > as.IDate("2022-07-01"), as.Date(NA_integer_), as.Date(q2_received_date))]

proms[, q2_surgery_date := fifelse(q2_surgery_date < as.IDate("2009-01-01") |
                                      q2_surgery_date > as.IDate("2022-07-01"), as.Date(NA_integer_), as.Date(q2_surgery_date))]


# epikey is integer64, which is not handled well, so convert to character
str(proms$epikey)
proms$epikey <- as.character(proms$epikey)

dbWriteTable(con, "proms", proms)


# PROMS attrition (Knees)
dbGetQuery(con, "SELECT Count(*) FROM proms")
dbGetQuery(con, "SELECT Count(*) FROM proms WHERE proms_proc_group='Knee Replacement';")
dbGetQuery(con, "SELECT Count(*) FROM proms 
                 WHERE proms_proc_group='Knee Replacement'
                 AND epikey IS NOT NULL;")

dbGetQuery(con, "SELECT COUNT (DISTINCT (epikey, fyear)) AS 'No. records'
                 FROM proms
                 WHERE proms_proc_group='Knee Replacement'
                 AND epikey IS NOT NULL;")


# PROMS attrition (Hips)
dbGetQuery(con, "SELECT Count(*) FROM proms")
dbGetQuery(con, "SELECT Count(*) FROM proms WHERE proms_proc_group='Hip Replacement';")
dbGetQuery(con, "SELECT Count(*) FROM proms 
                 WHERE proms_proc_group='Hip Replacement'
                 AND epikey IS NOT NULL;")

dbGetQuery(con, "SELECT COUNT (DISTINCT (epikey, fyear)) AS 'No. records'
                 FROM proms
                 WHERE proms_proc_group='Hip Replacement'
                 AND epikey IS NOT NULL;")

