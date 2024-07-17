# R NHSD 20 - Epidemiology and Patient-Relevant Outcomes Following Revisions of Partial Knee Replacements

# Research questions:
## How common is revision of a partial knee replacement (i.e. medial UKR, lateral UKR, PFJR) and has this changed over time?
## Why are partial knee replacements revised? And, has this changed over time?
## When are partial knee replacements revised? And does this depend on the indication for revision?
## Where are partial knee replacements revised? And, by whom - the original surgeon or someone else?
## What type of revision operations are performed for partial knee replacements?
## What are the patient-relevant outcomes following revision of a partial knee replacement? i.e. Re-revision; Mortality; Serious medical complications; PROMS
## How do these patient-relevant outcomes compare to primary total knee replacement?



# ########################################## Connect to db ##########################################
# 
# p_unload(all)
# pacman::p_load(pacman, data.table, tidyverse, DBI, duckdb, tictoc, janitor, survival, KMunicate, flextable, gtsummary, stringr, logger)
# 
# setwd(data_dir)
# 
# # Connect to db
# con = dbConnect(duckdb::duckdb(), dbdir=paste0(data_dir,"SQL/ss.duckdb"), read_only=TRUE)
# 
# # PRAGMAs
# dbExecute(con, "PRAGMA threads=10")
# dbExecute(con, paste0("PRAGMA temp_directory='", data_dir,"TEMP/hes.tmp'"))
# dbExecute(con, "PRAGMA memory_limit='32GB'")
# 
# # Check tables have loaded as expected
# dbListTables(con)
# 
# 
# 
# ########################################## Load required datasets ##########################################
# 
# # Load primary knee dataset
# pk <- dbGetQuery(con, "SELECT * 
#                        FROM pk
#                        WHERE primary_patient_procedure =='Patello-Femoral Replacement' OR
#                              primary_patient_procedure =='Unicompartmental Knee Replacement' OR
#                              primary_patient_procedure =='Unicondylar Knee Replacement';")
# 
# # Load revision datasets
# 
# ## rk_combined = Left and right revision knees from NJR linked to ONS, HES and PROMs
# rk_combined <- dbGetQuery(con, "SELECT * FROM rk_combined")
# 
# ## all_rk = All revision knees on the NJR (not linked to other datasets)
# all_rk <- dbGetQuery(con, "SELECT * FROM all_rk")
# 
# # Log number of rows
# log_info('\n
#          Number of partial knees on NJR = {nrow(pk)}
#          Number of first revision knees on NJR = {nrow(rk_combined)}
#          Number of first and re-revision knees on NJR = {nrow(all_rk)}
#          ')
# 
# # Disconnect
# dbDisconnect(con, shutdown = TRUE)
# rm(con)
# 
# 
# 
# ########################################## Create cohort for analysis ##########################################
# 
# # Inclusion criteria
# 
# ## Timeframe
# ### Date of primary partial knee replacement: 1/1/2009 to 31/12/2019
# pk <- pk |> filter(primary_op_date>='2009-01-01', primary_op_date<='2019-12-31' )
# rk_combined <- rk_combined |> filter(revision_date>='2009-01-01', revision_date<='2019-12-31' )
# all_rk <- all_rk |> filter(op_date>='2009-01-01', op_date<='2019-12-31' )
# 
# # Log number of rows
# log_info('\n
#          Number of partial knees on NJR (2009-2019) = {nrow(pk)}
#          Number of first revision knees on NJR (2009-2019) = {nrow(rk_combined)}
#          Number of first and re-revision knees on NJR (2009-2019) = {nrow(all_rk)}
#          ')
# 
# # Prepare derived fields required for analysis
# pk <- 
#   pk |>
#     mutate(group = case_when(primary_patient_procedure == "Patello-Femoral Replacement" ~ "PFJR", TRUE ~ "UKR")) |>       # UKR v PFJR
#     mutate(pyear = format(primary_op_date, format = "%Y")) |>                                                             # Year of primary
#     mutate(ryear = format(revision_date, format = "%Y"))  |>                                                              # Year of revision
#     mutate(ifrhier = case_when(
#       ind_rev_other==1 ~ 9,
#       ind_rev_pain ==1 ~ 8,
#       ind_rev_stiffness ==1 ~ 7,
#       ind_rev_progressive_arthritis_remaining ==1 ~ 6,
#       ind_rev_peri_prosthetic_fracture ==1 ~ 5,
#       ind_rev_mds1patella_maltracking==1|ind_rev_component_dissociation ==1|ind_rev_dislocation_subluxation ==1|ind_rev_instability==1 ~ 4,
#       ind_rev_mds1wear_of_tibia==1 | ind_rev_mds1wear_of_patella==1|ind_rev_implant_fracture_tibial==1|ind_rev_implant_fracture_patella ==1|ind_rev_implant_fracture_femoral ==1|ind_rev_implant_fracture ==1|ind_rev_wear_of_polyethylene_component==1 ~ 3,
#       ind_rev_mds1lysis ==1|ind_rev_lysis_tibia ==1|ind_rev_lysis_femur ==1|ind_rev_mds1aseptic_loosening ==1|ind_rev_aseptic_loosening_patella ==1|ind_rev_aseptic_loosening_tibia==1|ind_rev_aseptic_loosening_femur  ==1 ~ 3,
#       ind_rev_mds1incorrect_sizing==1|ind_rev_malalignment ==1 ~ 2,
#       ind_rev_infection ==1 ~1,
#       TRUE ~ 9))
# 
# pk$ifrhier <- factor(pk$ifrhier, levels=c(1:9), ordered=TRUE, labels = c("Infection", "Malalignment", "Loosening/Lysis", "Instability", "Fracture", "Progressive Arthritis", "Stiffness", "Unexplained pain", "Other"))
# 
# # Where not revised, code ifrhier as NA
# pk <- pk |>  mutate(ifrhier = case_when(is.na(revision_njr_index_no) ~ NA_character_, TRUE ~ ifrhier))
# 
# 
# # Create dataset for analysis
# ## Base cohort is generated from NJR primary knee dataset (i.e. pk)
# ## rk_combined is merged on to provide information on the revision procedure from other datasets (e.g. ONS, HES, PROMs)
# ## all_rk is used later to provide information on re-revision procedures
# rpk <-
#   pk |> 
#   filter(!is.na(revision_njr_index_no), !is.na(revision_procedure_id)) |>
#   mutate(revision_njr_index_no = as.numeric(as.character(revision_njr_index_no))) |>
#   mutate(revision_procedure_id = as.numeric(as.character(revision_procedure_id))) |>
#   select(primary_njr_index_no, primary_procedure_id, primary_surgical_unit_id, group, ifrhier,
#          primary_consultant, revision_njr_index_no, revision_procedure_id) |>
#   inner_join(rk_combined, by = c("revision_njr_index_no", "revision_procedure_id"))
# 
# ## Add on ASA grade which was missing in rk_combined
# rpk<-
#   rpk |>
#   inner_join(select(all_rk, revision_njr_index_no, revision_procedure_id, asa), by = c("revision_njr_index_no", "revision_procedure_id"))
# 
# ## Merge on revision details fields needed to evaluate type of revision
# rk <- rio::import("NJR/KneeAllRevisions.txt", quote="")
# rk <- rk |> janitor::clean_names()
# cols_rpk <- colnames(rpk)
# keep_cols <- c("revision_njr_index_no", "revision_procedure_id")
# exclude_cols <- setdiff(cols_rpk, keep_cols)
# exclude_cols <- exclude_cols[exclude_cols %in% colnames(rk)]
# rk <- rk |> select(-all_of(exclude_cols))
# 
# rpk<-
#   rpk |>
#   inner_join(rk, by = c("revision_njr_index_no", "revision_procedure_id"))
# 
#   
# # Log number of rows
# log_info('\n
#          Number of revision of partial knee replacements analysed (2009-2019) = {nrow(rpk)}
#          ')
# 
# # Remove unnecessary items from environment
# rm(cols_rpk, exclude_cols, keep_cols)
# 
# 
# 
# ########################################## Export datasets ##########################################
# 
# setwd(paste0(data_dir, "rUKR"))
# 
# ## The purpose of this is to remove the dependency on the duckdb connection
# saveRDS(all_rk, file= "all_rk")
# saveRDS(pk, file= "pk")
# saveRDS(rk, file= "rk")
# saveRDS(rk_combined, file= "rk_combined")
# saveRDS(rpk, file= "rpk")
# 
# # Remove all items from environment
# rm(list = ls())
# 
# 
# 
########################################## Import data ##########################################

setwd(paste0(data_dir, "rUKR"))

all_rk <- readRDS("all_rk")
pk <- readRDS("pk")
rk <- readRDS("rk")
rk_combined <- readRDS("rk_combined")
rpk <- readRDS("rpk")



########################################## Flow Diagram ##########################################

### To do



###################################################################################################

## How common is revision of a partial knee replacement (i.e. medial UKR, lateral UKR, PFJR) and has this changed over time?

# Number of partial knee replacements revised per year
pk |>
  filter(!is.na(revision_njr_index_no)) |>
  count(ryear)

# Number of UKR v PFJR revised per year
pk |>
  filter(!is.na(revision_njr_index_no)) |>
  group_by(ryear, group) |>
  summarize(count = n()) |>
  pivot_wider(names_from = group, values_from = count, values_fill = list(count = 0)) |>
  mutate(Total = PFJR + UKR)

# # Plot revisions of partial knee replacements per year by group
plot_rev_yr <-
  pk |> 
  filter(!is.na(revision_njr_index_no)) |>
  ggplot(aes(x = factor(ryear), fill= group)) +
  geom_bar(position = "stack") +
  labs(x = "Year", y = "Count", fill = "Group") +
  ggtitle("Counts of revisions of partial knee replacements by year") +
  theme_minimal()



###################################################################################################

## Why are partial knee replacements revised? And, has this changed over time?

# Examine revision indications
p_load(janitor)

pk |>
  filter(!is.na(ifrhier)) |>
  tabyl(ifrhier, group) |>
  adorn_totals("col") |>
  janitor::adorn_percentages("col") |> 
  adorn_pct_formatting(digits = 1) |>
  adorn_ns(position = "front", format_func = function(x) format(x, big.mark = ",", decimal.mark = ".")) |> 
  select(ifrhier, Total, everything()) |> 
  rename(`Indication for revision` = ifrhier)

# Insufficient cases to look at trends over time



###################################################################################################

## When are partial knee replacements revised? And does this depend on the indication for revision?

# Set-up the outcome field for a simple KM analysis
pk <- pk %>%  
  mutate(outcome = case_when(
    outcome_type == "Revised" ~ 1,
    TRUE ~ 0))

## Plot overall implant survival
p_load(KMunicate)
trace("KMunicate",edit=TRUE)
# Change ylim on L81 manually to c(0,20)
# Change L53-55
# data$surv <- (1 - data$surv) * 100
# data$lower <- (1 - data$lower) * 100
# data$upper <- (1 - data$upper) * 100

# Plot survival of partial knee replacements
KM <- survfit(Surv(primary_to_outcome_years, outcome) ~1, data=pk)
time_scale <- seq(0, ceiling(max(pk$primary_to_outcome_years)), by=1)
p <- KMunicate(fit = KM, time_scale = time_scale,
               .risk_table = NULL,
               .reverse = TRUE,
               .xlab = "Time (years)",
               .ylab = "Cumulative incidence of revision (%)",
               .alpha = 0.75 # To make confidence intervals completely transparent!
) 

km_os <- p

# Plot survival of PFJR v UKR
KM <- survfit(Surv(primary_to_outcome_years, outcome) ~group, data=pk)
time_scale <- seq(0, ceiling(max(pk$primary_to_outcome_years)), by=1)
p <- KMunicate(fit = KM, time_scale = time_scale,
               .risk_table = NULL,
               .reverse = TRUE,
               .xlab = "Time (years)",
               .ylab = "Cumulative incidence of revision (%)",
               .alpha = 0.75 # To make confidence intervals completely transparent!
) 

p <- p + theme(legend.position="right")

km_pfj_ukr <- p

# Competing risk modelling
p_load(cmprsk)

# Explore outcome variable
pk |> janitor::tabyl(outcome)

# Set-up outcome variable for competing risks
pk <-
  pk |>
  mutate(outcome_ind = case_when(outcome==1 ~ ifrhier, TRUE ~ "Unrevised"))

# Explore outcome variable
janitor::tabyl(pk$outcome_ind)

# Convert the outcomes to numeric factors
pk$status <- as.factor(pk$outcome_ind)

pk$status <- fct_relevel(pk$status, "Unrevised", "Infection", "Malalignment", "Loosening/Lysis", 
                         "Instability", "Fracture", "Progressive Arthritis", "Stiffness", "Unexplained pain", "Other")

# Code appropriately for cmprsk
# 0 = censored, 1 = event 1, 2 = event 2, etc.
pk$status_numeric <- as.numeric(pk$status) - 1

# Check coding
janitor::tabyl(pk$status_numeric)

# Check for NA values
sum(is.na(pk$primary_to_outcome_years))
sum(is.na(pk$status_numeric))

# Fit the model
cr <- cuminc(ftime = pk$primary_to_outcome_years, 
             fstatus = pk$status_numeric, 
             cencode = 0)

p_load(survminer)
p <- ggcompetingrisks(fit = cr, 
                      multiple_panels = F, 
                      xlab = "Years", 
                      ylab = "Cumulative incidence of event",
                      title = "Competing Risks Analysis") 

p$mapping <- aes(x = time, y = est, colour = event)

# View colour palette
# p_load(RColorBrewer)
# display.brewer.pal(n = 10, name = 'Paired')

p_load(pals)
event_labels = c("Infection", "Malalignment", "Loosening/Lysis", 
                 "Instability", "Fracture", "Progressive Arthritis", 
                 "Stiffness", "Unexplained pain", "Other")

# Modify the plot to add custom legend labels
cif_cmp_rsk <-
  p + scale_color_manual(values = unname(cols25()), labels = event_labels) +
  labs(colour = "Event") +
  theme(legend.position = "bottom",                    # Position the legend at the bottom
        legend.text = element_text(size = 8),         # Adjust the size of the legend text
        legend.title = element_text(size = 10))       # Adjust the size of the legend title

cif_cmp_rsk



###################################################################################################

## What are the baseline characteristics of patients undergoing revision of a partial knee replacement?

t1 <- rpk |> 
  mutate(year = format(revision_date, format = "%Y")) |> 
  mutate(revision_age = as.numeric(as.character(revision_age))) |>
  mutate(asa_grade = case_when(asa == "P1 - Fit and healthy" ~ "ASA 1",
                               asa == "P2 - Mild disease not incapacitating" ~ "ASA 2",
                               TRUE ~ "ASA 3+")) |>
  select(revision_age, gender, asa_grade, charl01, bmi_plaus, ethnic5, imd5, year, ifrhier, group) %>% droplevels()

theme_gtsummary_compact()

tbl1 <- 
  tbl_summary(
    t1,
    by = group,
    missing = "no",
    type = all_continuous() ~ "continuous2",
    statistic = list(
      all_continuous() ~ c("{mean} ({sd})",
                           "{N_miss} ({p_miss}%)"),
      all_categorical(dichotomous = FALSE) ~ "{n} ({p}%)",
      all_dichotomous() ~ "{n} ({p}%)"),
    label = list(
      year = "Year of surgery",
      revision_age = "Age at revision (years)",
      gender = "Gender",
      asa_grade = "ASA Class",
      charl01 = "Charlson comorbidity index",
      bmi_plaus = "Body mass index",
      ethnic5 = "Ethnicity",
      imd5 = "Index of multiple deprivation",
      ifrhier = "Indication for revision"),
    digits = list(
      all_categorical() ~ c(0, 0),
      all_continuous() ~ c(1,1,0,0))) %>%
  add_overall() %>%
  bold_labels() %>%
  italicize_levels()

tbl1_flex <- tbl1 %>% as_flex_table()

tbl1_flex



###################################################################################################

## What are the patient-relevant outcomes following revision of a partial knee replacement? i.e. Re-revision; Mortality; Serious medical complications; PROMS

##### 1. Implant survivorship #####

#first_is <- rpk |> rename(op_date = revision_date)

first_is <- rpk

### a. Logistic regression at 2y
second_is <-
  all_rk %>%
  filter(year %between% c("2009", "2019"), revno=="Second linked rKR") %>% 
  select(nn_nid, side, revision_njr_index_no, revision_procedure_id, op_date) %>% 
  rename(op_date_2 = op_date, revision_njr_index_no_2 = revision_njr_index_no, revision_procedure_id_2 = revision_procedure_id)

# Merge first and second rKR
first_is <-
  merge(first_is, second_is, by=c("nn_nid", "side"), all.x = TRUE) %>% 
  mutate(ttsr = as.numeric(op_date_2 - op_date)) %>%
  mutate(ttsr = case_when(ttsr == 0 ~ NA_real_,
                          TRUE ~ ttsr))

# Create table of third revisions
third_is <-
  all_rk %>%
  filter(year %between% c("2009", "2019"), revno=="Third or more linked rKR") %>% 
  select(nn_nid, side, revision_njr_index_no, revision_procedure_id, op_date) %>%
  group_by(nn_nid, side) %>% 
  arrange(op_date) %>%
  mutate(row = row_number()) %>% 
  filter(row ==1) %>%
  select(-row) %>% 
  ungroup() %>% 
  rename(op_date_3 = op_date, revision_njr_index_no_3 = revision_njr_index_no, revision_procedure_id_3 = revision_procedure_id)

# Merge third revisions
first_is <-
  merge(first_is, third_is, by=c("nn_nid", "side"), all.x = TRUE)

# When is.na(ttsr), see if there is a third revision
first_is <-
  first_is %>% 
  mutate(ttsr = case_when(is.na(ttsr) ~ as.numeric(op_date_3 - op_date),
                          TRUE ~ ttsr)) 

# Remove duplicates again, where ttsr==0
first_is <-
  first_is %>% 
  mutate(ttsr = case_when(ttsr == 0 ~ NA_real_,
                          TRUE ~ ttsr))


# Create field for re-revised/not within 2 years
first_is <-
  first_is %>% 
  mutate(sr2y = case_when(ttsr < 730 ~ 1,
                          TRUE ~ 0))


# Calculate re-revision rate at 2 years overall
first_is |> janitor::tabyl(sr2y)

# Calculate re-revision rate at 2 years by diagnosis for revision
is2y <-
  first_is %>% tabyl(sr2y, ifrhier) %>% 
  adorn_percentages("col") %>% 
  adorn_pct_formatting(digits = 2) %>% 
  adorn_ns()



### b. Implant survivorship
# Create follow-up time and outcome fields

## Final follow-up date
first_is <- first_is %>% mutate(ffu = as.Date('2019-12-31'))

# Create outcome field
first_is <-
  first_is %>% 
  mutate(outcome = case_when(
    op_date_2 < ffu ~ "Re-revised",
    dod < ffu ~ "Dead",
    TRUE ~ "Unrevised"))

# Tag those who died before re-revision
first_is <-
  first_is %>% 
  mutate(dbrr = case_when(dod < op_date_2 ~ 1, TRUE ~0))

# None found
first_is %>% 
  tabyl(dbrr)

# Create follow-up field
str(first_is$op_date)
str(first_is$op_date_2)
str(first_is$dod)
str(first_is$ffu)

first_is <-
  first_is %>% 
  mutate(fu = case_when(
    outcome == "Re-revised" ~ op_date_2 - op_date,
    outcome == "Dead" ~ dod - op_date,
    outcome == "Unrevised" ~ ffu - op_date))

# Count records with follow-up <=0
first_is %>% filter(fu <=0) %>% count()

# Remove records with follow-up <=0
first_is <-
  first_is %>% filter(!fu <=0)

# Convert to years
first_is$fu <- as.numeric(first_is$fu)/365.25

# Create outcome field for survival analysis
first_is <-
  first_is %>% 
  mutate(outcome_sa = case_when(
    outcome == "Re-revised" ~ 1,
    TRUE ~ 0))

# Survival graphs

# Plot one survival graph for re-revisions of partial knee replacements by type (UKR v PFJR)
trace("KMunicate",edit=TRUE)
# Change line 82 to c(0,25)
KM <- survfit(Surv(fu, outcome_sa) ~group, data=first_is)
time_scale <- seq(0, ceiling(max(first_is$fu)), by=1)
p <- KMunicate(fit = KM, time_scale = time_scale,
               .risk_table = NULL,
               .reverse = TRUE,
               .xlab = "Time (years)",
               .ylab = "Cumulative incidence of re-revision (%)",
               .alpha = 0.5 # To make confidence intervals completely transparent!
) 

p <- p + theme(legend.position="right")
km_rerev <- p


##### 3. Mortality at 90 days #####

## Create field mortality y/n at 90 days
first_is <- as.data.frame(first_is) %>% mutate(mort90 = case_when(as.Date(dod) - op_date <=90 ~ 1, TRUE ~ 0))

# Function to create counts and percentages with Poisson 95% CI

pois_perc_ci <- function(data, variable, ...) {
  
  events = sum(data[[variable]])
  denom = length(data[[variable]])
  
  test <- poisson.test(events, alternative = "two.sided", conf.level = 0.95)
  
  dplyr::tibble(
    events = round(events,0),
    denom =round(denom,0),
    perc = round(events/denom *100,1),
    conf.low = round(test$conf.int[1]/denom *100,1),
    conf.high = round(test$conf.int[2]/denom *100,1)
  )
  
}

# This determines the order of the table
t2 = first_is %>% select("mort90", "group")

tbl2 <-
  t2 %>% 
  dplyr::mutate_if(~inherits(., "ordered"), ~factor(., ordered = FALSE)) %>%
  tbl_custom_summary(
    by = "group",
    type = list(all_continuous() ~ "continuous2",
                all_categorical(dichotomous = FALSE) ~ "continuous2",
                all_dichotomous() ~ "continuous2"),
    stat_fns = ~ pois_perc_ci,
    statistic = ~ c("{events}/{denom}", "({perc}% [{conf.low} to {conf.high}%])"),
    label = list(
      mort90 = "Death within 90 days"),
    digits = everything() ~ 1,
    value = NULL,
    missing = "no",
    missing_text = NULL,
    include = everything(),
    overall_row = FALSE,
    overall_row_last = FALSE,
    overall_row_label = NULL) %>% 
  add_overall() %>%
  bold_labels() %>%
  italicize_levels() %>%
  add_stat_label(label = everything() ~ c("n/N", "(% [95% CI])"))

tbl2_flex <- tbl2 %>% as_flex_table()
tbl2_flex



########################################## 4. Complications at 90 days ##########################################

## Complications fields

comps <-
  first_is %>% 
  select(EPIKEY, group, compacva90, complrti90, compmi90, compdvt90, comppe90, compaki90, computi90) %>% 
  mutate(any_med_comp =
           if_else(if_any(.cols = c(compacva90, complrti90, compmi90, compdvt90, comppe90, compaki90, computi90),`==`,TRUE),TRUE, FALSE))

comps <-
  comps %>% 
  filter(!is.na(EPIKEY) & EPIKEY!=0 & !is.na(any_med_comp))

log_info('Number of NJR-ONS-HES linked episodes: {comps %>% count()}')

# This determines the order of the table
t3 = comps %>% select("any_med_comp", "compaki90", "compdvt90", "complrti90", "compmi90", "comppe90", "compacva90", "computi90", "group")

tbl3 <-
  t3 %>% 
  dplyr::mutate_if(~inherits(., "ordered"), ~factor(., ordered = FALSE)) %>%
  tbl_custom_summary(
    by = "group",
    type = list(all_continuous() ~ "continuous2",
                all_categorical(dichotomous = FALSE) ~ "continuous2",
                all_dichotomous() ~ "continuous2"),
    stat_fns = ~ pois_perc_ci,
    statistic = ~ c("{events}/{denom}", "({perc}% [{conf.low} to {conf.high}%])"),
    digits = everything() ~ 1,
    label = list(
      any_med_comp = "Any medical complication",
      compaki90 = "Acute kidney injury",
      compdvt90 = "Deep vein thrombosis",
      complrti90 = "Lower respiratory tract infection",
      compmi90 = "Myocardial infarction",
      comppe90 = "Pulmonary embolism",
      compacva90 = "Stroke",
      computi90 = "Urinary tract infection")) %>%
  add_overall() %>%
  bold_labels() %>%
  italicize_levels() %>%
  add_stat_label(label = everything() ~ c("n/N", "(% [95% CI])"))

tbl3_flex <- tbl3 %>% as_flex_table()
tbl3_flex



########################################## 5. Length of stay ##########################################

los <-
  first_is %>%
  select(EPIDUR, SPELDUR, los, group, EPIKEY) %>% 
  filter(!is.na(EPIKEY) & EPIKEY!=0)

str(los$EPIDUR)
str(los$SPELDUR)
str(los$los)

# Note that scoped verbs (e.g. _at) have been superseded by 'across()'
aggs <- list(
  median = ~median(.x, na.rm = TRUE),
  mean = ~round(mean(.x, na.rm = TRUE),0),
  #min = ~min(.x, na.rm = TRUE), 
  max = ~max(.x, na.rm = TRUE),
  q1 = ~round(quantile(.x, na.rm = TRUE, prob=c(.25)),0),
  q3 = ~round(quantile(.x, na.rm = TRUE, prob=c(.75)),0)
)

# Summarise for FCE, spells and CIPS
los_summary <-
  los %>% 
  group_by(group) %>% 
  summarise(across(c(EPIDUR, SPELDUR, los), aggs))

los_summary

# Transpose dataframe
cols <- names(los_summary)[2:15]

los_summary <-
  los_summary %>%
  mutate_at(cols, as.numeric) %>% 
  as.data.frame()

rownames(los_summary) <- los_summary[,1]
los_summary[,1] <- NULL

los_summary <-
  los_summary %>%
  tibble::rownames_to_column() %>%  
  pivot_longer(-rowname) %>% 
  pivot_wider(names_from=rowname, values_from=value) %>% 
  rename(`Length of stay (days)` = "name")

los_summary

# Bind on a new column
col1 <- c("median", "mean", "max", "q1", "q3", "median", "mean", "max", "q1", "q3", "median", "mean", "max", "q1", "q3")
los_summary["Length of stay (days)"] <- col1

los_summary <-
  los_summary %>% 
  add_row(`Length of stay (days)` = "Finished consultant episodes", .before= 1) %>% 
  add_row(`Length of stay (days)` = "Provider spells", .before= 7) %>% 
  add_row(`Length of stay (days)` = "Continuous inpatient spells", .before= 13)

set_flextable_defaults(font.size = 7, padding = 0, line_spacing = 1)
los_flex <- flextable(los_summary)
los_flex <- add_header_row(los_flex, values = c("","Type of partial knee replacement revised"), colwidths = c(1,2))
los_flex <- align(los_flex, align = "center", part = "header")
los_flex <- bold(los_flex, c(1,7,13), bold = TRUE)
los_flex <- italic(los_flex, c(2:6,8:12,14:18), 1, italic = TRUE)
los_flex <- bold(los_flex, bold = TRUE, part = "header")
los_flex



########################################## 2. PROMs ##########################################


## Create proms dataset for analysis
proms <- first_is %>% filter(!is.na(proms_serial_no))

## Count records
proms %>% count()

## Outcomes
## a. OKS score (pre, post, change)
## b. EQ-5D index (pre, post, change)
## c. Number achieving MIC (mic01 is based on a change score of >=6 ~ yes -- this corresponds with mic-pred-adj for rKR)

# Create PROM outcomes table

## Prep
p_load(gtsummary, flextable)

# Select variables
t4 = proms %>% filter(!is.na(proms_serial_no)) %>% select("kr_q1_score", "kr_q2_score", "ch", "mic01", "q1_eq5d_index", "q2_eq5d_index", "eq5d_index_change", "q2_satisfaction", "q2_success", "group") %>% droplevels()

tbl4 <- 
  tbl_summary(
    t4,
    by = "group", # split table by group
    missing = "no",
    type = list(
      c(all_continuous(), q1_eq5d_index,q2_eq5d_index, eq5d_index_change) ~ "continuous2"),
    statistic = list(
      c(all_continuous(),-c(q1_eq5d_index,q2_eq5d_index, eq5d_index_change)) ~ c("{mean} ({sd})",
                                                                                 "{N_miss} ({p_miss}%)"),
      c(q1_eq5d_index,q2_eq5d_index, eq5d_index_change) ~ c("{median} ({p25}, {p75})",
                                                            "{N_miss} ({p_miss}%)"),
      all_categorical(dichotomous = FALSE) ~ "{n} ({p}%)",
      all_dichotomous() ~ "{n} ({p}%)"),
    label = list(
      age_at_revision = "Age (years)",
      gender = "Gender",
      kr_q1_score = "Pre-operative OKS",
      kr_q2_score = "Post-operative OKS",
      ch = "Change in OKS",
      mic01 = "Responder",
      q1_eq5d_index = "Pre-operative EQ-5D utility",
      q2_eq5d_index = "Post-operative EQ-5D utility",
      eq5d_index_change = "Change in EQ-5D utility",
      q2_satisfaction = "Patient satisfaction",
      q2_success = "Perceived success"),
    digits = list(
      all_continuous() ~ c(1,1,0,0),
      all_categorical() ~ c(0, 0),
      c(q1_eq5d_index,q2_eq5d_index, eq5d_index_change) ~ c(3,3,3,0,0))) %>%
  add_overall() %>%
  bold_labels() %>%
  italicize_levels() %>% 
  add_stat_label(label = c(q1_eq5d_index,q2_eq5d_index, eq5d_index_change) ~ c("Median (Q1, Q3)", "N missing (% missing)")) #%>%
#modify_header(all_stat_cols(FALSE) ~ "**{level}**", stat_0 ~ "**Overall**")

tbl4_flex <- tbl4 %>% as_flex_table()

# Satisfaction & Perceived success as binary outcomes
t5 = proms %>% filter(!is.na(proms_serial_no)) %>% select("sat", "suc", "group") %>% droplevels()

tbl5 <- 
  tbl_summary(
    t5,
    by = "group", # split table by group
    missing = "no",
    statistic = list(
      all_dichotomous() ~ "{n} ({p}%)"),
    label = list(
      sat = "Patient satisfaction",
      suc = "Perceived success")) %>%
  add_overall() %>%
  bold_labels() %>%
  italicize_levels()

tbl5_flex <- tbl5 %>% as_flex_table()

# PROM attrition by indication
first_is <- 
  first_is %>% mutate(promyn = case_when(is.na(proms_serial_no) ~ "No",
                                         TRUE ~ "Yes"))

t6 <- first_is %>% select("promyn", "group")

tbl6 <- 
  tbl_summary(
    t6,
    by = "group", # split table by group
    missing = "no",
    statistic = list(
      all_dichotomous() ~ "{n}/{N} ({p}%)"),
    label = list(
      promyn = "PROM records linked"),
    digits = list(
      all_dichotomous() ~ c(0,0,0))) %>%
  add_overall() %>%
  bold_labels() %>%
  italicize_levels() %>% 
  modify_header(all_stat_cols(FALSE) ~ "**{level}**", stat_0 ~ "**Overall**")

tbl6_flex <- tbl6 %>% as_flex_table()

# PROM attrition could be linked into the main PROM table if desired
tbl7 <-
  tbl_stack(list(tbl6, tbl4)) %>%
  modify_footnote(all_stat_cols() ~ "Except where specified, percentages and counts were based on linked PROM records (not the total of first rKA on the NJR).")

tbl7_flex <- tbl7 %>% as_flex_table()
tbl7_flex



###################################################################################################

## Where are partial knee replacements revised? And, by whom - the original surgeon or someone else?

## How often is the revision consultant the same as the primary consultant?
rpk |>
  filter(primary_consultant == consultant) |>
  count()

## How often is the surgical unit the same?
rpk |>
  filter(primary_surgical_unit_id == surgical_unit_id) |>
  count()



###################################################################################################

## What type of revision operations are performed for partial knee replacements?

## Procedure type (e.g. single stage v two-stage)
rpk |> janitor::tabyl(procedure_type)

# ## Which components are removed?
# ## This is poorly coded -> suggest omit from analysis
# 
# # Which components are removed in single-stage revisions?
# janitor::tabyl(rpk$components_removed_summary[rpk$procedure_type=="Knee Single Stage Revision"])
# 
# # What is going on when the components removed is "" or "knee brand removed unavailable"?
# janitor::tabyl(rpk$meniscal_details01[rpk$procedure_type=="Knee Single Stage Revision" & (rpk$components_removed_summary=="" | rpk$components_removed_summary=="knee brand removed unavailable")])

## Revision constructs (in single stage revisions)
### Type of knee (e.g. bicondylar, hinged, PFJR addition)
### The vast majority are bicondylar knees
### There are a small number of hinges, additions of PFJRs -- these records probably need to be examined manually to understand them
rpk |>
  filter(group=="UKR", procedure_type=="Knee Single Stage Revision") |>
  janitor::tabyl(femoral_implant_type)

rpk |>
  filter(group=="UKR", procedure_type=="Knee Single Stage Revision") |>
  janitor::tabyl(tibial_tray_type)

rpk |>
  filter(group=="UKR", procedure_type=="Knee Single Stage Revision") |>
  janitor::tabyl(femoral_implant_type, tibial_tray_type)

## tibial_tray_implant_type == "Knee - Tibial" or <NA>
## tibial_tray_group == "Tibial Tray Component" or <NA>

# How to identify addition of a uni? e.g. mukr to biukr; pfjr to uni; etc.
# This may help to identify addition of a second uni..
rpk |> filter(group=="UKR", procedure_type=="Knee Single Stage Revision") |>
  janitor::tabyl(femoral_uni_manufacturer)

### Which revisions get a hinge?
rpk |>
  filter(group=="UKR", procedure_type=="Knee Single Stage Revision", hinged_type_knee=="0") |>
  janitor::tabyl(ifrhier)

rpk |>
  filter(group=="UKR", procedure_type=="Knee Single Stage Revision", femoral_implant_type=="Knee - Hinged/Linked") |>
  janitor::tabyl(ifrhier)

### Constraint
### Need to understand what types of revisions are those classified as NA?
rpk |>
  filter(group=="UKR", procedure_type=="Knee Single Stage Revision") |>
  janitor::tabyl(meniscal_constraint01)

rpk |>
  filter(group=="UKR", procedure_type=="Knee Single Stage Revision") |>
  janitor::tabyl(meniscal_constraint02)

rpk |>
  filter(group=="UKR", procedure_type=="Knee Single Stage Revision") |>
  janitor::tabyl(meniscal_constraint01, meniscal_constraint02)

rpk |>
  filter(group=="UKR", procedure_type=="Knee Single Stage Revision", femoral_implant_type=="Knee - Hinged/Linked") |>
  janitor::tabyl(meniscal_constraint01)


## What thickness of bearing is implanted at the revision (when revised to a TKR)?

# Define the regex pattern for the desired formats
pattern <- "([0-9]{2}MM|[0-9]{2}\\.[0-9]MM|[0-9]{2}mm|[0-9]{2}\\.[0-9]mm|[0-9]{2} mm|[0-9]{2}x|[0-9]\\.[0-9]MM|[0-9]MM|[0-9]{2}\\.[0-9] mm|[0-9]mm|[0-9] mm|Green [0-9]{2}|Yellow [0-9]{2}| Stripe [0-9]{2}|[0-9]{2} MM|75x[0-9]{2}|75X[0-9]{2}|Insert [A-Z]/[0-9]{2})"

# Extract the matching patterns
matches <- str_extract_all(rpk$meniscal_details01, pattern)

# Remove non-digit characters except for the decimal point and flatten the list of matches
rpk$thickness <- sapply(matches, function(x) {
  if (length(x) == 0) return(NA)
  gsub("[^0-9.]", "", x)
})

# Convert the list of thickness values into a more readable format
rpk$thickness <- sapply(rpk$thickness, paste, collapse = ", ")

# Make a few corrections manually
rpk$thickness <- gsub(", [0-9][0-9]", "", rpk$thickness)
rpk$thickness[rpk$thickness == "75" | rpk$thickness == "65" | rpk$thickness == "4"] <- "NA"
rpk$thickness[rpk$thickness == "7512"] <- "12"
rpk$thickness[rpk$thickness == "7514"] <- "14"
rpk$thickness[rpk$thickness == "7516"] <- "16"
rpk$thickness[rpk$thickness == "0"] <- "NA"
rpk$thickness[rpk$meniscal_details01 %like% "Unicondylar"] <- "NA"
rpk$thickness[rpk$meniscal_details01 %like% "UNI"] <- "NA"

# # What about the NAs?
# rpk %>% filter(thickness=="NA") %>% janitor::tabyl(meniscal_details01)

# Explore bearing thickness at revision
janitor::tabyl(rpk$thickness[rpk$procedure_type=="Knee Single Stage Revision"])



### How often are augments, sleeves, cones, stems used?

# Function to check if a column contains any of the specified strings
contains_strings <- function(col) {
  strings_to_find <- c("sleeve", "cone", "wedge","\\bstem\\b")
  any(sapply(strings_to_find, function(x) grepl(x, col, ignore.case = TRUE)))
}

# Apply the function to each column in the dataframe
contains_desired_strings <- apply(rpk, 2, contains_strings)

# Get the names of columns that contain any of the specified strings
columns_with_desired_strings <- names(rpk)[contains_desired_strings]

# Print the column names
print(columns_with_desired_strings)


janitor::tabyl(rpk$tibial_wedge_details)
janitor::tabyl(rpk$femoral_wedge_details)


### How often in revisions of UKRs is the patella resurfaced?
rpk |>
  filter(group=="UKR") |>
  janitor::tabyl(patella_group)




###################################################################################################

## How do these patient-relevant outcomes compare to primary total knee replacement? And first, aseptic revision of a primary total knee replacement?

###################################################################################################


# ########################################## Export images ##########################################
# 
# plotlist = list()
# plotlist[[1]] = km_os
# plotlist[[2]] = km_pfj_ukr
# plotlist[[3]] = cif_cmp_rsk
# plotlist[[4]] = km_rerev
# plotlist[[5]] = plot_rev_yr
# 
# 
# for(i in 1:4) {
#   ggsave(plot = plotlist[[i]], 
#          width = 200,
#          height = 200,
#          units = "mm",
#          dpi = 1000,
#          file = paste("file",i,".png",sep=""))
# }




########################################## Export tables ##########################################

library(officer)
library(flextable)

# Create new Word document and export tables
doc <- 
  read_docx() |>
  body_add_par("Epidemiology and Patient-Relevant Outcomes Following Revisions of Partial Knee Replacements", style = "heading 1") |>
  body_add_break(pos = "after") |>
  body_add_par("Baseline characteristics of patients undergoing revision of a partial knee replacement", style = "heading 1") |>
  body_add_par("", style = "Normal") |>
  body_add_flextable(value = tbl1_flex) |>
  body_add_break(pos = "after") |>
  body_add_par("Mortality after revision of a partial knee replacement", style = "heading 1") |>
  body_add_par("", style = "Normal") |>
  body_add_flextable(value = tbl2_flex) |>
  body_add_break(pos = "after") |>
  body_add_par("Serious medical complications after revision of a partial knee replacement", style = "heading 1") |>
  body_add_par("", style = "Normal") |>
  body_add_flextable(value = tbl3_flex) |>
  body_add_break(pos = "after") |>
  body_add_par("PROMs after revision of a partial knee replacement", style = "heading 1") |>
  body_add_par("", style = "Normal") |>
  body_add_flextable(value = tbl7_flex) |>
  body_add_break(pos = "after") |>
  body_add_par("Length of stay after revision of a partial knee replacement", style = "heading 1") |>
  body_add_par("", style = "Normal") |>
  body_add_flextable(value = los_flex) |>
  body_add_break(pos = "after") |>
  body_add_par("Counts of revisions of partial knee replacements by year", style = "heading 1") |>
  body_add_par("", style = "Normal") |>
  body_add_gg(value=plot_rev_yr) |>
  body_add_break(pos = "after") |>
  body_add_par("Cumulative incidence of revision for all types of primary partial knee replacement", style = "heading 1") |>
  body_add_par("", style = "Normal") |>
  body_add_gg(value=km_os) |>
  body_add_break(pos = "after") |>
  body_add_par("Cumulative incidence of revision for primary patello-femoral knee replacements versus unicompartmental knee replacements", style = "heading 1") |>
  body_add_par("", style = "Normal") |>
  body_add_gg(value=km_pfj_ukr) |>
  body_add_break(pos = "after") |>
  body_add_par("Competing risk analysis showing the incidence of revision for different indications for all types of primary partial knee replacement", style = "heading 1") |>
  body_add_par("", style = "Normal") |>
  body_add_gg(value=cif_cmp_rsk) |>
  body_add_break(pos = "after") |>
  body_add_par("Cumulative incidence of re-revision following revision of a patello-femoral knee replacement versus a unicompartmental knee replacement", style = "heading 1") |>
  body_add_par("", style = "Normal") |>
  body_add_gg(value=km_rerev)
  
# Save the Word document
print(doc, target = "rUKR.docx")



###################################################################################################

##### Misc code

# # Multi-state modelling
# 
# # A participant can be:
# ## Alive and unrevised 
# ## Dead and unrevised (absorbing)
# ## Alive and revised due to x indication
# ## Revised due to x indication then dead (absorbing)
# 
# # Fields of interest:
# ## Date of death = dod (from ONS table)
# ## Date of revision = revision_date
# ## Indication for revision = ifrhier
# 
# ons <- dbGetQuery(con, "SELECT * FROM ons")
# pk <- merge(pk, ons, by = c("nn_nid"), all.x = TRUE)
# 
# 
# library(mstate)
# library(survival)
# library(ggplot2)
# library(dplyr)
# 
# # Example data
# data <- data.frame(
#   patient_id = 1:100,
#   primary_op_date = as.Date('2020-01-01') + sample(0:1000, 100, replace = TRUE))
# 
# data <-
#   data |> mutate(dod = primary_op_date + sample(0:1000, 100, replace = TRUE),
#                   op_date = primary_op_date + sample(0:1000, 100, replace = TRUE),
#                   ifrhier = sample(1:11, 100, replace = TRUE)
# )
# 
# # Ensure proper NA handling (e.g., patients who are alive and unrevised)
# data$dod[runif(nrow(data)) > 0.5] <- NA
# data$op_date[runif(nrow(data)) > 0.5] <- NA
# data$ifrhier[is.na(data$op_date)] <- NA
# 
# # Transform dates to time since primary_op_date
# data$time_to_dod <- as.numeric(difftime(data$dod, data$primary_op_date, units = "days"))
# data$time_to_revision <- as.numeric(difftime(data$op_date, data$primary_op_date, units = "days"))
# 
# # Define states
# data$state <- "alive_unrevised"
# data$state[!is.na(data$dod) & is.na(data$op_date)] <- "dead_unrevised"
# data$state[is.na(data$dod) & !is.na(data$op_date)] <- "alive_revised"
# data$state[!is.na(data$dod) & !is.na(data$op_date)] <- "dead_revised"
# 
# # Account for cases where died before revision
# data <- data |>
#   mutate(dbr = case_when(time_to_dod<time_to_revision ~1, TRUE ~0)) |>
#   mutate(state = case_when(dbr==1 ~ "dead_unrevised", TRUE ~state)) |>
#   mutate(time_to_revision = case_when(dbr==1 ~ NA_real_, TRUE ~time_to_revision)) |>
#   mutate(ifrhier = case_when(is.na(time_to_revision) ~ NA_real_, TRUE ~ ifrhier)) |>
#   select(-dbr)
#   
# 
# # Handle multiple revision indications
# data$states <- paste0(data$state, "_", data$ifrhier)
# 
# ### Edit from here!!!
# 
# # Convert states to factors for better plotting
# data$state <- factor(data$state, levels = c("alive_unrevised", "dead_unrevised", "alive_revised", "dead_revised"))
# 
# # Define transition matrix
# tmat <- transMat(list(c(2, 3), c(), c(4), c()), names = c("alive_unrevised", "dead_unrevised", "alive_revised", "dead_revised"))
# 
# # Prepare data for the multi-state model
# ms_data <- msprep(data = data,
#                   trans = tmat,
#                   time = c(NA, "time_to_dod", "time_to_revision", "time_to_dod"),
#                   status = c(NA, 1, 1, 1),
#                   keep = c("patient_id", "state", "indication"))
# 
# # Augment data for transitions
# ms_data <- expand.covs(ms_data, covs = "indication", longnames = FALSE)
# 
# # Fit the multi-state model using Cox proportional hazards
# cox_model <- coxph(Surv(Tstart, Tstop, status) ~ strata(trans) + indication, data = ms_data)
# summary(cox_model)
# 
# # Create a dataset for plotting
# plot_data <- data %>%
#   gather(key = "event", value = "time", time_to_dod, time_to_revision) %>%
#   filter(!is.na(time)) %>%
#   arrange(patient_id, time)
# 
# # Plot
# ggplot(plot_data, aes(x = time, y = factor(patient_id), color = state)) +
#   geom_line() +
#   labs(x = "Time (days)", y = "Patient ID", color = "State") +
#   theme_minimal()

