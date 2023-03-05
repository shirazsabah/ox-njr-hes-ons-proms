# R NHSD 15 Analyse rHA outcomes

p_unload(all)
pacman::p_load(pacman, data.table, tidyverse, DBI, duckdb, tictoc, janitor, survival, KMunicate, flextable, gtsummary, stringr, logger)


########################################## Summary ##########################################

# Population of interest
  ## First rHA

# Exposure of interest
  ## Indication for revision surgery

# Timeframe
  ## Date of surgery from 1/1/2009 to 30/06/2019

# Outcome measures
  #1 Implant survivorship
        ## a. Logistic regression at 2y (not used here; set up for rHA caseload study; will eventually need filtering from 1/1/2009 to 31/12/2017;
        ## b. Survival analysis

  #2 PROMs
    ## a. OHS score (pre, post, change)
    ## b. EQ-5D index (pre, post, change)
    ## c. Number achieving MIC
    ## +/- d. Patient-reported adverse events

  #3 Mortality at 90 days

  #4 Serious complications at 90 days

  #5 Length of stay


# Analyse #1 IS, #3 Mort, #4 Comps, #5 LOS, #2 PROMs (because of order of joins)



########################################## Connect to db ##########################################

# Connect to db
con = dbConnect(duckdb::duckdb(), dbdir=paste0(data_dir,"SQL/ss.duckdb"), read_only=TRUE)

# PRAGMAs
dbExecute(con, "PRAGMA threads=10")
dbExecute(con, paste0("PRAGMA temp_directory='", data_dir,"TEMP/hes.tmp'"))
dbExecute(con, "PRAGMA memory_limit='32GB'")

# Check tables have loaded as expected
dbListTables(con)

# Bring rhhes into environment
rhhes <- dbGetQuery(con, "SELECT * FROM rh_ons_hes_proms")



########################################## 1. Implant survivorship ##########################################

first_is <- rhhes

##### a. Logistic regression at 2y
  ## Create an outcome variable for first rHA procedures, by reshaping second revision procedures from long to wide
  ## Mortality is ignored and follow-up is from date of revision to date of re-revision or end of study
    ### nn_nid, side --> identify the hip
    ### revision_njr_index_no and revision_procedure_id --> identify the revision record
    ### op_date --> needed to calculate time to second revision (ttsr) --> second revision within 2 years (sr2y) [730 days]
  ## Note: 
    ### This uses the dataset 'rh', which includes all rHA on the NJR
    ### i.e. Second, third, fourth, etc.
    ### Follow-up is to 31/12/2019, hence year %between% c("2009", "2019")

# Bring all rHA into R environment
rh <- dbGetQuery(con, "SELECT * FROM all_rh")

second_is <-
  rh %>%
  filter(year %between% c("2009", "2019"), revno=="Second linked rHR") %>% 
  select(nn_nid, side, revision_njr_index_no, revision_procedure_id, op_date) %>% 
  rename(op_date_2 = op_date, revision_njr_index_no_2 = revision_njr_index_no, revision_procedure_id_2 = revision_procedure_id)

# Merge first and second rHA
# However, there are some (likely) duplicates when time to second rHA = 0 days
first_is <-
  merge(first_is, second_is, by=c("nn_nid", "side"), all.x = TRUE) %>% 
  mutate(ttsr = as.numeric(op_date_2 - op_date)) %>%
  mutate(ttsr = case_when(ttsr == 0 ~ NA_real_,
                          TRUE ~ ttsr))

# Create table of third revisions
third_is <-
  rh %>%
  filter(year %between% c("2009", "2019"), revno=="Third or more linked rHR") %>% 
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

# # Check code works as expected for patients who do not undergo re-revision
# x <- first_is %>% janitor::tabyl(ttsr, sr2y)

# Calculate revision rate at 2 years by diagnosis
is2y <-
first_is %>% tabyl(sr2y, ifrhier) %>% 
  adorn_percentages("col") %>% 
  adorn_pct_formatting(digits = 2) %>% 
  adorn_ns()



##### b. Implant survivorship
# Create follow-up time and outcome fields

## Final follow-up date
first_is <- first_is %>% mutate(ffu = as.Date('2019-12-31'))

# Create outcome field
  ## Unrevised
  ## Re-revised
  ## Dead
    ## If a patient died on the same day as their re-revision, outcome = re-revised
    ## Conditional logic must reflect this
    ## We will (initially) ignore patients who died before re-revision

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
# Edit the KMunicate function for this session only
trace("KMunicate",edit=TRUE)
# Change ylim on L81 manually to c(0,0.3)

# Plot one survival graph with all the different indications for rHA
KM <- survfit(Surv(fu, outcome_sa) ~factor(ifrhier), data=first_is)
time_scale <- seq(0, ceiling(max(first_is$fu)), by=1)
p <- KMunicate(fit = KM, time_scale = time_scale,
          .risk_table = NULL,
          .reverse = TRUE,
          .xlab = "Time (years)",
          .ylab = "Cumulative incidence of re-revision",
          .alpha = 0 # To make confidence intervals completely transparent!
          ) 

p <- p + theme(legend.position="right")

# Plot survival graphs for each indication with 95% CIs and risk tables

setwd(paste0(data_dir,"FIGS/proms-rfr-hip/"))

rfr <- c("Infection", "Malalignment/Size mismatch", "Adverse soft tissue reaction", "Loosening/Lysis", "Component Wear/Breakage", "Dislocation/Instability", "Fracture", "Unexplained pain", "Other")

for (val in rfr) {
  
  KMfilt <- first_is %>% filter(ifrhier == val)
  
  KMrfr <- survfit(Surv(fu, outcome_sa) ~1, data=KMfilt)
  time_scale <- seq(0, ceiling(max(KMfilt$fu)), by=1)
  p_rfr <- KMunicate(fit = KMrfr, time_scale = time_scale,
                     .risk_table_base_size = 10, # Include a table below graph of number at-risk
                     .rel_heights = c(1, 0.25), # This plots the graph as 100% size, and the table below it as 25% size
                 .reverse = TRUE,
                 .xlab = "Time (years)",
                 .ylab = "Cumulative incidence of re-revision",
                 .alpha = 0.4)

  plotlist = list()
  plotlist[[1]] = p_rfr
  
  for(i in 1:1) {
    ggsave(plot = plotlist[[i]], 
           width = 200,
           height = 200,
           units = "mm",
           dpi = 1000,
           bg = 'white',
           file = paste("Fig",substr(val, 1, 5),".png",sep=""))
  }
}

# Survival tables
# # NB: This code outputs all data (events/censoring)
# p_load(broom)
# survfit(Surv(fu, outcome_sa) ~factor(ifrhier), data=first_is) %>% tidy()

res <- summary(KM, times = c(0, 2, 5, 10))
cols <- lapply(c(1:6, 10,15:16) , function(x) res[x])
survtbl <- do.call(data.frame, cols)
survtbl <- survtbl %>% select(-n)
survtbl$strata <- str_remove(survtbl$strata, '^factor\\(ifrhier\\)=') ### Need to escape brackets with \\ for them to be removed

# Switch to cumulative incidence
survtbl$surv <- (1-survtbl$surv)*100
survtbl$lower <- (1-survtbl$lower)*100
survtbl$upper <- (1-survtbl$upper)*100

survtbl <- survtbl %>% 
  mutate(across(c(5,7,8), round, 1)) %>% 
  relocate(strata)

survtbl <- survtbl %>% relocate(-lower)

survtbl

# Create flextable
surv_flex <- flextable(survtbl)
surv_flex <- set_header_labels(surv_flex,
                             strata = "Indication",
                             time = "Time (years)",
                             n.risk = "Number at risk",
                             n.event = "Number re-revised",
                             n.censor = "Number censored",
                             surv = "Cumulative incidence re-revision",
                             upper = "Lower 95% CI",
                             lower = "Upper 95% CI"
                             )

surv_flex <- hline(surv_flex, c(4,8,12,16,20,24,28,32), part = "body")
#surv_flex


# Implant survival rows for combined table
two.ys <- survtbl %>% filter(time==2) %>% select(strata, surv, upper, lower) %>% mutate(two.ys = paste0(surv, " (", upper, " - ", lower, ")")) %>% select(strata, two.ys)
fiv.ys <- survtbl %>% filter(time==5) %>% select(strata, surv, upper, lower) %>% mutate(fiv.ys = paste0(surv, " (", upper, " - ", lower, ")")) %>% select(strata, fiv.ys)

# Reshape
rownames(two.ys) <- two.ys[,1]
two.ys[,1] <- NULL
two.ys["fiv.ys"] <- fiv.ys$fiv.ys
two.ys

# Create rows
comb_tbl <-
  two.ys %>%
  tibble::rownames_to_column() %>%  
  pivot_longer(-rowname) %>% 
  pivot_wider(names_from=rowname, values_from=value) %>% 
  rename(`Characteristic` = "name") %>%
  mutate(Characteristic = case_when(Characteristic == "two.ys" ~ "2 years / % (95% CI)",
                                    Characteristic == "fiv.ys" ~ "5 years / % (95% CI)",
                                    TRUE ~ Characteristic))

# Add column: Overall
KM.overall <- survfit(Surv(fu, outcome_sa) ~1, data=first_is)
res <- summary(KM.overall, times = c(0, 2, 5, 10))
cols <- lapply(c(1:15) , function(x) res[x])
tbl <- do.call(data.frame, cols)

# Switch to cumulative incidence
tbl$surv <- (1 - tbl$surv)*100
tbl$lower <- (1 - tbl$lower)*100
tbl$upper <- (1 - tbl$upper)*100

comb_tbl["Overall"] <- c(paste0(tbl %>% filter(time==2) %>% select(surv) %>% round(1), " (", tbl %>% filter(time==2) %>% select(upper) %>% round(1), " - ", tbl %>% filter(time==2) %>% select(lower) %>% round(1), ")"),
                       paste0(tbl %>% filter(time==5) %>% select(surv) %>% round(1), " (", tbl %>% filter(time==5) %>% select(upper) %>% round(1), " - ", tbl %>% filter(time==5) %>% select(lower) %>% round(1), ")"))
                       
comb_tbl <- comb_tbl %>% relocate(Characteristic, Overall)
#comb_tbl

rm(cols, fiv.ys, res, time_scale, two.ys)



########################################## Baseline table ##########################################

# Create table of baseline characteristics, stratified by indication for revision

## Procedure
### Year of surgery (year)

## Patient
### Age (age_at_revision)
### Gender (gender)
### ASA (asa_grade)
### Charlson comorbidity index (charl01)
### Body mass index (bmi_plaus)
### Ethnicity (ETHNIC5)
### Index of multiple deprivation decile (imd_dec)

## First rHA recruited until 30/06/2019

t1 <- first_is %>% select(age_at_revision, gender, asa_grade, charl01, bmi_plaus, ethnic5, imd5, year, ifrhier) %>% droplevels()

theme_gtsummary_compact()

tbl1 <- 
  tbl_summary(
    t1,
    by = "ifrhier", # split table by group
    missing = "no",
    type = all_continuous() ~ "continuous2",
    statistic = list(
      all_continuous() ~ c("{mean} ({sd})",
                           "{N_miss} ({p_miss}%)"),
      all_categorical(dichotomous = FALSE) ~ "{n} ({p}%)",
      all_dichotomous() ~ "{n} ({p}%)"),
    label = list(
      year = "Year of surgery",
      age_at_revision = "Age at first rHA (years)",
      gender = "Gender",
      asa_grade = "ASA Class",
      charl01 = "Charlson comorbidity index",
      bmi_plaus = "Body mass index",
      ethnic5 = "Ethnicity",
      imd5 = "Index of multiple deprivation")) %>%
  add_overall() %>%
  bold_labels() %>%
  italicize_levels()

tbl1_flex <- tbl1 %>% as_flex_table()


########################################## 3. Mortality at 90 days ##########################################

## Create field mortality y/n at 90 days
first_is <- as.data.frame(first_is) %>% mutate(mort90 = case_when(as.Date(dod) - op_date <=90 ~ 1, TRUE ~ 0))

# mort90d <-
#   first_is %>% tabyl(mort90, ifrhier) %>%
#   adorn_percentages("col") %>%
#   adorn_pct_formatting(digits = 2) %>%
#   adorn_ns()
# 
# mort90d

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

# # Test function
# pois_perc_ci(t2, "mort90")

# This determines the order of the table
t2 = first_is %>% select("mort90", "ifrhier")

tbl2 <-
  t2 %>% 
  dplyr::mutate_if(~inherits(., "ordered"), ~factor(., ordered = FALSE)) %>%
  tbl_custom_summary(
    by = "ifrhier",
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


# Multiple cursors on same line = Find > In selection > All
tbl2[["table_body"]][["stat_0"]][2] <- stringr::str_remove_all(tbl2[["table_body"]][["stat_0"]][2], "\\.0")
tbl2[["table_body"]][["stat_1"]][2] <- stringr::str_remove_all(tbl2[["table_body"]][["stat_1"]][2], "\\.0")
tbl2[["table_body"]][["stat_2"]][2] <- stringr::str_remove_all(tbl2[["table_body"]][["stat_2"]][2], "\\.0")
tbl2[["table_body"]][["stat_3"]][2] <- stringr::str_remove_all(tbl2[["table_body"]][["stat_3"]][2], "\\.0")
tbl2[["table_body"]][["stat_4"]][2] <- stringr::str_remove_all(tbl2[["table_body"]][["stat_4"]][2], "\\.0")
tbl2[["table_body"]][["stat_5"]][2] <- stringr::str_remove_all(tbl2[["table_body"]][["stat_5"]][2], "\\.0")
tbl2[["table_body"]][["stat_6"]][2] <- stringr::str_remove_all(tbl2[["table_body"]][["stat_6"]][2], "\\.0")
tbl2[["table_body"]][["stat_7"]][2] <- stringr::str_remove_all(tbl2[["table_body"]][["stat_7"]][2], "\\.0")
tbl2[["table_body"]][["stat_8"]][2] <- stringr::str_remove_all(tbl2[["table_body"]][["stat_8"]][2], "\\.0")
tbl2[["table_body"]][["stat_9"]][2] <- stringr::str_remove_all(tbl2[["table_body"]][["stat_9"]][2], "\\.0")

tbl2_flex <- tbl2 %>% as_flex_table()

## Create rows for combined table
x <- unname(as.data.frame(tbl2_flex$body$dataset))
colnames(x) <- names(comb_tbl)
comb_tbl <- rbind(comb_tbl, x)
comb_tbl

rm(x)



########################################## 4. Complications at 90 days ##########################################

## Complications fields
  ### Acute stroke = compacva <- c("I60",	"I61",	"I62",	"I63", "I610",	"I611",	"I612",	"I613",	"I614",	"I615",	"I616",	"I618",	"I619", "I630",	"I631",	"I632",	"I633",	"I634",	"I635",	"I636",	"I638",	"I639")
  ### Lower resp. tract infection = complrti <- c("J12",	"J13",	"J14",	"J15",	"J16",	"J18",	"J20",	"J22",	"J86", "J440",	"J441",	"J851",	"J852",	"J690")
  ### MI = compmi <- c("I21", "I210",	"I211",	"I212",	"I213",	"I214",	"I219",	"I220",	"I221", "I228", "I229")
  ### DVT = compdvt <- c("I801", "I802", "I803", "I808", "I809", "I828", "I829", "I824")
  ### PE = comppe <- c("I822",	"I823",	"I260",	"I269")
  ### AKI = compaki <- c("N170", "N171", "N172", "N178", "N179")
  ### UTI = computi <- c("N300", "N308", "N309", "N390")
  ### Wound dehiscence = compwound <- c("T813")
  ### SSI = compssi <- c("T814")
  ### Periprosthetic fracture = compppf <- c("M966")
  ### Complication with prosthesis = comppros <- c("T840")
  ### Neurovascular injury = compnvinj <- c("T812")
  ### Blood transfusion = compbloodtx <- c("X331", "X332", "X333", "X337", "X338", "X339", "X341", "X342", "X343", "X344")
# TRUE, FALSE, NA (if episode not linked to HES)

# This keeps episodes not linked to HES as NA (as desired)
comps <-
  first_is %>% 
  select(EPIKEY, ifrhier, compacva90, complrti90, compmi90, compdvt90, comppe90, compaki90, computi90) %>% 
  mutate(any_med_comp =
           if_else(if_any(.cols = c(compacva90, complrti90, compmi90, compdvt90, comppe90, compaki90, computi90),`==`,TRUE),TRUE, FALSE))

comps <-
  comps %>% 
  filter(!is.na(EPIKEY) & EPIKEY!=0 & !is.na(any_med_comp))

log_info('Number of NJR-ONS-HES linked episodes: {comps %>% count()}')

# This determines the order of the table
t3 = comps %>% select("any_med_comp", "compaki90", "compdvt90", "complrti90", "compmi90", "comppe90", "compacva90", "computi90", "ifrhier")

tbl3 <-
  t3 %>% 
  dplyr::mutate_if(~inherits(., "ordered"), ~factor(., ordered = FALSE)) %>%
  tbl_custom_summary(
    by = "ifrhier",
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

# Clean up decimal places
x <- c(2,5,8,11,14,17,20,23)

for(i in x) {
  tbl3[["table_body"]][["stat_0"]][i] <- stringr::str_remove_all(tbl3[["table_body"]][["stat_0"]][i], "\\.0")
  tbl3[["table_body"]][["stat_1"]][i] <- stringr::str_remove_all(tbl3[["table_body"]][["stat_1"]][i], "\\.0")
  tbl3[["table_body"]][["stat_2"]][i] <- stringr::str_remove_all(tbl3[["table_body"]][["stat_2"]][i], "\\.0")
  tbl3[["table_body"]][["stat_3"]][i] <- stringr::str_remove_all(tbl3[["table_body"]][["stat_3"]][i], "\\.0")
  tbl3[["table_body"]][["stat_4"]][i] <- stringr::str_remove_all(tbl3[["table_body"]][["stat_4"]][i], "\\.0")
  tbl3[["table_body"]][["stat_5"]][i] <- stringr::str_remove_all(tbl3[["table_body"]][["stat_5"]][i], "\\.0")
  tbl3[["table_body"]][["stat_6"]][i] <- stringr::str_remove_all(tbl3[["table_body"]][["stat_6"]][i], "\\.0")
  tbl3[["table_body"]][["stat_7"]][i] <- stringr::str_remove_all(tbl3[["table_body"]][["stat_7"]][i], "\\.0")
  tbl3[["table_body"]][["stat_8"]][i] <- stringr::str_remove_all(tbl3[["table_body"]][["stat_8"]][i], "\\.0")
  tbl3[["table_body"]][["stat_9"]][i] <- stringr::str_remove_all(tbl3[["table_body"]][["stat_9"]][i], "\\.0")
}

tbl3_flex <- tbl3 %>% as_flex_table()


## Create rows for combined table
x <- unname(as.data.frame(tbl3_flex$body$dataset))
colnames(x) <- names(comb_tbl)
comb_tbl <- rbind(comb_tbl, x)
comb_tbl

rm(x)



########################################## 5. Length of stay ##########################################

# FCE (EPIDUR)
# Spell (SPELDUR)
# CIPS (los)

los <-
  first_is %>%
  select(EPIDUR, SPELDUR, los, ifrhier, EPIKEY) %>% 
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
  group_by(ifrhier) %>% 
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
los_flex <- add_header_row(los_flex, values = c("","Indication for revision"), colwidths = c(1,9))
los_flex <- align(los_flex, align = "center", part = "header")
los_flex <- bold(los_flex, c(1,7,13), bold = TRUE)
los_flex <- italic(los_flex, c(2:6,8:12,14:18), 1, italic = TRUE)
los_flex <- bold(los_flex, bold = TRUE, part = "header")
los_flex


## Create rows for combined table
aggs <- list(
  median = ~median(.x, na.rm = TRUE),
  q1 = ~round(quantile(.x, na.rm = TRUE, prob=c(.25)),0),
  q3 = ~round(quantile(.x, na.rm = TRUE, prob=c(.75)),0)
)

# Create rows for combined table
x <-
  los %>% 
  group_by(ifrhier) %>% 
  summarise(across(c(los), aggs)) %>% 
  mutate(`Length of stay (days) / median (Q1 - Q3)` = paste0(los_median, " (", los_q1, " - ", los_q3,")")) %>% 
  select(ifrhier, `Length of stay (days) / median (Q1 - Q3)`) %>% 
  tibble::rownames_to_column() %>%  
  pivot_longer(-rowname) %>% 
  pivot_wider(names_from=rowname, values_from=value) %>% 
  filter(!name =="ifrhier")

y <-
  los %>%
  summarise(across(c(los), aggs))

y <- c(paste0(y$los_median, " (", y$los_q1, " - ", y$los_q3, ")"))

x["Overall"] <- y

x <- x %>% relocate(name, Overall)

colnames(x) <- names(comb_tbl)

comb_tbl <- rbind(comb_tbl, x)
comb_tbl

rm(x, cols, col1, y)



########################################## Combined table ##########################################

# Combined outcome table
comb_tbl <-
  comb_tbl %>% 
  add_row(`Characteristic` = "Cumulative incidence re-revision", .before= 1) %>%
  add_row(`Characteristic` = "Complications", .before= 7) %>%
  add_row(`Characteristic` = "Hospital admission", .before= 32)

comb_tbl <- comb_tbl %>% replace(is.na(.), "")
  
  
set_flextable_defaults(font.size = 7, padding = 0, line_spacing = 1)
theme_gtsummary_compact()
comb_tbl_flex <- flextable(comb_tbl)
comb_tbl_flex <- add_header_row(comb_tbl_flex, values = c("","Indication for revision"), colwidths = c(2,9))
comb_tbl_flex <- align(comb_tbl_flex, align = "center", part = "header")
comb_tbl_flex <- bold(comb_tbl_flex, bold = TRUE, part = "header")
comb_tbl_flex <- bold(comb_tbl_flex, c(1,4,7,32), bold = TRUE)
comb_tbl_flex <- italic(comb_tbl_flex, c(2:3,5:6,8:31, 33), 1, italic = TRUE)
comb_tbl_flex



########################################## 2. PROMs ##########################################


## Create proms dataset for analysis
proms <- first_is %>% filter(!is.na(proms_serial_no))

## Count records
proms %>% count()

## Outcomes
## a. OHS score (pre, post, change)
## b. EQ-5D index (pre, post, change)
## c. Number achieving MIC (mic01 is based on a change score of >=6 ~ yes -- this corresponds with mic-pred-adj for rHA)

# Create PROM outcomes table

## Prep
p_load(gtsummary, flextable)

# Select variables
t4 = proms %>% filter(!is.na(proms_serial_no)) %>% select("hr_q1_score", "hr_q2_score", "ch", "mic01", "q1_eq5d_index", "q2_eq5d_index", "eq5d_index_change", "q2_satisfaction", "q2_success", "ifrhier") %>% droplevels()

tbl4 <- 
  tbl_summary(
    t4,
    by = "ifrhier", # split table by group
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
      hr_q1_score = "Pre-operative OHS",
      hr_q2_score = "Post-operative OHS",
      ch = "Change in OHS",
      mic01 = "Responder",
      q1_eq5d_index = "Pre-operative EQ-5D utility",
      q2_eq5d_index = "Post-operative EQ-5D utility",
      eq5d_index_change = "Change in EQ-5D utility",
      q2_satisfaction = "Patient satisfaction",
      q2_success = "Perceived success")) %>%
  add_overall() %>%
  bold_labels() %>%
  italicize_levels() %>% 
  add_stat_label(label = c(q1_eq5d_index,q2_eq5d_index, eq5d_index_change) ~ c("Median (Q1, Q3)", "N missing (% missing)")) #%>%
  #modify_header(all_stat_cols(FALSE) ~ "**{level}**", stat_0 ~ "**Overall**")

tbl4_flex <- tbl4 %>% as_flex_table()

# Satisfaction & Perceived success as binary outcomes
t5 = proms %>% filter(!is.na(proms_serial_no)) %>% select("sat", "suc", "ifrhier") %>% droplevels()

tbl5 <- 
  tbl_summary(
    t5,
    by = "ifrhier", # split table by group
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

t6 <- first_is %>% select("promyn", "ifrhier")

tbl6 <- 
  tbl_summary(
    t6,
    by = "ifrhier", # split table by group
    missing = "no",
    statistic = list(
      all_dichotomous() ~ "{n}/{N} ({p}%)"),
    label = list(
      promyn = "PROM records linked")) %>%
  add_overall() %>%
  bold_labels() %>%
  italicize_levels() %>% 
  modify_header(all_stat_cols(FALSE) ~ "**{level}**", stat_0 ~ "**Overall**")

tbl6_flex <- tbl6 %>% as_flex_table()

# PROM attrition could be linked into the main PROM table if desired
tbl7 <-
  tbl_stack(list(tbl6, tbl4)) %>%
  modify_footnote(all_stat_cols() ~ "Except where specified, percentages and counts were based on linked PROM records (not the total of first rHA on the NJR).")

tbl7_flex <- tbl7 %>% as_flex_table()


# Selected 95% CIs for results
p_load(Hmisc)
round(binconf(73, 95, alpha = 0.05),2)


########################################## Shutdown DuckDB con ##########################################

# Shutdown database
dbDisconnect(con, shutdown=TRUE)



########################################## Keep only objects needed ##########################################

#rm(list= ls()[! (ls() %in% c('base_tbl','comb_tbl_flex', 'comps_table','directory','directory2', 'flex_surv','p','los_flex','proms_table'))])

save.image(paste0(data_dir,"R_IMAGES/multimodal_rhr.RData"))



########################################## Export images ##########################################

setwd(paste0(data_dir,"FIGS/proms-rfr-hip/"))

plotlist = list()
plotlist[[1]] = p

for(i in 1:1) {
  ggsave(plot = plotlist[[i]], 
         width = 200,
         height = 200,
         units = "mm",
         dpi = 1000,
         file = paste("file",i,".png",sep=""))
}

