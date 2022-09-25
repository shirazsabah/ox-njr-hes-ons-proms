# R NHSD 7 Create NJR tables


########################################## Create NJR tables ##########################################

# Load rKR data
p_unload(all)
pacman::p_load(pacman, data.table, rio, tidyverse, logger)
setwd(data_dir)
setDTthreads(10)

# Import rKR
rk <- import("NJR/KneeAllRevisions.txt", quote="")
rk <- as.data.table(rk)

# Import pKR
pk <- import("NJR/KneePrimaryOutcomes.txt", quote="")
pk <- as.data.table(pk)

log_info('Initial NJR import: {pk %>% count()} pKA records & {rk %>% count()} rKA records')

# Change all names to lower case
rk <- rk %>% janitor::clean_names()
pk <- pk %>% janitor::clean_names()

# Sort out dates
rk[, op_date := as.Date(fasttime::fastPOSIXct(op_date))]
pk[, primary_op_date := as.Date(fasttime::fastPOSIXct(primary_op_date))]
pk[, revision_date := as.Date(fasttime::fastPOSIXct(revision_date))]

# Drop 'cat_no', 'details' and 'text' columns (by setting them to NULL)
rk[, grep("details", names(rk)) := NULL]
rk[, grep("text", names(rk)) := NULL]
pk[, grep("details", names(pk)) := NULL]
pk[, grep("cat_no", names(pk)) := NULL]
pk[, grep("text", names(pk)) := NULL]

# Replace "NULL", "" or "na" with NAs 
# Need to exclude date columns from this
## Revision dataset
p_load(car)
.cols <- setdiff(colnames(rk), "op_date")
rk[,(.cols):=lapply(.SD, recode, '"NULL"=NA'), .SDcols = .cols]
rk[,(.cols):=lapply(.SD, recode, '""=NA'), .SDcols = .cols]
rk[,(.cols):=lapply(.SD, recode, '"na"=NA'), .SDcols = .cols]

## Primary dataset
.cols <- setdiff(colnames(pk), c("primary_op_date", "revision_date"))
pk[,(.cols):=lapply(.SD, recode, '"NULL"=NA'), .SDcols = .cols]
pk[,(.cols):=lapply(.SD, recode, '""=NA'), .SDcols = .cols]
pk[,(.cols):=lapply(.SD, recode, '"na"=NA'), .SDcols = .cols]

# Convert all character fields to factor
## Revision dataset
changeCols <- colnames(rk)[which(as.vector(rk[,lapply(.SD, class)]) == "character")]
rk[,(changeCols):= lapply(.SD, as.factor), .SDcols = changeCols]

## Primary dataset
changeCols <- colnames(pk)[which(as.vector(pk[,lapply(.SD, class)]) == "character")]
pk[,(changeCols):= lapply(.SD, as.factor), .SDcols = changeCols]

# Look at field class
sapply(rk,class)
sapply(pk,class)

# Convert numeric fields
# (Except for those which need to remain numeric)
## Revision dataset
changeCols <- colnames(rk)[which(as.vector(rk[,lapply(.SD, class)]) == "numeric")]
changeCols <- setdiff(changeCols, "bmi")
rk[,(changeCols):= lapply(.SD, as.factor), .SDcols = changeCols]

## Primary dataset
changeCols <- colnames(pk)[which(as.vector(pk[,lapply(.SD, class)]) == "numeric")]
changeCols <- setdiff(changeCols, c("bmi", "primary_to_outcome_years"))
pk[,(changeCols):= lapply(.SD, as.factor), .SDcols = changeCols]

# There are no duplicates based on all fields
## Not that unique only works in this way if no key has been set
haskey(rk)
unique(rk[,.N])

# Study ID absent
rk %>% filter(nn_nid =="") %>% count()

# Side absent
rk %>% janitor::tabyl(side)

# Invalid age
rk <-rk %>% filter(age_at_revision>=18 & age_at_revision<=105)

log_info('rKA records remaining {rk %>% count()}')

##### Create field to 'number' sequence of linked rKR (revno)

# Initially, consider all linked revision procedures to be separate procedures
rk <-
  as.data.frame(rk) %>% 
  arrange(nn_nid, side, op_date) %>% 
  group_by(nn_nid, side) %>% 
  mutate(seq = row_number()) %>% 
  select(nn_nid, side, op_date, seq, everything())

# Get primary keys (primary_njr_index_no, primary_procedure_id) AND fields to identify knees
pk_id <- as.data.frame(pk) %>% select(nn_nid, side, primary_op_date, primary_njr_index_no, primary_procedure_id, patient_gender, age_at_primary)

# The first rKR on rk has seq==1, so create this as a merge field
pk_id <- pk_id %>% mutate(seq = 1)

# Merge rk to pk_id
rk <- merge(rk, pk_id, by=c("nn_nid", "side", "seq"), all.x = TRUE)

# The first revision with a linked primary should be labelled revno==1
rk <- rk %>% mutate(dummy_revno = case_when(!is.na(primary_njr_index_no) ~ 1, TRUE ~ 0))

# For a given nn_nid, side - Did the first rKR link to a primary?
rk <- rk %>% group_by(nn_nid, side) %>% mutate(linked = max(dummy_revno))

# If it did link, then revno == seq
# Else, code it as 0
rk <- rk %>% mutate(revno = case_when(linked ==1 ~ seq, TRUE ~ 0L))

# Re-code as 0,1,2,3 and create factor
rk <- rk %>% mutate(revno = case_when(revno >3 ~ 3L, TRUE ~ revno))

rk$revno <- factor(rk$revno, levels=c(1,2,3,0), ordered=TRUE, labels = c("First rKR", "Second rKR", "Third or more rKR", "No linked primary"))

rk <- as.data.frame(rk)

# Now, reprocess the data, considering staged revisions to be a single procedure if: (a) ITT as staged and (b) second stage <365 days
# We will only analyse the outcome of first-linked rKR performed as a single-stage procedure
# However, multi-stage procedures must remain in the dataset to calculate individual consultant and surgical unit caseloads

rk1 <-
  rk %>% 
  group_by(nn_nid, side) %>% 
  arrange(nn_nid, side, op_date) %>% 
  mutate(prev_proc = lag(procedure_type, n=1, default = NA)) %>% 
  mutate(date_prev = lag(op_date, n=1, default = NA)) %>% 
  mutate(date_diff = op_date - date_prev) %>% 
  mutate(toseq = case_when(
    procedure_type == "Knee Stage 2 of 2 Stage Revision" & date_diff <365 & prev_proc == "Knee Stage 1 of 2 Stage Revision" ~ 0,
    TRUE ~ 1)) %>%
  filter(toseq==1) %>%
  arrange(nn_nid, side, op_date) %>% 
  group_by(nn_nid, side) %>% 
  mutate(seq1 = case_when(
    revno == "No linked primary" ~ 0L,
    TRUE ~ row_number())) %>% 
  mutate(revno = seq1) %>% 
  mutate(revno = case_when(revno >3 ~ 3L, TRUE ~ revno)) %>%
  ungroup() %>% 
  select(revision_njr_index_no, revision_procedure_id, revno, seq1)

rk1$revno <- factor(rk1$revno, levels=c(1,2,3,0), ordered=TRUE, labels = c("First linked rKR", "Second linked rKR", "Third or more linked rKR", "No linked primary"))

# Merge these new tags back to the main dataset
rk <- rk %>% select(-revno)
rk <- merge(rk, rk1, by=c("revision_njr_index_no", "revision_procedure_id"), all = TRUE)
rm(rk1)

# Create ifrhier field
rk <-
  rk %>% mutate(ifrhier = case_when(
    ind_rev_other==1 ~ 10,
    ind_rev_pain ==1 ~ 9,
    ind_rev_stiffness ==1 ~ 8,
    ind_rev_progressive_arthritis_remaining ==1 ~ 7,
    ind_rev_peri_prosthetic_fracture ==1 ~ 6,
    ind_rev_mds1patella_maltracking==1|ind_rev_component_dissociation ==1|ind_rev_dislocation_subluxation ==1|ind_rev_instability==1 ~ 5,
    ind_rev_mds1wear_of_tibia==1 | ind_rev_mds1wear_of_patella==1|ind_rev_implant_fracture_tibial==1|ind_rev_implant_fracture_patella ==1|ind_rev_implant_fracture_femoral ==1|ind_rev_implant_fracture ==1|ind_rev_wear_of_polyethylene_component==1 ~ 4,
    ind_rev_mds1lysis ==1|ind_rev_lysis_tibia ==1|ind_rev_lysis_femur ==1|ind_rev_mds1aseptic_loosening ==1|ind_rev_aseptic_loosening_patella ==1|ind_rev_aseptic_loosening_tibia==1|ind_rev_aseptic_loosening_femur  ==1 ~ 3,
    ind_rev_mds1incorrect_sizing==1|ind_rev_malalignment ==1 ~ 2,
    ind_rev_infection ==1 ~1,
    TRUE ~ 11))

rk$ifrhier <- factor(rk$ifrhier, levels=c(1:11), ordered=TRUE, labels = c("Infection", "Malalignment", "Loosening/Lysis", "Component Wear", "Instability", "Fracture", "Progressive Arthritis", "Stiffness", "Unexplained pain", "Other", "Not specified")) 

# Drop all ind_rev fields
columns_to_remove <- grep("ind_rev", names(rk))
first_rk <- rk[,-columns_to_remove]

# Create first rKR dataset

# Not in timeframe
setDT(rk)[,year:=format(op_date,"%Y")]
first_rk <- rk %>% filter(op_date >= as.Date('2009-01-01'), op_date <= as.Date('2019-06-30'))
first_rk %>% count()

# Not first revision (and not staged procedure)
first_rk <- first_rk %>% filter(revno=="First linked rKR")
first_rk %>% count()

# Tag subsequent rKR on the opposite knee
first_rk <-
  first_rk %>% 
  group_by(nn_nid) %>%
  arrange(op_date, revision_procedure_id) %>% 
  mutate(oppknee = row_number()) %>% 
  ungroup() %>% 
  filter(oppknee==1) %>% 
  select(-oppknee)



########################################## Bring NJR tables into DuckDB ##########################################

p_load(DBI, duckdb)

## rKR tables
# First rk in timeframe
# Create start/end fields as +/- 7 day timeframe on which to perform a join to HES
setDT(first_rk)[, start := op_date -7]
setDT(first_rk)[, end := op_date +7]

dbWriteTable(con, "first_rk", first_rk)
dbWriteTable(con, "all_rk", rk)
dbWriteTable(con, "pk", pk)

dbListTables(con)