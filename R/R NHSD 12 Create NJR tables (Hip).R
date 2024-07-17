# R NHSD 12 Create NJR tables (Hip)


########################################## Create NJR tables ##########################################

# Load rHR data
# p_unload(all)
pacman::p_load(pacman, data.table, rio, tidyverse, logger)
setwd(data_dir)
setDTthreads(10)

# Import rHR
rh <- import("NJR/HipAllRevisions.txt", quote="")
rh <- as.data.table(rh)

# Import pHR
ph <- import("NJR/HipPrimaryOutcomes.txt", quote="")
ph <- as.data.table(ph)

log_info('Initial NJR import: {ph %>% count()} pKA records & {rh %>% count()} rHA records')

# Change all names to lower case
rh <- rh %>% janitor::clean_names()
ph <- ph %>% janitor::clean_names()

# Sort out dates
rh[, op_date := as.Date(fasttime::fastPOSIXct(op_date))]
ph[, primary_op_date := as.Date(fasttime::fastPOSIXct(primary_op_date))]
ph[, revision_date := as.Date(fasttime::fastPOSIXct(revision_date))]

# Drop 'cat_no', 'details' and 'text' columns (by setting them to NULL)
rh[, grep("details", names(rh)) := NULL]
rh[, grep("text", names(rh)) := NULL]
ph[, grep("details", names(ph)) := NULL]
ph[, grep("cat_no", names(ph)) := NULL]
ph[, grep("text", names(ph)) := NULL]

# Replace "NULL", "" or "na" with NAs 
# Need to exclude date columns from this
## Revision dataset
p_load(car)
.cols <- setdiff(colnames(rh), "op_date")
rh[,(.cols):=lapply(.SD, recode, '"NULL"=NA'), .SDcols = .cols]
rh[,(.cols):=lapply(.SD, recode, '""=NA'), .SDcols = .cols]
rh[,(.cols):=lapply(.SD, recode, '"na"=NA'), .SDcols = .cols]

## Primary dataset
.cols <- setdiff(colnames(ph), c("primary_op_date", "revision_date"))
ph[,(.cols):=lapply(.SD, recode, '"NULL"=NA'), .SDcols = .cols]
ph[,(.cols):=lapply(.SD, recode, '""=NA'), .SDcols = .cols]
ph[,(.cols):=lapply(.SD, recode, '"na"=NA'), .SDcols = .cols]

# Convert all character fields to factor
## Revision dataset
changeCols <- colnames(rh)[which(as.vector(rh[,lapply(.SD, class)]) == "character")]
rh[,(changeCols):= lapply(.SD, as.factor), .SDcols = changeCols]

## Primary dataset
changeCols <- colnames(ph)[which(as.vector(ph[,lapply(.SD, class)]) == "character")]
ph[,(changeCols):= lapply(.SD, as.factor), .SDcols = changeCols]

# Look at field class
sapply(rh,class)
sapply(ph,class)

# Convert numeric fields
# (Except for those which need to remain numeric)
## Revision dataset
changeCols <- colnames(rh)[which(as.vector(rh[,lapply(.SD, class)]) == "numeric")]
changeCols <- setdiff(changeCols, "bmi")
rh[,(changeCols):= lapply(.SD, as.factor), .SDcols = changeCols]

## Primary dataset
changeCols <- colnames(ph)[which(as.vector(ph[,lapply(.SD, class)]) == "numeric")]
changeCols <- setdiff(changeCols, c("bmi", "primary_to_outcome_years"))
ph[,(changeCols):= lapply(.SD, as.factor), .SDcols = changeCols]

# There are no duplicates based on all fields
## Not that unique only works in this way if no key has been set
haskey(rh)
unique(rh[,.N])

# Study ID absent
rh %>% filter(nn_nid =="") %>% count()

# Side absent
rh %>% janitor::tabyl(side)

# Invalid age
rh <-rh %>% filter(age_at_revision>=18 & age_at_revision<=105)

log_info('rHA records remaining {rh %>% count()}')

##### Create field to 'number' sequence of linked rHR (revno)

# Initially, consider all linked revision procedures to be separate procedures
rh <-
  as.data.frame(rh) %>% 
  arrange(nn_nid, side, op_date) %>% 
  group_by(nn_nid, side) %>% 
  mutate(seq = row_number()) %>% 
  select(nn_nid, side, op_date, seq, everything())

# Get primary keys (primary_njr_index_no, primary_procedure_id) AND fields to identify hips
ph_id <- as.data.frame(ph) %>% select(nn_nid, side, primary_op_date, primary_njr_index_no, primary_procedure_id, patient_gender, age_at_primary)

# The first rHR on rh has seq==1, so create this as a merge field
ph_id <- ph_id %>% mutate(seq = 1)

# Merge rh to ph_id
rh <- merge(rh, ph_id, by=c("nn_nid", "side", "seq"), all.x = TRUE)

# The first revision with a linked primary should be labelled revno==1
rh <- rh %>% mutate(dummy_revno = case_when(!is.na(primary_njr_index_no) ~ 1, TRUE ~ 0))

# For a given nn_nid, side - Did the first rHR link to a primary?
rh <- rh %>% group_by(nn_nid, side) %>% mutate(linked = max(dummy_revno))

# If it did link, then revno == seq
# Else, code it as 0
rh <- rh %>% ungroup()
rh <- rh %>% mutate(revno = case_when(linked ==1 ~ seq, TRUE ~ 0L))

# Re-code as 0,1,2,3 and create factor
rh <- rh %>% mutate(revno = case_when(revno >3 ~ 3L, TRUE ~ revno))

rh$revno <- factor(rh$revno, levels=c(1,2,3,0), ordered=TRUE, labels = c("First rHR", "Second rHR", "Third or more rHR", "No linked primary"))

setDT(rh)

# Now, reprocess the data, considering staged revisions to be a single procedure if: (a) ITT as staged and (b) second stage <365 days
# We will only analyse the outcome of first-linked rHR performed as a single-stage procedure
# However, multi-stage procedures must remain in the dataset to calculate individual consultant and surgical unit caseloads

rh1 <-
  rh[, `:=`(prev_proc = shift(procedure_type, n = 1, fill = NA, type = "lag")), by = .(nn_nid, side)][
    , `:=`(date_prev = shift(op_date, n = 1, fill = NA, type = "lag")), by = .(nn_nid, side)][
      , `:=`(date_diff = op_date - date_prev), by = .(nn_nid, side)][
        , `:=`(toseq = fcase(procedure_type == "Hip Stage 2 of 2 Stage Revision" & date_diff < 365 & prev_proc == "Hip Stage 1 of 2 Stage Revision", 0, procedure_type == "Hip Re-operation other than Revision", 0, rep(TRUE, .N), 1)), by = .(nn_nid, side)][
          toseq == 1][
            , `:=`(seq1 = fcase(revno == "No linked primary", 0L, rep(TRUE, .N), seq_len(.N))), by = .(nn_nid, side)][, `:=`(revno = seq1)][
              , `:=`(revno = fcase(revno > 3, 3L, rep(TRUE, .N), revno))][
                , .(revision_njr_index_no, revision_procedure_id, revno, seq1)]

rh1$revno <- factor(rh1$revno, levels=c(1,2,3,0), ordered=TRUE, labels = c("First linked rHR", "Second linked rHR", "Third or more linked rHR", "No linked primary"))

# Merge these new tags back to the main dataset
rh <- rh %>% select(-revno)
rh <- merge(rh, rh1, by=c("revision_njr_index_no", "revision_procedure_id"), all = TRUE)
rm(rh1)

# Need to convert some cols from factor to int first
changeCols = c("ind_rev_periprosthetic_fracture_socket", "ind_rev_periprosthetic_fracture_stem", "ind_rev_peri_prosthetic_fracture", "ind_rev_dislocation_subluxation", "ind_rev_dissociation_of_liner", "ind_rev_wear_of_polyethylene_component", "ind_rev_wear_of_acetabular_component", "ind_rev_implant_fracture_stem", "ind_rev_implant_fracture_socket", "ind_rev_implant_fracture_head", "ind_rev_aseptic_loosening_stem", "ind_rev_aseptic_loosening_socket", "ind_rev_mds1aseptic_loosening", "ind_rev_lysis_stem", "ind_rev_lysis_socket", "ind_rev_mds1lysis", "ind_rev_adverse_soft_tissue_reaction_to_particle_debris", "ind_rev_incorrect_sizing_head", "ind_rev_incorrect_sizing_socket", "ind_rev_mds1incorrect_sizing", "ind_rev_incorrect_sizing", "ind_rev_malalignment_socket", "ind_rev_malalignment_stem", "ind_rev_malalignment", "ind_rev_infection")
rh[,(changeCols):= lapply(.SD, function(x) as.numeric(as.character(x))), .SDcols = changeCols]

# rh %>% 
#   select(all_of(changeCols)) %>% 
#   map(., ~janitor::tabyl(.))


# Include 'Not specified' in with 'Other'
rh <-
  rh %>% mutate(ifrhier = case_when(
    ind_rev_other==1 ~ 9,
    ind_rev_pain ==1 ~ 8,
    ind_rev_periprosthetic_fracture_socket ==1|ind_rev_periprosthetic_fracture_stem ==1|ind_rev_peri_prosthetic_fracture ~7,
    ind_rev_dislocation_subluxation ==1|ind_rev_dissociation_of_liner ==1~ 6,
    ind_rev_wear_of_polyethylene_component ==1|ind_rev_wear_of_acetabular_component ==1|ind_rev_implant_fracture_stem ==1|ind_rev_implant_fracture_socket ==1|ind_rev_implant_fracture_head ==1 ~5,
    ind_rev_aseptic_loosening_stem ==1|ind_rev_aseptic_loosening_socket ==1|ind_rev_mds1aseptic_loosening ==1|ind_rev_lysis_stem ==1|ind_rev_lysis_socket ==1|ind_rev_mds1lysis ==1 ~ 4,
    ind_rev_adverse_soft_tissue_reaction_to_particle_debris ==1 ~ 3,
    ind_rev_incorrect_sizing_head ==1|ind_rev_incorrect_sizing_socket ==1|ind_rev_mds1incorrect_sizing ==1|ind_rev_incorrect_sizing ==1|ind_rev_malalignment_socket ==1|ind_rev_malalignment_stem ==1|ind_rev_malalignment ==1 ~ 2,
    ind_rev_infection ==1 ~ 1,
    TRUE ~ 9))

rh$ifrhier <- factor(rh$ifrhier, levels=c(1:9), ordered=TRUE, labels = c("Infection", "Malalignment/Size mismatch", "Adverse soft tissue reaction", "Loosening/Lysis", "Component Wear/Breakage", "Dislocation/Instability", "Fracture", "Unexplained pain", "Other")) %>% droplevels()

# Drop all ind_rev fields
columns_to_remove <- grep("ind_rev", names(rh))
first_rh <- rh[,-columns_to_remove]

# Create first rHR dataset

# Not in timeframe
setDT(rh)[,year:=format(op_date,"%Y")]
first_rh <- rh %>% filter(op_date >= as.Date('2009-01-01'), op_date <= as.Date('2019-06-30'))
first_rh %>% count()

# Not first revision (and not staged procedure)
first_rh <- first_rh %>% filter(revno=="First linked rHR")
first_rh %>% count()

# Tag subsequent rHR on the opposite hip
first_rh <-
  first_rh %>% 
  group_by(nn_nid) %>%
  arrange(op_date, revision_procedure_id) %>% 
  mutate(opphip = row_number()) %>% 
  ungroup() %>% 
  filter(opphip==1) %>% 
  select(-opphip)

first_rh %>% count()



########################################## Bring NJR tables into DuckDB ##########################################

pacman::p_load(DBI, duckdb)

# ## rHR tables
# # First rh in timeframe
# # Create start/end fields as +/- 7 day timeframe on which to perform a join to HES
# setDT(first_rh)[, start := op_date -7]
# setDT(first_rh)[, end := op_date +7]

ph <- as.data.frame(ph)
first_rh <- as.data.frame(first_rh)
rh <- as.data.frame(rh)


# write.csv(ph,file=paste0(data_dir,"/EXPORT/ph.csv"))
# write.csv(first_rh,file=paste0(data_dir,"/EXPORT/first_rh.csv"))
# write.csv(rh,file=paste0(data_dir,"/EXPORT/rh.csv"))

# Connect to database
con = dbConnect(duckdb::duckdb(), dbdir=paste0(data_dir,"SQL/ss.duckdb"), read_only=FALSE)

dbListTables(con)

# This work-around is needed for some bad unicode (rather than just writing table directly)

# save.image(paste0(data_dir,"R_IMAGES/hips.RData"))
# load(file=paste0(data_dir,"R_IMAGES/hips.RData"))

rh <- rh %>% select(-date_diff)
rh <- rh %>% rename(gender = patient_gender.x)
rh <- rh %>% rename(gender2 = patient_gender.y)
# Drop all ind_rev fields
columns_to_remove <- grep("ind_rev", names(rh))
rh <- rh[,-columns_to_remove]

write.csv(ph, paste0(data_dir,"EXPORT/ph.csv"), row.names=FALSE)
write.csv(rh, paste0(data_dir,"EXPORT/all_rh.csv"), row.names=FALSE)
write.csv(first_rh, paste0(data_dir,"EXPORT/first_rh.csv"), row.names=FALSE)

dbExecute(con, paste0("CREATE TABLE ph AS SELECT * FROM read_csv_auto('", data_dir,"EXPORT/ph.csv', ALL_VARCHAR=1, header=TRUE)"))
dbExecute(con, paste0("CREATE TABLE all_rh AS SELECT * FROM read_csv_auto('", data_dir,"EXPORT/all_rh.csv', ALL_VARCHAR=1, header=TRUE)"))
dbExecute(con, paste0("CREATE TABLE first_rh AS SELECT * FROM read_csv_auto('", data_dir,"EXPORT/first_rh.csv', ALL_VARCHAR=1, header=TRUE)"))

dbExecute(con, "UPDATE ph SET primary_op_date= NULL WHERE primary_op_date = 'NA';")
dbExecute(con, "UPDATE ph SET revision_date= NULL WHERE revision_date = 'NA';")
dbExecute(con, "ALTER TABLE ph ALTER primary_op_date TYPE DATE;")
dbExecute(con, "ALTER TABLE ph ALTER revision_date TYPE DATE;")

dbExecute(con, "UPDATE all_rh SET primary_op_date= NULL WHERE primary_op_date = 'NA';")
dbExecute(con, "UPDATE all_rh SET date_prev= NULL WHERE date_prev = 'NA';")
dbExecute(con, "ALTER TABLE all_rh ALTER op_date TYPE DATE;")
dbExecute(con, "ALTER TABLE all_rh ALTER primary_op_date TYPE DATE;")
dbExecute(con, "ALTER TABLE all_rh ALTER date_prev TYPE DATE;")

dbExecute(con, "UPDATE first_rh SET primary_op_date= NULL WHERE primary_op_date = 'NA';")
dbExecute(con, "UPDATE first_rh SET op_date= NULL WHERE op_date = 'NA';")
dbExecute(con, "UPDATE first_rh SET date_prev= NULL WHERE date_prev = 'NA';")
dbExecute(con, "ALTER TABLE first_rh ALTER primary_op_date TYPE DATE;")
dbExecute(con, "ALTER TABLE first_rh ALTER op_date TYPE DATE;")
dbExecute(con, "ALTER TABLE first_rh ALTER date_prev TYPE DATE;")

# # Find columns that end with...
# rh %>%
#   select(ends_with(".y")) %>% 
#   head()



########################################## Shutdown DuckDB con ##########################################

# Shutdown database
dbDisconnect(con, shutdown=TRUE)
