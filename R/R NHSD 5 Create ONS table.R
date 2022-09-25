# R NHSD 5 Create ONS table


########################################## Create ONS table ##########################################

log_info('Create ONS table')

ons <- rio::import(paste0(data_dir, "ONS/FILE0154086_NIC380650_ONS_Mortality.txt", quote=""))

p_load(janitor)

# Clean and process ONS data
## Count records
ons %>% count()

# ## Non-duplicates on all fields
# ons <- unique(ons)

## Sort out variable names 
ons <- clean_names(ons)
ons <- ons %>% rename(nn_nid = study_id)

## Keep only records with nn_nids that can match to NJR
## (i.e. records with short nn_nid character lengths)
ons <- ons %>% filter(nchar(nn_nid) <12)

## Study ID present
ons %>% count()

## Check that nn_nid is a unique identifier -> It isn't
eeptools::isid(ons, "nn_nid")

## Find duplicates on nn_nid in ONS
## ONS match_rank
### 0 indicates that the death record is present in HES only, because an ONS record could not be matched to HES, or the death record is not available in the ONS dataset.
### 1 corresponds to an exact match between the confirmed NHS number in the ONS data and the PERSON_ID in the HES data.
### 8 corresponds to an exact match of date of birth, sex and postcode for the ONS and HES data (excluding any records where the date of birth is 1 January).
## If equal match_rank, take earliest date of death

### Set match_rank as factor to allow custom sort order (1,8,0)
ons$match_rank <- factor(ons$match_rank, levels=c(1,8,0), ordered=TRUE)

ons <- ons %>% 
  group_by(nn_nid) %>%
  arrange(match_rank, dod) %>% 
  mutate(dup = row_number()) %>% 
  ungroup()

## Explore duplicates
ons %>% tabyl(dup)

## Remove duplicates
ons <- ons %>% filter(dup==1) %>% select(-dup)

## Check that nn_nid is a unique identifier
eeptools::isid(ons, "nn_nid")

## Count records
ons %>% count()

# Add ONS to DuckDB
dbWriteTable(con, "ons", ons)

dbListTables(con)