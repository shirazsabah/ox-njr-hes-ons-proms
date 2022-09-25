# R NHSD 10 Join Primary knee replacements-ONS-HES-PROMs

p_unload(all)
pacman::p_load(pacman, data.table, DBI, duckdb, tidyverse, tictoc)

# Source R NHSD 1 to set-up directories

# Connect to database
con = dbConnect(duckdb::duckdb(), dbdir=paste0(data_dir,"SQL/ss.duckdb"), read_only=FALSE)

# PRAGMAs
dbExecute(con, "PRAGMA threads=10")
dbExecute(con, paste0("PRAGMA temp_directory='", data_dir,"TEMP/hes.tmp'"))
dbExecute(con, "PRAGMA memory_limit='32GB'")

# Check tables have loaded as expected
dbListTables(con)

########################################## Merge pk to ONS ##########################################

# Keep the first pk for a given patient
dbExecute(con, "CREATE TABLE pk_first AS
          WITH cte AS
          (
            SELECT *,
            ROW_NUMBER() OVER (PARTITION BY nn_nid ORDER BY primary_op_date ASC) AS rn
            FROM pk
          )
          SELECT *
            FROM cte
          WHERE rn = 1;")


## LEFT JOIN
## The syntax is:
## SELECT lefttbl righttbl
## FROM lefttbl
## LEFT OUTER JOIN righttbl
## ON righttbl.id = lefttbl.id

# Merge
dbExecute(con, "CREATE TABLE pk_ons AS
                SELECT pk_first.*, ons.*
                FROM pk_first
                LEFT OUTER JOIN ons
                ON ons.nn_nid = pk_first.nn_nid;")
      
# Exclude records where DOD < DOR
dbExecute(con, "DELETE FROM pk_ons WHERE dod < primary_op_date;")



########################################## Merge NJR to HES using a range join ##########################################

# Change nn_nid to string, to match nn_nid in HES tables
dbExecute(con, "ALTER TABLE pk_ons ALTER nn_nid TYPE VARCHAR;")

# Create fields 'start' and 'end' to provides a tolerance of +/- 1 week for an episode to merge
dbExecute(con, "ALTER TABLE pk_ons ADD COLUMN start_date DATE;")
dbExecute(con, "UPDATE pk_ons SET start_date = primary_op_date - 7;")

dbExecute(con, "ALTER TABLE pk_ons ADD COLUMN end_date DATE;")
dbExecute(con, "UPDATE pk_ons SET end_date = primary_op_date + 7;")

# Merge to HES

dbExecute(con, "CREATE TABLE temp AS
                  SELECT pk_ons.*, hes_working.*
                  FROM pk_ons
                  LEFT OUTER JOIN hes_working
                  ON hes_working.study_id = pk_ons.nn_nid
                  AND hes_working.admidate_filled >= pk_ons.start_date
                  AND hes_working.admidate_filled <= pk_ons.end_date;")


# We have allowed multiplication with the first join, and will sort it out with a second query

# Create a date diff field
dbExecute(con, "ALTER TABLE temp ADD COLUMN date_diff INTEGER;")
dbExecute(con, "UPDATE temp SET date_diff = ABS(primary_op_date - admidate_filled);")

# Look at date diff
dbGetQuery(con, "SELECT date_diff, COUNT(*)
           FROM temp
           GROUP BY date_diff")

# Identify the first match
dbExecute(con, "CREATE TABLE pk_ons_hes AS
                WITH cte AS
                (
                   SELECT *,
                         ROW_NUMBER() OVER (PARTITION BY nn_nid ORDER BY date_diff ASC) AS row_num
                   FROM temp
                )
                SELECT *
                FROM cte
                WHERE row_num = 1;")

# Remove the row_number field
dbExecute(con, "ALTER TABLE pk_ons_hes DROP row_num")

# Get rid of intermediary table
dbExecute(con, "DROP TABLE temp")

dbListTables(con)

# JOIN PROMs
dbExecute(con, "ALTER TABLE proms ALTER epikey TYPE BIGINT;")

dbExecute(con, "CREATE TABLE temp3 AS
          WITH cte AS 
            (
            SELECT *,
            ROW_NUMBER() OVER(PARTITION BY epikey ORDER BY episode_match_rank ASC) AS rn
            FROM proms
            WHERE EXISTS (
              SELECT epikey, primary_op_date
              FROM pk_ons_hes
              WHERE pk_ons_hes.epikey = proms.epikey
                AND pk_ons_hes.primary_op_date - proms.q1_completed_date <=90
                AND pk_ons_hes.primary_op_date - proms.q1_completed_date >=0
                AND proms.q2_completed_date - pk_ons_hes.primary_op_date >=180
                AND proms.q2_completed_date - pk_ons_hes.primary_op_date <=365
                AND proms.q1_completed_date IS NOT NULL
                AND proms.q2_completed_date IS NOT NULL
                )
            )

          SELECT pk_ons_hes.*, cte.*
          FROM pk_ons_hes
          LEFT OUTER JOIN cte
          ON cte.epikey = pk_ons_hes.epikey
            AND cte.rn = 1;")



########################################## Derived fields ##########################################

df <- dbGetQuery(con, "SELECT * FROM temp3")

# 5 year Charlson lookback
cci <- df %>% select(primary_njr_index_no, primary_procedure_id, cm5y1, cm5y2, cm5y3, cm5y4, cm5y5, cm5y6, cm5y7, cm5y8, cm5y9, cm5y10, cm5y11, cm5y12, cm5y13, cm5y14, cm5y15, cm5y16, cm5y17)

# Convert logical to numeric
cci <- cci *1
cci <- cci %>% mutate(cm5y18 = case_when(cm5y11 >0 & cm5y15 >0 ~ 0,
                                         TRUE ~ cm5y11))

# Calculate index
cci <- cci %>% mutate(cci = (5*cm5y1) + (11*cm5y2) + (13*cm5y3) + (4*cm5y4) + (14*cm5y5) + (3*cm5y6) + (8*cm5y7) + (9*cm5y8) + (6*cm5y9) + (4*cm5y10) + (8*cm5y18) + (-1*cm5y12) + (1*cm5y13) + (10*cm5y14) + (14*cm5y15) + (18*cm5y16) + (2*cm5y17))

# Set lowest index as 0
cci <- cci %>% mutate(cci = case_when(cci== -1 ~0,
                                      TRUE ~ cci))

# Merge back
cci <- cci %>% select(primary_njr_index_no, primary_procedure_id, cci)
df <- merge(df, cci, by = c("primary_njr_index_no", "primary_procedure_id"))

# Also create a Charlson comorbidity index that is binary
## Coded as 0 or 1+ or Missing
df <-
  df %>% 
  mutate(charl01 = case_when(cci == 0 ~ "0",
                             cci >0 ~ "1+",
                             TRUE ~ "Not specified"))


# Gender
## Use NJR field
df$gender <- recode(df$patient_gender, `M` = "Male", `F` = "Female", `x` = "Not specified")
df <- df %>% select(-patient_gender)


# ASA
## There is no missing data
## df %>% janitor::tabyl(primary_asa)
## Coded as ASA 1, ASA 2 or ASA 3+
df <-
  df %>% 
  mutate(asa_grade = case_when(primary_asa == "P1 - Fit and healthy" ~ "ASA 1",
                               primary_asa == "P2 - Mild disease not incapacitating" ~ "ASA 2",
                               TRUE ~ "ASA 3+"))


# IMD decile
## There is missing data (from NJR episodes that did not link to HES)
## Some records are coded using UPPER CASE and do not match lower case entries
## Coded into deciles

# Create function to capitalise first letter only
firstup <- function(x) {
  x <- tolower(x)
  substr(x, 1, 1) <- toupper(substr(x, 1, 1))
  x
}

df$imd5 <- firstup(df$IMD04_DECILE)
rm(firstup)

df <-
  df %>% mutate(imd5 = case_when(
    imd5 == "" | is.na(imd5) ~ "Not specified",
    imd5 == "Least deprived 10%" | imd5 == "Less deprived 10-20%" ~ "Least deprived 20%",
    imd5 == "Less deprived 20-30%" | imd5 == "Less deprived 30-40%" ~ "Less deprived 20-40%",
    imd5 == "Less deprived 40-50%" | imd5 == "More deprived 10-20%" ~ "Middle group",
    imd5 == "More deprived 20-30%" | imd5 == "More deprived 30-40%" ~ "More deprived 20-40%",
    imd5 == "More deprived 40-50%" | imd5 == "Most deprived 10%" ~ "Most deprived 20%"))

# Create an ordered factor
df$imd5 <- 
  factor(df$imd5, levels=c("Most deprived 20%", "More deprived 20-40%", "Middle group", "Less deprived 20-40%", "Least deprived 20%", "Not specified"), ordered=TRUE) 


# Ethnicity
df <-
  df %>% mutate(ethnic5 = case_when(
    ETHNIC5 == "Unknown" | is.na(ETHNIC5) ~ "Not specified",
    TRUE ~ ETHNIC5))

df$ethnic5 <- 
  factor(df$ethnic5, levels=c("Asian/Asian British", "Black/Black British", "Chinese/Other", "Mixed", "White", "Not specified"), ordered=TRUE) 

df <- df %>% select(-ETHNIC5)


# BMI
# Set BMI <13 and >80 as implausible
df$primary_bmi <- as.numeric(as.character(df$primary_bmi))

df <-
  df %>% 
  mutate(bmi_plaus = case_when(primary_bmi <13 ~ NA_real_,
                               primary_bmi >80 ~ NA_real_,
                               TRUE ~ primary_bmi))


# There are two epikey fields, so it will not write to DuckDB
df %>% filter(!is.na(`epikey:1`)) %>% count()
df %>% filter(!is.na(EPIKEY)) %>% count()

# Keep the one with more information, which is the uppercase one
df <- df %>% select(-`epikey:1`)

## Convert epikey to character before writing table
df$EPIKEY <- as.character(df$EPIKEY)



########################################## Create DuckDB tbl ##########################################

## Write Duck DB table
dbWriteTable(con, "pk_ons_hes_proms", df)

## Remove temporary table
dbExecute(con, "DROP TABLE temp3")
rm(df, cci)



########################################## Summary of attrition ##########################################

# Number of pk supplied by NJR
dbGetQuery(con, "SELECT count(*)
           FROM pk;")

# Number of first pk for a given patient
dbGetQuery(con, "SELECT count(*)
           FROM pk_first;")

# Number of death records with a study_id from ONS
dbGetQuery(con, "SELECT count(*)
           FROM ons;")

# Number of patients where primary_op_date < dod (or not dead)
dbGetQuery(con, "SELECT count(*)
           FROM pk_ons;")

# Number of linked to HES
dbGetQuery(con, "SELECT count(*)
           FROM pk_ons_hes_proms
           WHERE epikey IS NOT NULL;")

# Number with a returned PROM within an appropriate date
dbGetQuery(con, "SELECT count(*)
           FROM pk_ons_hes_proms
           WHERE proms_serial_no IS NOT NULL;")

# Number with a returned PROM within an appropriate date where OKS and success transition complete
dbGetQuery(con, "SELECT count(*)
           FROM pk_ons_hes_proms
           WHERE proms_serial_no IS NOT NULL
            AND cca = 1;")

# Number TKR/UKR with a returned PROM within an appropriate date
dbGetQuery(con, "SELECT primary_patient_procedure, count(*)
           FROM pk_ons_hes_proms
           WHERE proms_serial_no IS NOT NULL
           GROUP BY primary_patient_procedure;")



########################################## Shutdown DuckDB con ##########################################

dbListTables(con)

# Shutdown database
dbDisconnect(con, shutdown=TRUE)




########################################## FOOTNOTES ##########################################

# #1
# ## To join the NJR to HES on exact date, rather than a range...
# dbExecute(con, "CREATE TABLE pk_hes_temp AS
#                   SELECT pk_ons.*, hes1004.*
#                   FROM pk_ons
#                   LEFT OUTER JOIN hes1004
#                   ON pk_ons.nn_nid = hes1004.study_id
#                   AND pk_ons.primary_op_date = hes1004.admidate_filled;")
#
# #2
# ## For a more granular join to PROMs, where inappropriate dates are spelled out...
# df <- dbGetQuery(con, "SELECT * FROM pk_ons_hes")
# 
# # Merge on PROMs in R environment using epikey
# proms <- dbGetQuery(con, "SELECT * FROM proms")
# 
# # Check data types
# str(proms$epikey)
# str(df$EPIKEY)
# 
# # Convert
# proms$epikey <- bit64::as.integer64(proms$epikey)
# df$epikey <- bit64::as.integer64(df$EPIKEY)
# 
# # Check
# str(proms$epikey)
# str(df$epikey)
# 
# # Remove PROM records with no epikey
# proms <- proms %>% filter(!is.na(epikey))
# 
# setDT(df)
# setDT(proms)
# setNumericRounding(0)  # From data.table
# 
# # NOTE
# # data.table left join
# # The 'left' table is in the []!
# # Could avoid duplicates by using a rolling join and accepting first
# pkonshesproms <- proms[df, on="epikey"]
# 
# # This created duplicates, which again need to be removed
# pkonshesproms <- as.data.frame(pkonshesproms)
# 
# eeptools::isid(pkonshesproms, c("primary_njr_index_no", "primary_procedure_id"), verbose = FALSE)
# 
# pkonshesproms <-
#   pkonshesproms %>% 
#   group_by(primary_njr_index_no, primary_procedure_id) %>% 
#   mutate(rn = row_number()) %>% 
#   ungroup() %>% 
#   filter(rn ==1) %>% 
#   select(-rn)
# 
# eeptools::isid(pkonshesproms, c("primary_njr_index_no", "primary_procedure_id"), verbose = FALSE)
# 
# # Remove duplicate fields
# pkonshesproms <- pkonshesproms %>% select(-c(FYEAR, EPIKEY))
# 
# # Create wrong dates fields
# pkonshesproms <-
#   as.data.frame(pkonshesproms %>% mutate(wrongdate = case_when(
#     as.Date(primary_op_date) - as.Date(q1_completed_date) >90 ~ 1, # Q1 too early (before 90 days)
#     as.Date(primary_op_date) - as.Date(q1_completed_date) <0 ~ 2, # Q1 too late (after the surgery)
#     is.na(q1_completed_date) ~ 3, # Q1 date missing
#     as.Date(q2_completed_date) - as.Date(primary_op_date) <180 ~ 4, # Q2 too early (within 90 days)
#     as.Date(q2_completed_date) - as.Date(primary_op_date) >365.25 ~ 5, # Q2 too late (after 1 year)
#     is.na(q2_completed_date) ~ 6, # Q2 date missing
#     TRUE ~ 0)))
# 
# pkonshesproms %>% janitor::tabyl(wrongdate)
# 
# dbWriteTable(con, "pk_ons_hes_proms", pkonshesproms)
# 
# # #3 Summary of merges
# setDT(pkonshesproms)
# 
# # Count number of pk
# pkonshesproms[!is.na(nn_nid), .N]
# 
# # Count number of merged to HES APC
# pkonshesproms[epikey!=0, .N]
# 
# # Count number of PROMs (i.e. number of prom_serial_no)
# pkonshesproms[!is.na(proms_serial_no), .N]
# 
# # PROMs, administered with appropriate dates
# pkonshesproms[wrongdate==0, .N]
# 
# # How many !is.na(prom_serial_no) if is.na(epikey) -- i.e. bad linkages
# pkonshesproms[is.na(epikey) & !is.na(proms_serial_no), .N]
# pkonshesproms[is.na(fyear) & !is.na(proms_serial_no), .N]
# 
# # Can also check that proms_serial_no is unique
# as.data.frame(pkonshesproms) %>% filter(!is.na(proms_serial_no)) %>% eeptools::isid("proms_serial_no")
# 
# # Count number of complete Q1s
# pkonshesproms[q1_complete==1, .N]
# 
# # Count number of complete Q2s
# pkonshesproms[q2_complete==1, .N]
# 
# # Count number of complete PROMs
# pkonshesproms[cca==1, .N]
# 
# # How many TKR v UKR with PROMs?
# pkonshesproms[wrongdate==0 & cca==1, .N, primary_patient_procedure]

