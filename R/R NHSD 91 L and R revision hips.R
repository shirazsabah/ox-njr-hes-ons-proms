
pacman::p_load(pacman, data.table, DBI, duckdb, tidyverse, tictoc)

## Connect to database
con = dbConnect(duckdb::duckdb(), dbdir=paste0(data_dir,"SQL/ss.duckdb"), read_only=FALSE)



########################################## Right-sided revision hips ##########################################

dbExecute(con, "CREATE TABLE rh_right AS
          WITH cte AS
          (
            SELECT revision_njr_index_no, revision_procedure_id, nn_nid, side, revision_age, revision_bmi, revision_date, gender,
            primary_njr_index_no, primary_procedure_id,
            ind_rev_aseptic_loosening_stem, 
            ind_rev_aseptic_loosening_socket, ind_rev_mds1aseptic_loosening, ind_rev_dislocation_subluxation, ind_rev_implant_fracture_stem, 
            ind_rev_implant_fracture_socket, ind_rev_implant_fracture_head, ind_rev_incorrect_sizing_socket, ind_rev_incorrect_sizing_head, 
            ind_rev_incorrect_sizing, ind_rev_mds1incorrect_sizing, ind_rev_infection, ind_rev_lysis_stem, ind_rev_lysis_socket,
            ind_rev_mds1lysis, ind_rev_malalignment_stem, ind_rev_malalignment_socket, ind_rev_malalignment, ind_rev_pain, ind_rev_periprosthetic_fracture_stem,
            ind_rev_periprosthetic_fracture_socket, ind_rev_peri_prosthetic_fracture, ind_rev_wear_of_acetabular_component, ind_rev_wear_of_polyethylene_component,
            ind_rev_dissociation_of_liner, ind_rev_adverse_soft_tissue_reaction_to_particle_debris, ind_rev_other, ind_rev_summary_revision_reasons,
            ROW_NUMBER() OVER (PARTITION BY nn_nid ORDER BY revision_date ASC) AS rn
            FROM ph_combined
            WHERE side = 'R'
          )
          SELECT *
            FROM cte
          WHERE rn = 1 
            AND revision_njr_index_no !='NA';")



########################################## Merge to ONS ##########################################

dbExecute(con, "CREATE TABLE rh_right_ons AS
                SELECT rh_right.*, ons.*
                FROM rh_right
                LEFT OUTER JOIN ons
                ON ons.nn_nid = rh_right.nn_nid;")

# Exclude records where DOD < DOR
dbExecute(con, "DELETE FROM rh_right_ons WHERE dod < revision_date;")



########################################## Merge NJR to HES using a range join ##########################################

# Change nn_nid to string, to match nn_nid in HES tables
dbExecute(con, "ALTER TABLE rh_right_ons ALTER nn_nid TYPE VARCHAR;")

# Create fields 'start' and 'end' to provides a tolerance of +/- 1 week for an episode to merge
dbExecute(con, "ALTER TABLE rh_right_ons ADD COLUMN start_date DATE;")
dbExecute(con, "UPDATE rh_right_ons SET start_date = revision_date - 7;")

dbExecute(con, "ALTER TABLE rh_right_ons ADD COLUMN end_date DATE;")
dbExecute(con, "UPDATE rh_right_ons SET end_date = revision_date + 7;")


# Merge to HES

# Merge to first rHR
dbExecute(con, "CREATE TABLE temp AS
                  SELECT rh_right_ons.*, hes_working.*
                  FROM rh_right_ons
                  LEFT OUTER JOIN hes_working
                  ON hes_working.study_id = rh_right_ons.nn_nid
                  AND hes_working.admidate_filled >= rh_right_ons.start_date
                  AND hes_working.admidate_filled <= rh_right_ons.end_date;")


# Create a date diff field
dbExecute(con, "ALTER TABLE temp ADD COLUMN date_diff INTEGER;")
dbExecute(con, "UPDATE temp SET date_diff = ABS(revision_date - admidate_filled);")

# Look at date diff
dbGetQuery(con, "SELECT date_diff, COUNT(*)
           FROM temp
           GROUP BY date_diff")

# Identify the first match
dbExecute(con, "CREATE TABLE rh_right_hes AS
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
dbExecute(con, "ALTER TABLE rh_right_hes DROP row_num")

# Get rid of intermediary table
dbExecute(con, "DROP TABLE temp")
dbListTables(con)

# JOIN PROMs
dbExecute(con, "ALTER TABLE proms ALTER epikey TYPE BIGINT;")

dbExecute(con, "CREATE TABLE temp3 AS
          WITH cte AS (
            SELECT *,
            ROW_NUMBER() OVER(PARTITION BY epikey ORDER BY episode_match_rank ASC) AS rn
            FROM proms
            WHERE EXISTS (
              SELECT epikey, revision_date
              FROM rh_right_hes
              WHERE rh_right_hes.epikey = proms.epikey
                AND rh_right_hes.revision_date - proms.q1_completed_date <=90
                AND rh_right_hes.revision_date - proms.q1_completed_date >=0
                AND proms.q2_completed_date - rh_right_hes.revision_date >=180
                AND proms.q2_completed_date - rh_right_hes.revision_date <=365
                AND proms.q1_completed_date IS NOT NULL
                AND proms.q2_completed_date IS NOT NULL
                )
          )

          SELECT rh_right_hes.*, cte.*
          FROM rh_right_hes
          LEFT OUTER JOIN cte
          ON cte.epikey = rh_right_hes.epikey
            AND cte.rn = 1;")



########################################## Derived fields ##########################################

df <- dbGetQuery(con, "SELECT * FROM temp3")

# 5 year Charlson lookback
cci <- df %>% select(revision_njr_index_no, revision_procedure_id, cm5y1, cm5y2, cm5y3, cm5y4, cm5y5, cm5y6, cm5y7, cm5y8, cm5y9, cm5y10, cm5y11, cm5y12, cm5y13, cm5y14, cm5y15, cm5y16, cm5y17)

cci <- cci %>% mutate(across(everything(), as.numeric))
cci <- cci %>% mutate(cm5y18 = case_when(cm5y11 >0 & cm5y15 >0 ~ 0,
                                         TRUE ~ cm5y11))

# Calculate index
cci <- cci %>% mutate(cci = (5*cm5y1) + (11*cm5y2) + (13*cm5y3) + (4*cm5y4) + (14*cm5y5) + (3*cm5y6) + (8*cm5y7) + (9*cm5y8) + (6*cm5y9) + (4*cm5y10) + (8*cm5y18) + (-1*cm5y12) + (1*cm5y13) + (10*cm5y14) + (14*cm5y15) + (18*cm5y16) + (2*cm5y17))

# Set lowest index as 0
cci <- cci %>% mutate(cci = case_when(cci== -1 ~0,
                                      TRUE ~ cci))

# Merge back
cci <- cci %>% select(revision_njr_index_no, revision_procedure_id, cci)
# eeptools::isid(cci, c("revision_njr_index_no", "revision_procedure_id"))
# eeptools::isid(df, c("revision_njr_index_no", "revision_procedure_id"))

df <- merge(df, cci, by = c("revision_njr_index_no", "revision_procedure_id"))

# Also create a Charlson comorbidity index that is binary
## Coded as 0 or 1+ or Missing
df <-
  df %>% 
  mutate(charl01 = case_when(cci == 0 ~ "0",
                             cci >0 ~ "1+",
                             TRUE ~ "Not specified"))


# Gender
## Use NJR field
## df %>% janitor::tabyl(gender)


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

df$revision_bmi <- as.numeric(df$revision_bmi)
df <-
  df %>% 
  mutate(bmi_plaus = case_when(revision_bmi <13 ~ NA_real_,
                               revision_bmi >80 ~ NA_real_,
                               TRUE ~ revision_bmi))


## Convert epikey to character before writing table
df$EPIKEY <- as.character(df$EPIKEY)



########################################## Create DuckDB tbl ##########################################

## Write Duck DB table
dbWriteTable(con, "rh_right_hes_proms", df, overwrite=TRUE)

## Remove temporary tables
dbExecute(con, "DROP TABLE rh_right")
dbExecute(con, "DROP TABLE rh_right_ons")
dbExecute(con, "DROP TABLE rh_right_hes")
dbExecute(con, "DROP TABLE temp3")
rm(df, cci)





########################################## Left-sided revision hips ##########################################

dbExecute(con, "CREATE TABLE rh_left AS
          WITH cte AS
          (
            SELECT revision_njr_index_no, revision_procedure_id, nn_nid, side, revision_age, revision_bmi, revision_date, gender,
            primary_njr_index_no, primary_procedure_id,
            ind_rev_aseptic_loosening_stem, 
            ind_rev_aseptic_loosening_socket, ind_rev_mds1aseptic_loosening, ind_rev_dislocation_subluxation, ind_rev_implant_fracture_stem, 
            ind_rev_implant_fracture_socket, ind_rev_implant_fracture_head, ind_rev_incorrect_sizing_socket, ind_rev_incorrect_sizing_head, 
            ind_rev_incorrect_sizing, ind_rev_mds1incorrect_sizing, ind_rev_infection, ind_rev_lysis_stem, ind_rev_lysis_socket,
            ind_rev_mds1lysis, ind_rev_malalignment_stem, ind_rev_malalignment_socket, ind_rev_malalignment, ind_rev_pain, ind_rev_periprosthetic_fracture_stem,
            ind_rev_periprosthetic_fracture_socket, ind_rev_peri_prosthetic_fracture, ind_rev_wear_of_acetabular_component, ind_rev_wear_of_polyethylene_component,
            ind_rev_dissociation_of_liner, ind_rev_adverse_soft_tissue_reaction_to_particle_debris, ind_rev_other, ind_rev_summary_revision_reasons,
            ROW_NUMBER() OVER (PARTITION BY nn_nid ORDER BY revision_date ASC) AS rn
            FROM ph_combined
            WHERE side = 'L'
          )
          SELECT *
            FROM cte
          WHERE rn = 1 
            AND revision_njr_index_no !='NA';")


########################################## Merge to ONS ##########################################

dbExecute(con, "CREATE TABLE rh_left_ons AS
                SELECT rh_left.*, ons.*
                FROM rh_left
                LEFT OUTER JOIN ons
                ON ons.nn_nid = rh_left.nn_nid;")

# Exclude records where DOD < DOR
dbExecute(con, "DELETE FROM rh_left_ons WHERE dod < revision_date;")



########################################## Merge NJR to HES using a range join ##########################################

# Change nn_nid to string, to match nn_nid in HES tables
dbExecute(con, "ALTER TABLE rh_left_ons ALTER nn_nid TYPE VARCHAR;")

# Create fields 'start' and 'end' to provides a tolerance of +/- 1 week for an episode to merge
dbExecute(con, "ALTER TABLE rh_left_ons ADD COLUMN start_date DATE;")
dbExecute(con, "UPDATE rh_left_ons SET start_date = revision_date - 7;")

dbExecute(con, "ALTER TABLE rh_left_ons ADD COLUMN end_date DATE;")
dbExecute(con, "UPDATE rh_left_ons SET end_date = revision_date + 7;")


# Merge to HES

# Merge to first rHR
dbExecute(con, "CREATE TABLE temp AS
                  SELECT rh_left_ons.*, hes_working.*
                  FROM rh_left_ons
                  LEFT OUTER JOIN hes_working
                  ON hes_working.study_id = rh_left_ons.nn_nid
                  AND hes_working.admidate_filled >= rh_left_ons.start_date
                  AND hes_working.admidate_filled <= rh_left_ons.end_date;")


# Create a date diff field
dbExecute(con, "ALTER TABLE temp ADD COLUMN date_diff INTEGER;")
dbExecute(con, "UPDATE temp SET date_diff = ABS(revision_date - admidate_filled);")

# Look at date diff
dbGetQuery(con, "SELECT date_diff, COUNT(*)
           FROM temp
           GROUP BY date_diff")

# Identify the first match
dbExecute(con, "CREATE TABLE rh_left_hes AS
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
dbExecute(con, "ALTER TABLE rh_left_hes DROP row_num")

# Get rid of intermediary table
dbExecute(con, "DROP TABLE temp")
dbListTables(con)

# JOIN PROMs
dbExecute(con, "ALTER TABLE proms ALTER epikey TYPE BIGINT;")

dbExecute(con, "CREATE TABLE temp3 AS
          WITH cte AS (
            SELECT *,
            ROW_NUMBER() OVER(PARTITION BY epikey ORDER BY episode_match_rank ASC) AS rn
            FROM proms
            WHERE EXISTS (
              SELECT epikey, revision_date
              FROM rh_left_hes
              WHERE rh_left_hes.epikey = proms.epikey
                AND rh_left_hes.revision_date - proms.q1_completed_date <=90
                AND rh_left_hes.revision_date - proms.q1_completed_date >=0
                AND proms.q2_completed_date - rh_left_hes.revision_date >=180
                AND proms.q2_completed_date - rh_left_hes.revision_date <=365
                AND proms.q1_completed_date IS NOT NULL
                AND proms.q2_completed_date IS NOT NULL
                )
          )

          SELECT rh_left_hes.*, cte.*
          FROM rh_left_hes
          LEFT OUTER JOIN cte
          ON cte.epikey = rh_left_hes.epikey
            AND cte.rn = 1;")



########################################## Derived fields ##########################################

df <- dbGetQuery(con, "SELECT * FROM temp3")

# 5 year Charlson lookback
cci <- df %>% select(revision_njr_index_no, revision_procedure_id, cm5y1, cm5y2, cm5y3, cm5y4, cm5y5, cm5y6, cm5y7, cm5y8, cm5y9, cm5y10, cm5y11, cm5y12, cm5y13, cm5y14, cm5y15, cm5y16, cm5y17)

cci <- cci %>% mutate(across(everything(), as.numeric))
cci <- cci %>% mutate(cm5y18 = case_when(cm5y11 >0 & cm5y15 >0 ~ 0,
                                         TRUE ~ cm5y11))

# Calculate index
cci <- cci %>% mutate(cci = (5*cm5y1) + (11*cm5y2) + (13*cm5y3) + (4*cm5y4) + (14*cm5y5) + (3*cm5y6) + (8*cm5y7) + (9*cm5y8) + (6*cm5y9) + (4*cm5y10) + (8*cm5y18) + (-1*cm5y12) + (1*cm5y13) + (10*cm5y14) + (14*cm5y15) + (18*cm5y16) + (2*cm5y17))

# Set lowest index as 0
cci <- cci %>% mutate(cci = case_when(cci== -1 ~0,
                                      TRUE ~ cci))

# Merge back
cci <- cci %>% select(revision_njr_index_no, revision_procedure_id, cci)
df <- merge(df, cci, by = c("revision_njr_index_no", "revision_procedure_id"))

# Also create a Charlson comorbidity index that is binary
## Coded as 0 or 1+ or Missing
df <-
  df %>% 
  mutate(charl01 = case_when(cci == 0 ~ "0",
                             cci >0 ~ "1+",
                             TRUE ~ "Not specified"))


# Gender
## Use NJR field
## df %>% janitor::tabyl(gender)


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

df$revision_bmi <- as.numeric(df$revision_bmi)
df <-
  df %>% 
  mutate(bmi_plaus = case_when(revision_bmi <13 ~ NA_real_,
                               revision_bmi >80 ~ NA_real_,
                               TRUE ~ revision_bmi))


## Convert epikey to character before writing table
df$EPIKEY <- as.character(df$EPIKEY)



########################################## Create DuckDB tbl ##########################################

## Write Duck DB table
dbWriteTable(con, "rh_left_hes_proms", df, overwrite=TRUE)


## Remove temporary tables
dbExecute(con, "DROP TABLE rh_left")
dbExecute(con, "DROP TABLE rh_left_ons")
dbExecute(con, "DROP TABLE rh_left_hes")
dbExecute(con, "DROP TABLE temp3")
rm(df, cci)

#####

## The right and left datasets then need to be appended together

dbExecute(con, "CREATE TABLE rh_combined AS
          SELECT *
  FROM rh_left_hes_proms

 UNION

 SELECT *
   FROM rh_right_hes_proms
          ")


## Remove intermediary tables
dbExecute(con, "DROP TABLE rh_right_hes_proms")
dbExecute(con, "DROP TABLE rh_left_hes_proms")

dbListTables(con)



# ########################################## Shutdown DuckDB con ##########################################
# 
# # Shutdown database
# dbDisconnect(con, shutdown=TRUE)
