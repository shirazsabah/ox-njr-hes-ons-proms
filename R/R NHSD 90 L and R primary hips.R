pacman::p_load(pacman, data.table, DBI, duckdb, tidyverse, tictoc)

## Connect to database
con = dbConnect(duckdb::duckdb(), dbdir=paste0(data_dir,"SQL/ss.duckdb"), read_only=FALSE)

########################################## Right- and left-sided primary hips ##########################################

dbExecute(con, "CREATE TABLE ph_right AS
          WITH cte AS
          (
            SELECT *,
            ROW_NUMBER() OVER (PARTITION BY nn_nid ORDER BY primary_op_date ASC) AS rn
            FROM ph
            WHERE side = 'R'
          )
          SELECT *
            FROM cte
          WHERE rn = 1;")

dbExecute(con, "CREATE TABLE ph_left AS
          WITH cte AS
          (
            SELECT *,
            ROW_NUMBER() OVER (PARTITION BY nn_nid ORDER BY primary_op_date ASC) AS rn
            FROM ph
            WHERE side = 'L'
          )
          SELECT *
            FROM cte
          WHERE rn = 1;")


########################################## Merge NJR to other datasets ##########################################

# Merge right
dbExecute(con, "CREATE TABLE ph_right_ons AS
                SELECT ph_right.*, ons.*
                FROM ph_right
                LEFT OUTER JOIN ons
                ON ons.nn_nid = ph_right.nn_nid;")

# Exclude records where DOD < DOR
dbExecute(con, "DELETE FROM ph_right_ons WHERE dod < primary_op_date;")


# Merge left
dbExecute(con, "CREATE TABLE ph_left_ons AS
                SELECT ph_left.*, ons.*
                FROM ph_left
                LEFT OUTER JOIN ons
                ON ons.nn_nid = ph_left.nn_nid;")

# Exclude records where DOD < DOR
dbExecute(con, "DELETE FROM ph_left_ons WHERE dod < primary_op_date;")



########################################## Merge NJR to HES using a range join ##########################################

# Change nn_nid to string, to match nn_nid in HES tables
dbExecute(con, "ALTER TABLE ph_right_ons ALTER nn_nid TYPE VARCHAR;")
dbExecute(con, "ALTER TABLE ph_left_ons ALTER nn_nid TYPE VARCHAR;")

# Create fields 'start' and 'end' to provides a tolerance of +/- 1 week for an episode to merge
dbExecute(con, "ALTER TABLE ph_right_ons ADD COLUMN start_date DATE;")
dbExecute(con, "UPDATE ph_right_ons SET start_date = primary_op_date - 7;")
dbExecute(con, "ALTER TABLE ph_right_ons ADD COLUMN end_date DATE;")
dbExecute(con, "UPDATE ph_right_ons SET end_date = primary_op_date + 7;")

dbExecute(con, "ALTER TABLE ph_left_ons ADD COLUMN start_date DATE;")
dbExecute(con, "UPDATE ph_left_ons SET start_date = primary_op_date - 7;")
dbExecute(con, "ALTER TABLE ph_left_ons ADD COLUMN end_date DATE;")
dbExecute(con, "UPDATE ph_left_ons SET end_date = primary_op_date + 7;")

# Merge to HES

dbExecute(con, "CREATE TABLE temp_right AS
                  SELECT ph_right_ons.*, hes_working.*
                  FROM ph_right_ons
                  LEFT OUTER JOIN hes_working
                  ON hes_working.study_id = ph_right_ons.nn_nid
                  AND hes_working.admidate_filled >= ph_right_ons.start_date
                  AND hes_working.admidate_filled <= ph_right_ons.end_date;")

dbExecute(con, "CREATE TABLE temp_left AS
                  SELECT ph_left_ons.*, hes_working.*
                  FROM ph_left_ons
                  LEFT OUTER JOIN hes_working
                  ON hes_working.study_id = ph_left_ons.nn_nid
                  AND hes_working.admidate_filled >= ph_left_ons.start_date
                  AND hes_working.admidate_filled <= ph_left_ons.end_date;")


# We have allowed multiplication with the first join, and will sort it out with a second query

# Create a date diff field
dbExecute(con, "ALTER TABLE temp_right ADD COLUMN date_diff INTEGER;")
dbExecute(con, "UPDATE temp_right SET date_diff = ABS(primary_op_date - admidate_filled);")

dbExecute(con, "ALTER TABLE temp_left ADD COLUMN date_diff INTEGER;")
dbExecute(con, "UPDATE temp_left SET date_diff = ABS(primary_op_date - admidate_filled);")

# Identify the first match - right
dbExecute(con, "CREATE TABLE ph_right_ons_hes AS
                WITH cte AS
                (
                   SELECT *,
                         ROW_NUMBER() OVER (PARTITION BY nn_nid ORDER BY date_diff ASC) AS row_num
                   FROM temp_right
                )
                SELECT *
                FROM cte
                WHERE row_num = 1;")

# Remove the row_number field
dbExecute(con, "ALTER TABLE ph_right_ons_hes DROP row_num")

# Get rid of intermediary table
dbExecute(con, "DROP TABLE temp_right")


# Identify the first match - left
dbExecute(con, "CREATE TABLE ph_left_ons_hes AS
                WITH cte AS
                (
                   SELECT *,
                         ROW_NUMBER() OVER (PARTITION BY nn_nid ORDER BY date_diff ASC) AS row_num
                   FROM temp_left
                )
                SELECT *
                FROM cte
                WHERE row_num = 1;")

# Remove the row_number field
dbExecute(con, "ALTER TABLE ph_left_ons_hes DROP row_num")

# Get rid of intermediary table
dbExecute(con, "DROP TABLE temp_left")


# JOIN PROMs
dbExecute(con, "ALTER TABLE proms ALTER epikey TYPE BIGINT;")

dbExecute(con, "CREATE TABLE temp3_right AS
          WITH cte AS 
            (
            SELECT *,
            ROW_NUMBER() OVER(PARTITION BY epikey ORDER BY episode_match_rank ASC) AS rn
            FROM proms
            WHERE EXISTS (
              SELECT epikey, primary_op_date
              FROM ph_right_ons_hes
              WHERE ph_right_ons_hes.epikey = proms.epikey
                AND ph_right_ons_hes.primary_op_date - proms.q1_completed_date <=90
                AND ph_right_ons_hes.primary_op_date - proms.q1_completed_date >=0
                AND proms.q2_completed_date - ph_right_ons_hes.primary_op_date >=180
                AND proms.q2_completed_date - ph_right_ons_hes.primary_op_date <=365
                AND proms.q1_completed_date IS NOT NULL
                AND proms.q2_completed_date IS NOT NULL
                )
            )

          SELECT ph_right_ons_hes.*, cte.*
          FROM ph_right_ons_hes
          LEFT OUTER JOIN cte
          ON cte.epikey = ph_right_ons_hes.epikey
            AND cte.rn = 1;")


# JOIN PROMs
dbExecute(con, "ALTER TABLE proms ALTER epikey TYPE BIGINT;")

dbExecute(con, "CREATE TABLE temp3_left AS
          WITH cte AS 
            (
            SELECT *,
            ROW_NUMBER() OVER(PARTITION BY epikey ORDER BY episode_match_rank ASC) AS rn
            FROM proms
            WHERE EXISTS (
              SELECT epikey, primary_op_date
              FROM ph_left_ons_hes
              WHERE ph_left_ons_hes.epikey = proms.epikey
                AND ph_left_ons_hes.primary_op_date - proms.q1_completed_date <=90
                AND ph_left_ons_hes.primary_op_date - proms.q1_completed_date >=0
                AND proms.q2_completed_date - ph_left_ons_hes.primary_op_date >=180
                AND proms.q2_completed_date - ph_left_ons_hes.primary_op_date <=365
                AND proms.q1_completed_date IS NOT NULL
                AND proms.q2_completed_date IS NOT NULL
                )
            )

          SELECT ph_left_ons_hes.*, cte.*
          FROM ph_left_ons_hes
          LEFT OUTER JOIN cte
          ON cte.epikey = ph_left_ons_hes.epikey
            AND cte.rn = 1;")



########################################## Derived fields right ##########################################

df <- dbGetQuery(con, "SELECT * FROM temp3_right")

# 5 year Charlson lookback
cci <- df %>% select(primary_njr_index_no, primary_procedure_id, cm5y1, cm5y2, cm5y3, cm5y4, cm5y5, cm5y6, cm5y7, cm5y8, cm5y9, cm5y10, cm5y11, cm5y12, cm5y13, cm5y14, cm5y15, cm5y16, cm5y17)

# Convert logical to numeric
cci <- cci %>% mutate(across(everything(), as.numeric))
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


## Convert epikey to character before writing table
df$EPIKEY <- as.character(df$EPIKEY)

## Write Duck DB table
dbWriteTable(con, "ph_right_ons_hes_proms", df)

## Remove temporary table
dbExecute(con, "DROP TABLE temp3_right")
rm(df, cci)


########################################## Derived fields left ##########################################

df <- dbGetQuery(con, "SELECT * FROM temp3_left")

# 5 year Charlson lookback
cci <- df %>% select(primary_njr_index_no, primary_procedure_id, cm5y1, cm5y2, cm5y3, cm5y4, cm5y5, cm5y6, cm5y7, cm5y8, cm5y9, cm5y10, cm5y11, cm5y12, cm5y13, cm5y14, cm5y15, cm5y16, cm5y17)

# Convert logical to numeric
cci <- cci %>% mutate(across(everything(), as.numeric))
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


## Convert epikey to character before writing table
df$EPIKEY <- as.character(df$EPIKEY)

## Write Duck DB table
dbWriteTable(con, "ph_left_ons_hes_proms", df)

## Remove temporary table
dbExecute(con, "DROP TABLE temp3_left")
rm(df, cci)



#####

## The right and left datasets then need to be appended together

dbExecute(con, "CREATE TABLE ph_combined AS
          SELECT *
  FROM ph_left_ons_hes_proms

 UNION

 SELECT *
   FROM ph_right_ons_hes_proms
          ")


## Remove intermediary tables
dbExecute(con, "DROP TABLE ph_left")
dbExecute(con, "DROP TABLE ph_left_ons")
dbExecute(con, "DROP TABLE ph_left_ons_hes")
dbExecute(con, "DROP TABLE ph_left_ons_hes_proms")
dbExecute(con, "DROP TABLE ph_right")
dbExecute(con, "DROP TABLE ph_right_ons")
dbExecute(con, "DROP TABLE ph_right_ons_hes")
dbExecute(con, "DROP TABLE ph_right_ons_hes_proms")



########################################## Shutdown DuckDB con ##########################################

dbListTables(con)

# Shutdown database
dbDisconnect(con, shutdown=TRUE)
