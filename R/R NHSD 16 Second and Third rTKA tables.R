##### R NHS D - Set-up linked 2nd revision and 3rd revision tables

pacman::p_load(pacman, data.table, DBI, duckdb, tidyverse, tictoc)

## Connect to database
con = dbConnect(duckdb::duckdb(), dbdir=paste0(data_dir,"SQL/ss.duckdb"), read_only=FALSE)

## Create datasets of second rTKA
dbExecute(con, "CREATE TABLE second_rk AS
                SELECT *
                FROM all_rk
                WHERE revno = ?",
          params = list("Second linked rKR"))

## Link table to ONS
dbExecute(con, "CREATE TABLE second_rk_ons AS
                SELECT second_rk.*, ons.*
                FROM second_rk
                LEFT OUTER JOIN ons
                ON ons.nn_nid = second_rk.nn_nid;")

## Exclude records where DOD < DOR
dbExecute(con, "DELETE FROM second_rk_ons WHERE dod < op_date;")

## Link to HES
dbExecute(con, "ALTER TABLE second_rk_ons ALTER nn_nid TYPE VARCHAR;")
dbExecute(con, "ALTER TABLE second_rk_ons ADD COLUMN start DATE;")
dbExecute(con, "ALTER TABLE second_rk_ons ADD COLUMN finish DATE;")
dbExecute(con, "UPDATE second_rk_ons SET start = op_date - 7;")
dbExecute(con, "UPDATE second_rk_ons SET finish = op_date + 7;")

dbExecute(con, "CREATE TABLE temp AS
                  SELECT second_rk_ons.*, hes_working.*
                  FROM second_rk_ons
                  LEFT OUTER JOIN hes_working
                  ON hes_working.study_id = second_rk_ons.nn_nid
                  AND hes_working.admidate_filled >= second_rk_ons.start
                  AND hes_working.admidate_filled <= second_rk_ons.finish;")

dbExecute(con, "ALTER TABLE temp DROP COLUMN date_diff;")
dbExecute(con, "ALTER TABLE temp ADD COLUMN date_diff INTEGER;")
dbExecute(con, "UPDATE temp SET date_diff = ABS(op_date - admidate_filled);")

dbExecute(con, "CREATE TABLE second_rk_ons_hes AS
                WITH cte AS
                (
                   SELECT *,
                         ROW_NUMBER() OVER (PARTITION BY nn_nid ORDER BY date_diff ASC) AS row_num
                   FROM temp
                )
                SELECT *
                FROM cte
                WHERE row_num = 1;")

dbExecute(con, "ALTER TABLE second_rk_ons_hes DROP row_num")

dbExecute(con, "DROP TABLE temp")

# Join PROMs
dbExecute(con, "ALTER TABLE proms ALTER epikey TYPE BIGINT;")

dbExecute(con, "CREATE TABLE temp3 AS
          WITH cte AS (
            SELECT *,
            ROW_NUMBER() OVER(PARTITION BY epikey ORDER BY episode_match_rank ASC) AS rn
            FROM proms
            WHERE EXISTS (
              SELECT epikey, op_date
              FROM second_rk_ons_hes
              WHERE second_rk_ons_hes.epikey = proms.epikey
                AND second_rk_ons_hes.op_date - proms.q1_completed_date <=90
                AND second_rk_ons_hes.op_date - proms.q1_completed_date >=0
                AND proms.q2_completed_date - second_rk_ons_hes.op_date >=180
                AND proms.q2_completed_date - second_rk_ons_hes.op_date <=365
                AND proms.q1_completed_date IS NOT NULL
                AND proms.q2_completed_date IS NOT NULL
                )
          )

          SELECT second_rk_ons_hes.*, cte.*
          FROM second_rk_ons_hes
          LEFT OUTER JOIN cte
          ON cte.epikey = second_rk_ons_hes.epikey
            AND cte.rn = 1;")

# Derive some fields
df <- dbGetQuery(con, "SELECT * FROM temp3")

# 5 year Charlson lookback
cci <- df %>% select(revision_njr_index_no, revision_procedure_id, cm5y1, cm5y2, cm5y3, cm5y4, cm5y5, cm5y6, cm5y7, cm5y8, cm5y9, cm5y10, cm5y11, cm5y12, cm5y13, cm5y14, cm5y15, cm5y16, cm5y17)

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
# df %>% janitor::tabyl(patient_gender.x)
df$gender <- dplyr::recode(df$patient_gender.x, `M` = "Male", `F` = "Female", `x` = "Not specified")
df <- df %>% select(-patient_gender.x)


# ASA
## There is no missing data
## df %>% janitor::tabyl(asa)
## Coded as ASA 1, ASA 2 or ASA 3+
df <-
  df %>% 
  mutate(asa_grade = case_when(asa == "P1 - Fit and healthy" ~ "ASA 1",
                               asa == "P2 - Mild disease not incapacitating" ~ "ASA 2",
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
# str(df$bmi)

df <-
  df %>% 
  mutate(bmi_plaus = case_when(bmi <13 ~ NA_real_,
                               bmi >80 ~ NA_real_,
                               TRUE ~ bmi))


# Create new diagnosis field
df <- df %>%
  mutate(ifrhier2 = case_when(ifrhier == "Component Wear" ~ "Loosening/Lysis",
                              ifrhier == "Not specified" ~ "Other",
                              TRUE ~ as.character(ifrhier)))

df$ifrhier2 <- factor(df$ifrhier2, levels=c("Infection", "Malalignment", "Loosening/Lysis", "Instability", "Fracture", "Progressive Arthritis", "Stiffness", "Unexplained pain", "Other"), ordered=TRUE) 


## Convert epikey to character before writing table
df$EPIKEY <- as.character(df$EPIKEY)

## Write Duck DB table
dbWriteTable(con, "second_rk_ons_hes_proms", df, overwrite=TRUE)

## Remove temporary tables
dbExecute(con, "DROP TABLE temp3")
rm(df, cci)

## Calculate the number of second rTKA linked to PROMs
dbExecute(con, "ALTER TABLE second_rk_ons_hes_proms ADD COLUMN promyn VARCHAR;")

dbExecute(con, "UPDATE second_rk_ons_hes_proms
                SET promyn = CASE WHEN proms_serial_no IS NULL THEN 'No'
                             ELSE 'Yes'
                             END;")



################################################################################

## Create datasets of third rTKA
dbExecute(con, "CREATE TABLE third_rk AS
                SELECT *
                FROM all_rk
                WHERE revno = ?",
          params = list("Third or more linked rKR"))


## Link table to ONS
dbExecute(con, "CREATE TABLE third_rk_ons AS
                SELECT third_rk.*, ons.*
                FROM third_rk
                LEFT OUTER JOIN ons
                ON ons.nn_nid = third_rk.nn_nid;")

## Exclude records where DOD < DOR
dbExecute(con, "DELETE FROM third_rk_ons WHERE dod < op_date;")

## Link to HES
dbExecute(con, "ALTER TABLE third_rk_ons ALTER nn_nid TYPE VARCHAR;")
dbExecute(con, "ALTER TABLE third_rk_ons ADD COLUMN start DATE;")
dbExecute(con, "ALTER TABLE third_rk_ons ADD COLUMN finish DATE;")
dbExecute(con, "UPDATE third_rk_ons SET start = op_date - 7;")
dbExecute(con, "UPDATE third_rk_ons SET finish = op_date + 7;")

dbExecute(con, "CREATE TABLE temp AS
                  SELECT third_rk_ons.*, hes_working.*
                  FROM third_rk_ons
                  LEFT OUTER JOIN hes_working
                  ON hes_working.study_id = third_rk_ons.nn_nid
                  AND hes_working.admidate_filled >= third_rk_ons.start
                  AND hes_working.admidate_filled <= third_rk_ons.finish;")

dbExecute(con, "ALTER TABLE temp DROP COLUMN date_diff;")
dbExecute(con, "ALTER TABLE temp ADD COLUMN date_diff INTEGER;")
dbExecute(con, "UPDATE temp SET date_diff = ABS(op_date - admidate_filled);")

dbExecute(con, "CREATE TABLE third_rk_ons_hes AS
                WITH cte AS
                (
                   SELECT *,
                         ROW_NUMBER() OVER (PARTITION BY nn_nid ORDER BY date_diff ASC) AS row_num
                   FROM temp
                )
                SELECT *
                FROM cte
                WHERE row_num = 1;")

dbExecute(con, "ALTER TABLE third_rk_ons_hes DROP row_num")

dbExecute(con, "DROP TABLE temp")

# Join PROMs
dbExecute(con, "ALTER TABLE proms ALTER epikey TYPE BIGINT;")

dbExecute(con, "CREATE TABLE temp3 AS
          WITH cte AS (
            SELECT *,
            ROW_NUMBER() OVER(PARTITION BY epikey ORDER BY episode_match_rank ASC) AS rn
            FROM proms
            WHERE EXISTS (
              SELECT epikey, op_date
              FROM third_rk_ons_hes
              WHERE third_rk_ons_hes.epikey = proms.epikey
                AND third_rk_ons_hes.op_date - proms.q1_completed_date <=90
                AND third_rk_ons_hes.op_date - proms.q1_completed_date >=0
                AND proms.q2_completed_date - third_rk_ons_hes.op_date >=180
                AND proms.q2_completed_date - third_rk_ons_hes.op_date <=365
                AND proms.q1_completed_date IS NOT NULL
                AND proms.q2_completed_date IS NOT NULL
                )
          )

          SELECT third_rk_ons_hes.*, cte.*
          FROM third_rk_ons_hes
          LEFT OUTER JOIN cte
          ON cte.epikey = third_rk_ons_hes.epikey
            AND cte.rn = 1;")

# Derive some fields
df <- dbGetQuery(con, "SELECT * FROM temp3")

# 5 year Charlson lookback
cci <- df %>% select(revision_njr_index_no, revision_procedure_id, cm5y1, cm5y2, cm5y3, cm5y4, cm5y5, cm5y6, cm5y7, cm5y8, cm5y9, cm5y10, cm5y11, cm5y12, cm5y13, cm5y14, cm5y15, cm5y16, cm5y17)

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
# df %>% janitor::tabyl(patient_gender.x)
df$gender <- dplyr::recode(df$patient_gender.x, `M` = "Male", `F` = "Female", `x` = "Not specified")
df <- df %>% select(-patient_gender.x)


# ASA
## There is no missing data
## df %>% janitor::tabyl(asa)
## Coded as ASA 1, ASA 2 or ASA 3+
df <-
  df %>% 
  mutate(asa_grade = case_when(asa == "P1 - Fit and healthy" ~ "ASA 1",
                               asa == "P2 - Mild disease not incapacitating" ~ "ASA 2",
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
# str(df$bmi)

df <-
  df %>% 
  mutate(bmi_plaus = case_when(bmi <13 ~ NA_real_,
                               bmi >80 ~ NA_real_,
                               TRUE ~ bmi))


# Create new diagnosis field
df <- df %>%
  mutate(ifrhier2 = case_when(ifrhier == "Component Wear" ~ "Loosening/Lysis",
                              ifrhier == "Not specified" ~ "Other",
                              TRUE ~ as.character(ifrhier)))

df$ifrhier2 <- factor(df$ifrhier2, levels=c("Infection", "Malalignment", "Loosening/Lysis", "Instability", "Fracture", "Progressive Arthritis", "Stiffness", "Unexplained pain", "Other"), ordered=TRUE) 


## Convert epikey to character before writing table
df$EPIKEY <- as.character(df$EPIKEY)

## Write Duck DB table
dbWriteTable(con, "third_rk_ons_hes_proms", df, overwrite=TRUE)

## Remove temporary tables
dbExecute(con, "DROP TABLE temp3")
rm(df, cci)

## Calculate the number of third rTKA linked to PROMs
dbExecute(con, "ALTER TABLE third_rk_ons_hes_proms ADD COLUMN promyn VARCHAR;")

dbExecute(con, "UPDATE third_rk_ons_hes_proms
                SET promyn = CASE WHEN proms_serial_no IS NULL THEN 'No'
                             ELSE 'Yes'
                             END;")


# First rTKA

## Calculate the number of first rTKA linked to PROMs
dbExecute(con, "ALTER TABLE rk_ons_hes_proms ADD COLUMN promyn VARCHAR;")

dbExecute(con, "UPDATE rk_ons_hes_proms
                SET promyn = CASE WHEN proms_serial_no IS NULL THEN 'No'
                             ELSE 'Yes'
                             END;")




# Shutdown
dbDisconnect(con, shutdown=TRUE)
