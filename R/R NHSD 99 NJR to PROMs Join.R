# R NHSD 99 Join NJR to PROMs directly



########################################## Create bridge table in DuckDb ##########################################

p_unload(all)
pacman::p_load(pacman, data.table, rio, tidyverse, janitor, duckdb, DBI, logger)
setwd(data_dir)

# Source R NHSD 1 to set-up directories

# Connect to database
con = dbConnect(duckdb::duckdb(), dbdir=paste0(data_dir,"SQL/ss.duckdb"), read_only=FALSE)

# PRAGMAs
dbExecute(con, "PRAGMA threads=10")
dbExecute(con, paste0("PRAGMA temp_directory='", data_dir,"TEMP/hes.tmp'"))
dbExecute(con, "PRAGMA memory_limit='32GB'")

# Check tables have loaded as expected
dbListTables(con)

# Import
log_info('Create Bridge table')
bridge <- import(paste0(data_dir, "PROMS/FILE0156957_NIC380650_PROMs_BRIDGE.txt"), quote="")

# Understand the bridge file
colSums(!is.na(bridge))
colSums(is.na(bridge))

# There are no missing PROMS_SERIAL_NO, but it is not distinct
eeptools::isid(bridge, c("PROMS_SERIAL_NO"), verbose = FALSE)

# However, there are only a small number of duplicates
bridge %>%
  group_by(PROMS_SERIAL_NO) %>%
  summarise(count = n()) %>% 
  janitor::tabyl(count)

# Remove duplicates on PROMS_SERIAL_NO
bridge <-
  bridge %>%
  distinct(PROMS_SERIAL_NO, .keep_all = TRUE)

# epikey is integer64, which is not handled well, so convert to character
str(bridge$EPIKEY)
bridge$EPIKEY <- as.character(bridge$EPIKEY)

# Rename fields prior to joins
bridge <- bridge %>%
  rename(epikey_b = EPIKEY, 
         fyear_b = FYEAR)

# Create bridge table 
dbWriteTable(con, "bridge", bridge)

# Convert EPIKEY back
dbExecute(con, "ALTER TABLE bridge ALTER epikey_b TYPE BIGINT;")



########################################## Create distinct PROMS table in DuckDb ##########################################

# Identify duplicates on PROMS_SERIAL_NO from the PROMS table
dbGetQuery(con, "
                SELECT PROMS_SERIAL_NO, COUNT(*) AS count
                FROM proms
                GROUP BY PROMS_SERIAL_NO
                HAVING count > 1;")

# Create new distinct PROMS table
dbExecute(con, "
               CREATE TABLE proms_d AS
                SELECT * FROM 
                (
                  SELECT *, 
                    ROW_NUMBER() OVER(PARTITION BY PROMS_SERIAL_NO) AS rownum
                  FROM proms
                ) as a
                WHERE rownum =1;")



########################################## Merge STUDY_ID on to PROMS ##########################################

# Join STUDY_ID from the BRIDGE table on to the PROMS table
dbExecute(con, "CREATE TABLE proms_b AS
                  SELECT proms_d.*, bridge.*
                  FROM proms_d
                  LEFT OUTER JOIN bridge
                  ON proms_d.proms_serial_no = bridge.proms_serial_no;")


# # Do some checks for conflicts within the new table
# checks <-
#   dbGetQuery(con, 
#            "SELECT *
#              FROM proms_b;")
# 
# checks <- checks %>% select(epikey, epikey_b, proms_serial_no, `PROMS_SERIAL_NO:1`, STUDY_ID, fyear, fyear_b)
# 
# checks %>%
#   filter(epikey != epikey_b)
# 
# checks %>%
#   filter(proms_serial_no != `PROMS_SERIAL_NO:1`)
# 
# checks %>%
#   filter(fyear != fyear_b)
# 
# checks %>%
#   filter(epikey != fyear)
# 
# rm(checks)


x <-
  dbGetQuery(con,
           "SELECT *
             FROM proms_b;")

x <- x %>% select(epikey, proms_serial_no, fyear, STUDY_ID, `PROMS_SERIAL_NO:1`, epikey_b, fyear_b)

x1 <-
  x %>%
  summarise(
    across(everything(), list(
      nulls = ~sum(is.na(.)),
      notnulls = ~sum(!is.na(.))
    ))
  )

# Copy column names to clipboard
column_names <- colnames(x1)
clipr::write_clip(column_names)

# Reshape the dataframe from wide to long format

x1_long <- gather(x1, key = Variable, value = Value)

# Separate the 'Variable' column into 'Column' and 'Type'
x1_long <- separate(x1_long, col = Variable, into = c("Column", "Type"), sep = "_(?=[^_]*$)", remove = FALSE)

# Display the reshaped DataFrame
print(x1_long)


########################################## Merge NJR directly to PROMS ##########################################

# Revision knees
dbExecute(con, "CREATE TABLE rk_ons_hes_proms_new AS
          WITH cte AS (
            SELECT *,
            ROW_NUMBER() OVER(PARTITION BY study_id ORDER BY episode_match_rank ASC) AS rn
            FROM proms_b
            WHERE EXISTS (
              SELECT study_id, op_date
              FROM rk_ons_hes
              WHERE rk_ons_hes.study_id = proms_b.study_id
                AND rk_ons_hes.op_date - proms_b.q1_completed_date <=90
                AND rk_ons_hes.op_date - proms_b.q1_completed_date >=0
                AND proms_b.q2_completed_date - rk_ons_hes.op_date >=180
                AND proms_b.q2_completed_date - rk_ons_hes.op_date <=365
                AND proms_b.q1_completed_date IS NOT NULL
                AND proms_b.q2_completed_date IS NOT NULL
                )
          )

          SELECT rk_ons_hes.*, cte.*
          FROM rk_ons_hes
          LEFT OUTER JOIN cte
          ON cte.study_id = rk_ons_hes.study_id
            AND cte.rn = 1;")


# Compare rk_ons_hes_proms_new with rk_ons_hes_proms
df <- dbGetQuery(con, "SELECT proms_serial_no FROM rk_ons_hes_proms")
df1 <- dbGetQuery(con, "SELECT * FROM rk_ons_hes_proms_new")
df1 <- df1 |> select(proms_serial_no, `PROMS_SERIAL_NO:1`)

colSums(!is.na(df))
colSums(!is.na(df1))



# Primary knees
dbExecute(con, "CREATE TABLE pk_ons_hes_proms_new AS
          WITH cte AS (
            SELECT *,
            ROW_NUMBER() OVER(PARTITION BY study_id ORDER BY episode_match_rank ASC) AS rn
            FROM proms_b
            WHERE EXISTS (
              SELECT study_id, primary_op_date
              FROM pk_ons_hes
              WHERE pk_ons_hes.study_id = proms_b.study_id
                AND pk_ons_hes.primary_op_date - proms_b.q1_completed_date <=90
                AND pk_ons_hes.primary_op_date - proms_b.q1_completed_date >=0
                AND proms_b.q2_completed_date - pk_ons_hes.primary_op_date >=180
                AND proms_b.q2_completed_date - pk_ons_hes.primary_op_date <=365
                AND proms_b.q1_completed_date IS NOT NULL
                AND proms_b.q2_completed_date IS NOT NULL
                )
          )

          SELECT pk_ons_hes.*, cte.*
          FROM pk_ons_hes
          LEFT OUTER JOIN cte
          ON cte.study_id = pk_ons_hes.study_id
            AND cte.rn = 1;")


# Compare pk_ons_hes_proms_new with pk_ons_hes_proms

## How many PROMs are there for TKAs and UKAs in each dataset?
df <- dbGetQuery(con, "SELECT proms_serial_no, primary_patient_procedure FROM pk_ons_hes_proms")
df1 <- dbGetQuery(con, "SELECT * FROM pk_ons_hes_proms_new")
df %>% filter(!is.na(proms_serial_no)) %>% janitor::tabyl(primary_patient_procedure)
df1 %>% filter(!is.na(proms_serial_no)) %>% janitor::tabyl(primary_patient_procedure)



# How many records on proms_b have a study_id?
pb <- dbGetQuery(con, "SELECT * FROM proms_b")
pb %>% filter(!is.na(STUDY_ID)) %>% count()







# No date filters

# Primary knees
dbExecute(con, "CREATE TABLE pk_ons_hes_proms_lax AS
          WITH cte AS (
            SELECT *,
            ROW_NUMBER() OVER(PARTITION BY study_id ORDER BY episode_match_rank ASC) AS rn
            FROM proms_b
            WHERE EXISTS (
              SELECT study_id, primary_op_date
              FROM pk_ons_hes
              WHERE pk_ons_hes.study_id = proms_b.study_id
                )
          )

          SELECT pk_ons_hes.*, cte.*
          FROM pk_ons_hes
          LEFT OUTER JOIN cte
          ON cte.study_id = pk_ons_hes.study_id
            AND cte.rn = 1;")

## How many PROMs are there for TKAs and UKAs in each dataset?
df <- dbGetQuery(con, "SELECT proms_serial_no, primary_patient_procedure FROM pk_ons_hes_proms")
df1 <- dbGetQuery(con, "SELECT * FROM pk_ons_hes_proms_lax")
df %>% filter(!is.na(proms_serial_no)) %>% janitor::tabyl(primary_patient_procedure)
df1 %>% filter(!is.na(proms_serial_no)) %>% janitor::tabyl(primary_patient_procedure)
    




# Wide date filters
dbExecute(con, "CREATE TABLE pk_ons_hes_proms_wd AS
          WITH cte AS (
            SELECT *,
            ROW_NUMBER() OVER(PARTITION BY study_id ORDER BY episode_match_rank ASC) AS rn
            FROM proms_b
            WHERE EXISTS (
              SELECT study_id, primary_op_date
              FROM pk_ons_hes
              WHERE pk_ons_hes.study_id = proms_b.study_id
                AND pk_ons_hes.primary_op_date - proms_b.q1_completed_date <=365
                AND pk_ons_hes.primary_op_date - proms_b.q1_completed_date >-30
                AND proms_b.q2_completed_date - pk_ons_hes.primary_op_date >=90
                AND proms_b.q2_completed_date - pk_ons_hes.primary_op_date <=730
                AND proms_b.q1_completed_date IS NOT NULL
                AND proms_b.q2_completed_date IS NOT NULL
                )
          )

          SELECT pk_ons_hes.*, cte.*
          FROM pk_ons_hes
          LEFT OUTER JOIN cte
          ON cte.study_id = pk_ons_hes.study_id
            AND cte.rn = 1;")


## How many PROMs are there for TKAs and UKAs in each dataset?
df <- dbGetQuery(con, "SELECT proms_serial_no, primary_patient_procedure FROM pk_ons_hes_proms")
df1 <- dbGetQuery(con, "SELECT * FROM pk_ons_hes_proms_wd")
df %>% filter(!is.na(proms_serial_no)) %>% janitor::tabyl(primary_patient_procedure)
df1 %>% filter(!is.na(proms_serial_no)) %>% janitor::tabyl(primary_patient_procedure)




# Primary knees - Q1 only
dbExecute(con, "CREATE TABLE pk_ons_hes_proms_q1 AS
          WITH cte AS (
            SELECT *,
            ROW_NUMBER() OVER(PARTITION BY study_id ORDER BY episode_match_rank ASC) AS rn
            FROM proms_b
            WHERE EXISTS (
              SELECT study_id, primary_op_date
              FROM pk_ons_hes
              WHERE pk_ons_hes.study_id = proms_b.study_id
                AND pk_ons_hes.primary_op_date - proms_b.q1_completed_date <=365
                AND pk_ons_hes.primary_op_date - proms_b.q1_completed_date >-30
                AND proms_b.q1_completed_date IS NOT NULL
                )
          )

          SELECT pk_ons_hes.*, cte.*
          FROM pk_ons_hes
          LEFT OUTER JOIN cte
          ON cte.study_id = pk_ons_hes.study_id
            AND cte.rn = 1;")


## How many PROMs are there for TKAs and UKAs in each dataset?
df1 <- dbGetQuery(con, "SELECT * FROM pk_ons_hes_proms_q1")
df1 %>% filter(!is.na(proms_serial_no)) %>% janitor::tabyl(primary_patient_procedure)


## Bridge file
# Purpose:
# To join NJR directly to PROMs using STUDY_ID
# To join HES directly to PROMs using STUDY_ID

# Expectations:
# 95% primary procedures have a Q1
# ~50% primary procedures have a Q2

# How many rows in bridge file? ~1.07 million
nrow(bridge)

# How many records in bridge file link to HES? ~50k
nrow(bridge |> filter(nchar(STUDY_ID) > 12))

# How many records in bridge file link to NJR? ~1.02 million
nrow(bridge |> filter(nchar(STUDY_ID) <= 12))


dbGetQuery(con, "SELECT count(*) from pk_ons_hes_proms WHERE primary_op_date > '2008-12-31'")
dbGetQuery(con, "SELECT count(*) from pk_ons_hes_proms_q1 WHERE primary_op_date > '2008-12-31'")
dbGetQuery(con, "SELECT count(*) from pk_ons_hes_proms WHERE proms_serial_no IS NOT NULL")
dbGetQuery(con, "SELECT count(*) from pk_ons_hes_proms_q1 WHERE proms_serial_no IS NOT NULL")
dbGetQuery(con, "SELECT count(*) from pk_ons_hes_proms_lax WHERE proms_serial_no IS NOT NULL")


dbListTables(con)

dbExecute(con, "DROP TABLE pk_ons_hes_proms_wd;
                DROP TABLE pk_ons_hes_proms_lax;
                DROP TABLE pk_ons_hes_proms_q1;")


# How many distinct patients are in the entire dataset? ~3.35 million
dbExecute(con, "CREATE TABLE ids AS
                 SELECT DISTINCT study_id FROM hes_working
                 UNION
                 SELECT DISTINCT primary_njr_index_no FROM pk
                 UNION
                 SELECT DISTINCT primary_njr_index_no FROM ph
                 UNION
                 SELECT DISTINCT revision_njr_index_no FROM all_rk
                 UNION
                 SELECT DISTINCT revision_njr_index_no FROM all_rh
                 UNION
                 SELECT DISTINCT study_id FROM proms_b
           ")

df <- dbGetQuery(con, "SELECT * FROM ids")

dbExecute(con, "DROP TABLE ids")

# # How many distinct patients are in the entire dataset excluding PROMS? ~3.31 million
# dbExecute(con, "CREATE TABLE ids AS
#                  SELECT DISTINCT study_id FROM hes_working
#                  UNION
#                  SELECT DISTINCT primary_njr_index_no FROM pk
#                  UNION
#                  SELECT DISTINCT primary_njr_index_no FROM ph
#                  UNION
#                  SELECT DISTINCT revision_njr_index_no FROM all_rk
#                  UNION
#                  SELECT DISTINCT revision_njr_index_no FROM all_rh")
# 
# dbExecute(con, "DROP TABLE ids")

# How many records in the proms_b table have no STUDY_ID? 483k
dbGetQuery(con, "SELECT COUNT(*) AS count_null_study_id
                 FROM proms_b
                 WHERE study_id IS NULL;")

# How many have a STUDY_ID? 1.05m
dbGetQuery(con, "SELECT COUNT(*) AS count_null_study_id
                 FROM proms_b
                 WHERE study_id IS NOT NULL;")

# Shutdown
dbDisconnect(con, shutdown=TRUE)
