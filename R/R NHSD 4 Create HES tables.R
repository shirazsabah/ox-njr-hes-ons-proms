# R NHSD 4 Create HES tables in Duck DB
# # Open connection to DuckDB
# With a database file (not shared between processes)
con = dbConnect(duckdb::duckdb(), dbdir=paste0(data_dir,"SQL/ss.duckdb"), read_only=FALSE)

# # Register the Arrow data source as a virtual table
duckdb::duckdb_register_arrow(con, "hes_table", hes)

# Set PRAGMAs
# NB: dbExecute always returns a scalar of the number of rows affected, which will be 0 here
dbExecute(con, "PRAGMA threads=10;
          PRAGMA memory_limit='32GB';
          PRAGMA enable_profiling='json';
          PRAGMA profiling_mode='detailed';
          SET enable_progress_bar=true;")

dbExecute(con, paste0("PRAGMA profile_output='", data_dir, "profile.json'"))

# Persistent storage back-end
dbExecute(con, paste0("PRAGMA temp_directory='", data_dir, "TEMP/hes.tmp'"))

# # Check settings
# settings <- dbGetQuery(con, "SELECT * FROM duckdb_settings();")

# Create a HES cohort
## The episode must be:
  ## Valid (EPI_VALID and EPI_BAD)
  ## Have a joint replacement OPCS code (jr)
  ## Have an NJR study_id
## And, not a duplicate record
## NHS Digital consider a record to be a duplicate if the following fields are the same for a given patient:
  ## PROCODE
  ## EPISTART
  ## EPIEND
## We have defined lower rq as better

# The episode must be valid
log_info('Creating HES cohort table')

dbExecute(con, ("CREATE TABLE hes_cohort AS
                        WITH cte AS
                        (
                          SELECT study_id, epistart, epiend, procode3, rq, row_id, epikey, fyear, epi_valid, epi_bad,
                          row_number() OVER dupwindow AS dups,
                          FROM hes_table
                          WHERE epi_valid = TRUE 
                            AND epi_bad = FALSE
                            AND jr IS NOT NULL
                            AND LEN(study_id) <12
                          WINDOW dupwindow AS (
                            PARTITION BY study_id, procode3, epistart, epiend
                              ORDER BY rq ASC
                              )
                          ORDER BY study_id, procode3, epistart, epiend, rq
                        )
                  
                  SELECT *
                  FROM cte
                  WHERE dups =1;"))

# Flowchart
# Records excluded because:
  # No suitable STUDY_ID
  # + Not a valid record
  # + Duplicate record
  # + Not a joint replacement

supplied <- hes %>% count() %>% collect()

no_id <- supplied - dbGetQuery(con, "SELECT Count(*)
                                     FROM hes_table
                                     WHERE LEN(study_id) <12;")

not_valid <- supplied - no_id - dbGetQuery(con, "SELECT Count(*)
                                                 FROM hes_table
                                                 WHERE LEN(study_id) <12 
                                                  AND epi_valid = TRUE 
                                                  AND epi_bad = FALSE;")

not_jr <- supplied - no_id - not_valid - dbGetQuery(con, "SELECT Count(*)
                                                          FROM hes_table
                                                          WHERE LEN(study_id) <12 
                                                            AND epi_valid = TRUE 
                                                            AND epi_bad = FALSE
                                                            AND jr IS NOT NULL;")

dup <- supplied - no_id - not_valid - not_jr - dbGetQuery(con, "SELECT Count(*) FROM hes_cohort;")

cohort <- dbGetQuery(con, "SELECT Count(*) FROM hes_cohort;")

log_info('Number of HES records supplied: {supplied}')

log_info('HES records excluded: 
         
         - No study_id {no_id}
         - Bad episode coding {not_valid}
         - Not joint replacement {not_jr}
         - Duplicates {dup}
         ')

log_info('HES cohort presented for linkage: {cohort}') 
         
# Charlson
log_info('Creating 5-year look-back Charlson comorbidity index & frailty dummies')

dbExecute(con, paste0("CREATE TABLE charlson AS
                        SELECT study_id, row_id, fyear,
                          MAX(ami) OVER fiveyr AS cm5y1,
                          MAX(cva) OVER fiveyr AS cm5y2,
                          MAX(chf) OVER fiveyr AS cm5y3,
                          MAX(ctd) OVER fiveyr AS cm5y4,
                          MAX(dem) OVER fiveyr AS cm5y5,
                          MAX(dia) OVER fiveyr AS cm5y6,
                          MAX(ldi) OVER fiveyr AS cm5y7,
                          MAX(pud) OVER fiveyr AS cm5y8,
                          MAX(pvd) OVER fiveyr AS cm5y9,
                          MAX(pdi) OVER fiveyr AS cm5y10,
                          MAX(can) OVER fiveyr AS cm5y11,
                          MAX(dco) OVER fiveyr AS cm5y12,
                          MAX(par) OVER fiveyr AS cm5y13,
                          MAX(rdi) OVER fiveyr AS cm5y14,
                          MAX(mcan) OVER fiveyr AS cm5y15,
                          MAX(sld) OVER fiveyr AS cm5y16,
                          MAX(hiv) OVER fiveyr AS cm5y17,
                          MAX(anaem) OVER fiveyr AS frailty5y1,
                          MAX(arthr) OVER fiveyr AS frailty5y2,
                          MAX(atria) OVER fiveyr AS frailty5y3,
                          MAX(cereb) OVER fiveyr AS frailty5y4,
                          MAX(chron) OVER fiveyr AS frailty5y5,
                          MAX(diabe) OVER fiveyr AS frailty5y6,
                          MAX(dizzi) OVER fiveyr AS frailty5y7,
                          MAX(dyspn) OVER fiveyr AS frailty5y8,
                          MAX(falls) OVER fiveyr AS frailty5y9,
                          MAX(footp) OVER fiveyr AS frailty5y10,
                          MAX(fragi) OVER fiveyr AS frailty5y11,
                          MAX(heari) OVER fiveyr AS frailty5y12,
                          MAX(heafa) OVER fiveyr AS frailty5y13,
                          MAX(heava) OVER fiveyr AS frailty5y14,
                          MAX(house) OVER fiveyr AS frailty5y15,
                          MAX(hyper) OVER fiveyr AS frailty5y16,
                          MAX(hypot) OVER fiveyr AS frailty5y17,
                          MAX(ischa) OVER fiveyr AS frailty5y18,
                          MAX(memor) OVER fiveyr AS frailty5y19,
                          MAX(mobil) OVER fiveyr AS frailty5y20,
                          MAX(osteo) OVER fiveyr AS frailty5y21,
                          MAX(parki) OVER fiveyr AS frailty5y22,
                          MAX(pepti) OVER fiveyr AS frailty5y23,
                          MAX(perip) OVER fiveyr AS frailty5y24,
                          MAX(respi) OVER fiveyr AS frailty5y25,
                          MAX(skinu) OVER fiveyr AS frailty5y26,
                          MAX(sleep) OVER fiveyr AS frailty5y27,
                          MAX(socia) OVER fiveyr AS frailty5y28,
                          MAX(thyro) OVER fiveyr AS frailty5y29,                        
                          MAX(incon) OVER fiveyr AS frailty5y30,  
                          MAX(uridi) OVER fiveyr AS frailty5y31,  
                          MAX(visua) OVER fiveyr AS frailty5y32,  
                          MAX(weigh) OVER fiveyr AS frailty5y33,  
                          MAX(activ) OVER fiveyr AS frailty5y34
                        FROM hes_table
                        WINDOW fiveyr AS (
                          PARTITION BY study_id
                            ORDER BY admidate_filled ASC
                            RANGE BETWEEN INTERVAL 1826 DAYS PRECEDING
                                      AND INTERVAL 0 DAYS FOLLOWING)
                        QUALIFY(epi_valid = TRUE
                                  AND epi_bad = FALSE
                                  AND jr IS NOT NULL
                                  AND LEN(study_id) <12)
                        ORDER BY study_id, admidate_filled
                        "))


# 90d comps
log_info('Creating 90-day complications table')

dbExecute(con, paste0("CREATE TABLE complications AS
                        SELECT study_id, row_id, fyear,
                          MAX(compacva) OVER ninety AS compacva90,
                          MAX(complrti) OVER ninety AS complrti90,
                          MAX(compmi) OVER ninety AS compmi90,
                          MAX(compdvt) OVER ninety AS compdvt90,
                          MAX(comppe) OVER ninety AS comppe90,
                          MAX(compaki) OVER ninety AS compaki90,
                          MAX(computi) OVER ninety AS computi90,
                          MAX(compwound) OVER ninety AS compwound90,
                          MAX(compssi) OVER ninety AS compssi90,
                          MAX(compppf) OVER ninety AS compppf90,
                          MAX(comppros) OVER ninety AS comppros90,
                          MAX(compnvinj) OVER ninety AS compnvinj90,
                          MAX(compbloodtx) OVER ninety AS compbloodtx90
                        FROM hes_table
                        WINDOW ninety AS (
                          PARTITION BY study_id
                            ORDER BY admidate_filled ASC
                            RANGE BETWEEN INTERVAL 0 DAYS PRECEDING
                                      AND INTERVAL 90 DAYS FOLLOWING)
                        QUALIFY(epi_valid = TRUE
                                  AND epi_bad = FALSE
                                  AND jr IS NOT NULL
                                  AND LEN(study_id) <12)
                        ORDER BY study_id, admidate_filled"))


# Spells
## provspnops = unique spell id
## spelbgin
# 0 = Not the first episode of spell
# 1 = First episode of spell that started in a previous year
# 2 = First episode of spell that started in current year
# Null = Not applicable
## speldur = # days
## spelend = boolean

# CIPS
## Episodes are ordered by: study_id, admidate, disdate, transit, provspnops
## Two (or more) consecutive spells belong to the same CIP if they:
# Belong to the same spell OR
# Have the same study_id AND
# Not more than 2 days elapsed between discharge from the previous spell and admission to the next spell AND
# One of the following criteria is met, indicating that a transfer has taken place:
#   disdest of the previous provider spell is in [49-53, 84, 87] OR
#   admisorc of next provider spell is in [49-53, 87] OR
#   admimeth of next provider spell is in [81, 2B] AND
#   EXCLUDE the combination where disdest of the first spell is 19 AND admisorc of the next spell is 51 AND admimeth of the next spell is 21

# DISMETH
# 1 = Discharged on clinical advice
# 2 = Self-discharged
# 3 = Discharge by MH tribunal
# 4 = Died
# 8 = Not applicable: patient still in hospital
# 9 = Not known: a validation error

# DISDEST
# 19 = Usual place of residence
# 51 = NHS other hospital provider - ward for general PATIENTS or the younger physically disabled
# 79 = Not applicable - PATIENT died or still birth
# 87 = Non-NHS run hospital
# 98 = Not applicable - Hospital Provider Spell not finished at episode end i.e. not discharged
# 99 = Not Known

# EPIORDER
# 98 = Not applicable
# 99 = Not known

# TRANSIT
# 0 = Neither admission nor discharge a transfer
# 1 = Admission not a transfer, discharge is a transfer
# 2 = Admission is a transfer, discharge is not a transfer
# 3 = Admission is a transfer, discharge is a transfer

log_info('Creating SPELLS/CIPS')

# The code below translates the HES Pipeline R method to DuckDB.
# Our old method used derived fields from NHS Digital (e.g. provspnops).
# In case of out-of-memory error, split the SQL queries rather than using CTEs.

# Create spells
cols_to_keep <- c("row_id", "fyear", "study_id", "admidate_filled", "epistart", "dismeth", "disdate", "epiend", "epi_valid", "epi_bad", "epiorder", "transit", "epikey", "procode3", "disdest", "admimeth", "admisorc")
cols_to_keep <- cols_to_keep[!cols_to_keep %in% c("NEW_SPELL", "ROWNUMBER", "SPELL_ID")]
spell_grouping <- "(PARTITION BY STUDY_ID, PROCODE3 ORDER BY EPISTART, EPIEND, EPIORDER, TRANSIT, EPIKEY)"

dbExecute(con, paste0("CREATE TABLE APC AS 
                       WITH APC_DERIVED AS
                       (
                         SELECT ", str_c(cols_to_keep, collapse = ", "), ",
                         LAG(ADMIDATE_FILLED, 1) OVER ", spell_grouping, " AS PREV_ADMIDATE_FILLED,
                         LAG(EPISTART, 1) OVER ", spell_grouping, " AS PREV_EPISTART,
                         LAG(DISMETH, 1) OVER ", spell_grouping, " AS PREV_DISMETH,
                         LAG(EPIEND, 1) OVER ", spell_grouping, " AS PREV_EPIEND,
                         ROW_NUMBER() OVER ", spell_grouping, " AS ROWNUMBER
                         FROM hes_table
                       ),
                       APC_NEWSPELL AS
                       (
                         SELECT ", str_c(cols_to_keep, collapse = ", "), ", ROWNUMBER, CASE
                         WHEN EPI_VALID = FALSE THEN NULL
                         WHEN ADMIDATE_FILLED = PREV_ADMIDATE_FILLED THEN 0
                         WHEN EPISTART = PREV_EPISTART THEN 0
                         WHEN PREV_DISMETH IN (8,9) AND EPISTART = PREV_EPIEND THEN 0
                         ELSE 1
                         END NEW_SPELL
                         FROM APC_DERIVED
                       ),
                       APC_SPELLID1 AS
                       (
                         SELECT *, CASE
                         WHEN NEW_SPELL = 1 THEN ROWNUMBER
                         WHEN NEW_SPELL = 0 THEN 0
                         WHEN NEW_SPELL IS NULL THEN NULL
                         END SPELL_ID_TEMP
                         FROM APC_NEWSPELL
                       )
                       
                       SELECT ", str_c(cols_to_keep, collapse = ", "), ", NEW_SPELL, CASE
                       WHEN NEW_SPELL = 1 THEN SPELL_ID_TEMP
                       WHEN NEW_SPELL IS NULL THEN NULL
                       WHEN NEW_SPELL = 0 THEN MAX(SPELL_ID_TEMP) OVER (PARTITION BY STUDY_ID, PROCODE3 
                                                                        ORDER BY ROWNUMBER ASC ROWS BETWEEN
                                                                        UNBOUNDED PRECEDING AND CURRENT ROW)
                       END SPELL_ID
                       FROM APC_SPELLID1"))


first_episode_headers <- function(con) {
  headers <- dbListFields(con, "APC") 
  headers <- headers[!headers %in% c("EPIEND", "DISDATE", "DISDEST", "DISMETH", "DISREADYDATE", "EPIKEY",
                                     "EPIDUR", "EPIORDER", "EPISTAT", "MAINSPEF", "SPELBGIN", "SPELDUR",
                                     "SPELEND", "TRETSPEF", "CONSULT_TYPE", "ENCRYPTED_HESID_MISSING",
                                     "PROCODE3_MISSING", "TRANSIT", "ADMIDATE_MISSING", "EPIDUR_CALC",
                                     "EPI_BAD", "EPI_VALID", "NEW_SPELL", "ROWCOUNT",
                                     "SUBDATE", "SUSCOREHRG", "SUSHRG", "SUSHRGVERS", "SUSRECID")] 
  return(str_c(headers, collapse = ", "))
} 


first_episode_query <- function(con) {
  paste("SELECT ", 
        first_episode_headers(con),
        ", EPIKEY AS EPIKEY_ADM, EPI_COUNT
        FROM
        (SELECT *, COUNT() OVER (PARTITION BY STUDY_ID, SPELL_ID) AS EPI_COUNT FROM APC)
        WHERE NEW_SPELL = 1")
}


# SQL query to recover variables concerning the last episode in a spell
last_episode_query <- "SELECT STUDY_ID, DISDATE, DISDEST, DISMETH, SPELL_ID, PROCODE3,
                        EPIKEY AS EPIKEY_DIS, MAX_EPIEND AS EPIEND FROM
                        (SELECT *, MAX(EPIEND) OVER (PARTITION BY STUDY_ID, SPELL_ID) AS MAX_EPIEND FROM APC)"

dbExecute(con, paste("CREATE TABLE APCS AS 
           SELECT * FROM 
           (", first_episode_query(con), ")
           LEFT JOIN
           (", last_episode_query, ")
           USING (study_id, PROCODE3, SPELL_ID)"))


dbExecute(con, "ALTER TABLE APCS ADD COLUMN DISDATE_MISSING integer")

update_var <- function(con, table, var, value, condition_query) {
  dbExecute(con, paste0("UPDATE ", table, " SET ", var, " = ", value, " ", condition_query))
}

update_var(con, table = "APCS", var = "DISDATE_MISSING", value = FALSE,
           condition_query = "WHERE DISDATE IS NOT NULL")

updated <- update_var(con, table = "APCS", var = "DISDATE_MISSING", value = TRUE,
                      condition_query = "WHERE DISDATE IS NULL")

log_info("Found ", updated, " spells with missing DISDATE.")



cips_grouping <- "(PARTITION BY STUDY_ID ORDER BY ADMIDATE_FILLED)"
cols_to_keep <- dbListFields(con, "APCS")
cols_to_keep <- cols_to_keep[!cols_to_keep %in% c("NEW_CIPS", "ROWNUMBER", "CIPS_ID")]

dbExecute(con, paste0("CREATE TABLE APCS2 AS 
                        WITH APCS_DERIVED AS
                          (
                          SELECT ", str_c(cols_to_keep, collapse = ", "), ",
                          LAG(EPIEND, 1) OVER ", cips_grouping, " AS PREV_EPIEND,
                          LAG(DISDEST, 1) OVER ", cips_grouping, " AS PREV_DISDEST,
                          ROW_NUMBER() OVER ", cips_grouping, " AS ROWNUMBER
                          FROM APCS
                          ),
                       APCS_NEWCIPS AS
                          (
                          SELECT ", str_c(cols_to_keep, collapse = ", "), ", ROWNUMBER, CASE
                            WHEN EPISTART - PREV_EPIEND <= 3
                              AND (PREV_DISDEST IN (51, 52, 53) 
                                    OR ADMISORC IN (51, 52, 53)
                                    OR ADMIMETH IN (67, 81)) THEN 0
                            ELSE 1
                            END NEW_CIPS 
                          FROM APCS_DERIVED
                          ),
                       APCS_CIPSID1 AS
                          (
                          SELECT *, CASE
                            WHEN NEW_CIPS = 1 THEN ROWNUMBER
                            WHEN NEW_CIPS = 0 THEN 0
                          END CIPS_ID_TEMP
                           FROM APCS_NEWCIPS
                          )
                        
                        SELECT ", str_c(cols_to_keep, collapse = ", "), ", NEW_CIPS, CASE
                          WHEN NEW_CIPS = 1 THEN CIPS_ID_TEMP
                          WHEN NEW_CIPS = 0 THEN MAX(CIPS_ID_TEMP) OVER (PARTITION BY study_id 
                            ORDER BY ROWNUMBER ASC ROWS BETWEEN
                            UNBOUNDED PRECEDING AND CURRENT ROW)
                          END CIPS_ID
                        FROM APCS_CIPSID1
                       "))


dbRemoveTable(con, "APCS")
dbExecute(con, "ALTER TABLE APCS2 RENAME TO APCS")


first_spell_headers <- function(con) {
  headers <- dbListFields(con, "APCS") 
  headers <- headers[!headers %in% c("EPIEND", "DISDATE", "DISDEST", "DISMETH", "EPIKEY",
                                     "EPIDUR", "EPIORDER", "EPISTAT", "MAINSPEF", "SPELBGIN", "SPELDUR",
                                     "SPELEND", "TRETSPEF", "CONSULT_TYPE", "ENCRYPTED_HESID_MISSING",
                                     "PROCODE3_MISSING", "TRANSIT", "ADMIDATE_MISSING", "EPIDUR_CALC",
                                     "EPI_BAD", "EPI_VALID", "NEW_SPELL", "ROWCOUNT", "PROCODE3",
                                     "SUBDATE", "SUSCOREHRG", "SUSHRG", "SUSHRGVERS", "SUSRECID")] 
  return(str_c(headers, collapse = ", "))
} 


first_spell_query <- function(con) {
  paste("SELECT ", 
        first_spell_headers(con),
        ", PROCODE3 AS PROCODE3_FIRST_CIPS, SPELL_COUNT
        FROM
        (SELECT *, COUNT() OVER (PARTITION BY STUDY_ID, CIPS_ID) AS SPELL_COUNT FROM APCS)
        WHERE NEW_CIPS = 1")
}


last_spell_query <- "SELECT STUDY_ID, DISDEST, DISMETH, CIPS_ID, 
                        PROCODE3 AS PROCODE3_LAST_CIPS,
                        MAX_DISDATE AS DISDATE FROM
                       (SELECT *, MAX(DISDATE) OVER (PARTITION BY STUDY_ID, CIPS_ID) AS MAX_DISDATE FROM APCS)"


dbExecute(con, paste("CREATE TABLE APCC AS 
           SELECT * FROM 
           (", first_spell_query(con), ")
           LEFT JOIN
           (", last_spell_query, ")
           USING (STUDY_ID, CIPS_ID)"))

dbRemoveTable(con, "APC")
# dbRemoveTable(con, "APCS")


# # Count number of CIPS in following 90 days
# dbExecute(con, "CREATE TABLE APC AS
#           SELECT *,
#           approx_count_distinct(cips_id) over ninety AS ncips90d
#           FROM APCC
#               WINDOW ninety AS (
#               PARTITION BY study_id
#               ORDER BY admidate_filled ASC
#               RANGE BETWEEN INTERVAL 0 DAYS PRECEDING
#                        AND INTERVAL 90 DAYS FOLLOWING)
#           ORDER BY study_id, admidate_filled")

dbExecute(con, "ALTER TABLE APCC RENAME TO APC;")


# Count n distinct patients
dbGetQuery(con, "SELECT COUNT ( DISTINCT study_id ) AS n_pts FROM hes_table;")
dbGetQuery(con, "SELECT COUNT ( DISTINCT study_id ) AS n_pts FROM APC;")


# To consider in future:
  ## Coding an exact method for the number of CIPS in the first 90 days
    ## DISTINCT not supported with a window
    ## Subquery with dense rank -> Max dense rank --  How to implement with range?
  ## Number of days in hospital during first 90 days
    ## To add duration for each CIPS
    ## Then subtract: End of last CIPS - End of 90 day window (if end of last CIPS is later)



########################################## Inspect DuckDB Tables ##########################################

# log_info('Inspect tables')
# 
# # fyear and row_id should be a unique combination for all records
# # There are fewer records in hes_cohort because we have removed duplicates
dbGetQuery(con, "SELECT count() FROM hes_cohort")
dbGetQuery(con, "SELECT count() FROM charlson")
dbGetQuery(con, "SELECT count() FROM complications")
dbGetQuery(con, "SELECT count() FROM APC")
# 
# # The 'a' at the end is an 'alias' - every derived table must have it's own alias
# dbGetQuery(con, "SELECT count(*)
#            FROM
#             (SELECT DISTINCT fyear, row_id
#             FROM hes_cohort) a")
# 
# 
# dbGetQuery(con, "SELECT count(*)
#            FROM
#             (SELECT DISTINCT fyear, row_id
#             FROM charlson) a")
# 
# dbGetQuery(con, "SELECT count(*)
#            FROM
#             (SELECT DISTINCT fyear, row_id
#             FROM complications) a")
# 
# 
# dbGetQuery(con, "SELECT count(*)
#            FROM
#             (SELECT DISTINCT fyear, row_id
#             FROM spells) a")



########################################## Join DuckDB Tables ##########################################

log_info('Join HES tables')

# dbListFields(con, "hes_cohort")
# dbListFields(con, "charlson")
# dbListFields(con, "complications")
# dbListFields(con, "spells")

dbExecute(con, "CREATE TABLE hes_temp AS
                    WITH cte AS
                    (
                      SELECT hes_cohort.study_id, hes_cohort.row_id, hes_cohort.fyear, hes_cohort.dups, charlson.* EXCLUDE (study_id, row_id, fyear)
                      FROM hes_cohort
                      LEFT JOIN charlson
                      ON hes_cohort.row_id = charlson.row_id
                      AND hes_cohort.fyear = charlson.fyear
                    ),
                    
                    cte1 AS
                    (
                      SELECT cte.*, complications.* EXCLUDE (study_id, row_id, fyear)
                      FROM cte
                      LEFT JOIN complications
                      ON cte.row_id = complications.row_id
                      AND cte.fyear = complications.fyear
                    )
                    
                    SELECT cte1.*, APC.* EXCLUDE (study_id, row_id, fyear)
                      FROM cte1
                      LEFT JOIN APC
                      ON cte1.row_id = APC.row_id
                      AND cte1.fyear = APC.fyear;")



########################################## Add additional fields from hes_table ##########################################

# What outcome/control vars are needed from hes_table?

# Quality of linkage
# jr

# Control vars
# CCI
# cm5y1, cm5y2, cm5y3, cm5y4, cm5y5, cm5y6, cm5y7, cm5y8, cm5y9, cm5y10, cm5y11, cm5y12, cm5y13, cm5y14, cm5y15, cm5y16, cm5y17
# frailty5y1, frailty5y2, frailty5y3, frailty5y4,
# frailty5y5, frailty5y6, frailty5y7, frailty5y8, frailty5y9, frailty5y10, frailty5y11, frailty5y12, frailty5y13, frailty5y14, frailty5y15, frailty5y16, frailty5y17, frailty5y18, frailty5y19, frailty5y20, frailty5y21, frailty5y22, frailty5y23, frailty5y24, frailty5y25, frailty5y26, frailty5y27, frailty5y28, frailty5y29, frailty5y30, frailty5y31, frailty5y32, frailty5y33, frailty5y34

# IMD
# hes %>% group_by(IMD04_DECILE, FYEAR) %>% count() %>% collect()
# AVAILABLE:
# IMD04, IMD04_DECILE, IMD04RK,
# NEEDED:
# IMD04_DECILE,
# Ethnicity
# NEEDED:
# ETHNIC5,

# Outcomes
# Complications
# compacva90, complrti90, compmi90, compdvt90, comppe90, compaki90, computi90, compwound90, compssi90, compppf90, comppros90, compnvinj90, compbloodtx90,
# Remuneration
# SUSCOREHRG, SUSHRG, SUSHRGVERS

# Merge on the desired hes fields
# We could merge on all hes fields, but it would create a big table that is probably unnecessary
tic()
dbExecute(con, "CREATE TABLE hes_temp2 AS
                    SELECT hes_temp.*, hes_table.epikey, hes_table.IMD04_DECILE, hes_table.ETHNIC5, hes_table.jr, hes_table.SUSCOREHRG, hes_table.SUSHRG, hes_table.SUSHRGVERS, hes_table.CCG_RESIDENCE, hes_table.CCG_RESPONSIBILITY, hes_table.CCG_TREATMENT, hes_table.EPIDUR, hes_table.SPELDUR
                    FROM hes_temp
                    LEFT JOIN hes_table
                    ON hes_temp.row_id = hes_table.row_id
                    AND hes_temp.fyear = hes_table.fyear;")
toc()

tic()
dbExecute(con, ("CREATE TABLE hes_working AS
                        WITH cte AS
                        (
                          SELECT *,
                          row_number() OVER dupwindow AS duplicates,
                          FROM hes_temp2
                          WINDOW dupwindow AS (
                            PARTITION BY row_id, FYEAR
                              ORDER BY admidate_filled ASC
                              )
                          ORDER BY row_id, FYEAR, admidate_filled
                        )
                  
                  SELECT *
                  FROM cte;"))
toc()

# Create a LOS field
dbExecute(con, "ALTER TABLE hes_working ADD COLUMN los INTEGER;")
dbExecute(con, "UPDATE hes_working SET los = ABS(disdate - admidate_filled);")

## Set LOS <0 or >365 days as out of range
dbExecute(con, "UPDATE hes_working SET los = CASE
                  WHEN los<0 THEN NULL
                  WHEN los>365 THEN NULL
                  ELSE los
                  END;")

dbExecute(con, "DROP TABLE hes_temp")
dbExecute(con, "DROP TABLE hes_temp2")

log_info('Finished creating HES tables')



########################################## Shutdown DuckDB con ##########################################

# # Unregister virtual table
# duckdb_unregister(con, "hes_table")
# 
# # # Shutdown DuckDB
# dbDisconnect(con, shutdown=TRUE)
