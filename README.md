# National Joint Registry, Hospital Episode Statistics, NHS Patient Reported Outcome Measures, Civil Registrations (Mortality): Data cleaning and linkage workflows

## Aim

The aim of this project is to create an efficient and reproducible system to clean and link the National Joint Registry (NJR) to research datasets from NHS Digital, including:

- Hospital Episode Statistics Admitted Patient Care (HES APC)
- NHS Patient Reported Outcome Measures (PROMs)
- Civil Registrations (Mortality)


## Workflow design principles

- R-based for cleaning and data linkage
   - To process data in memory where possible using data.table
   - To avoid loops through sparse matrix multiplication (utilising the icd package)
   - For very large datasets (i.e. HES APC) to create an 'in-memory' dataset using arrow and then create a SQL database (using DuckDB)
- The final database can be exported to other statistical packages if desired 
- Note: The supplementary Stata code performs early data cleaning steps only, and is incomplete


## Instructions for use

.R scripts should be sourced in order:


`R NHSD 1 Set-up directories` 
- User to specify location of data and .R scripts.
- User to organise data in structure indicated, and create the required empty directories


`R NHSD 2 Clean HES`
- Imports each .txt file from NHS Digital
- Logs number of records
- Outputs clean .parquet files
- Cleaning includes
   - Coding of null values
   - Creating dummy variables
      - Charlson comorbidity index (SHMI variant)
      - Frailty index
      - Complications
      - Row quality
      - Joint replacement procedure type (i.e. primary or revision hip or knee replacement or none)


`R NHSD 3 Set schema for HES`
- Creates arrow schema for HES APC
- Opens HES APC as an 'in memory' dataset


`R NHSD 4 Create HES tables`
- Creates a new Duck DB database connection
- Window functions to identify:
  - Duplicates (based on study_id, procode3, epistart, epiend)
  - Maximum of the Charlson comorbidity index and Frailty index dummies for a given patient in the 5-years prior to each HES record for that patient
  - Complications (which includes serious medical and surgical complications) for a given patient in the 90 days following each record for that patient
  - Spells, CIPS
- Output: `hes_working` table in Duck DB con
- Notes 
   1. The spells/CIPS methodology is based on HES Pipeline R (translated from SQLite to Duck DB). This is an *expensive* process and may take many hours to run. If you get an out-of-memory error then split the queries rather than using CTEs.
   2. Those using HES APC for purposes other than the analysis of joint replacements may wish to remove/alter the QUALIFY statements.
   3. We currently use a probabilistic method for counting the number of CIPS in the 90 days following each procedure (due to the count distinct problem over a range) and will look to refine this in future.


`R NHSD 5 Create ONS table`
- Cleans Civil Registrations (Mortality) dataset
- Minimises cohort to patients with an NJR study_id
- Output: `ons` table in Duck DB con


`R NHSD 6 Create PROMS table`
- Cleans NHS PROMs dataset and creates derived variables
- Output: `proms` table in Duck DB con
   - Note: `mic01` field is based on >=6 points (i.e. appropriate for rHR & rKR) - new fields need to be derived for pHR (>=8 points) and pKR (>=7 points)


`R NHSD 7 Create NJR tables`
- Cleans primary and revision knee replacement procedure records on the NJR
- Outputs (tables in Duck DB con):
   - `pk` = primary knee replacement procedures
   - `first_rk` = first-linked revision knee replacement procedures
   - `all_rk` = all revision knee replacement procedures


`R NHSD 8 first-rk-ons-hes-proms`
- LEFT JOIN first_rk-ons on study_id to create `rk_ons` table
- LEFT JOIN rk_ons to hes on study_id AND admidate/date of surgery to create `rk_ons_hes` table
   - This is a range join with a tolerance +/- 7 days
   - Records are sorted by date-difference and the first match is accepted
- LEFT JOIN rk_ons_hes to proms on epikey and admidate/date of PROM to create `rk_ons_hes_proms` table
   - The preferred method of joining NJR-PROMs directly using study_id was not available to us in this extract
- Derive variables
- Create data for attrition flowchart


`R NHSD 9 Analyse rKA outcomes`
- Tables and figures for the analysis of multimodal outcomes following joint replacement, including:
   - Implant survivorsip
   - Mortality up to 90 days
   - Serious medical complications requiring hospital admission up to 90 days
   - Length of stay (as CIPS)
   - PROMS

`R NHSD 10 pk-ons-hes-proms`
- Equivalent to `R NHSD 8 first-rk-ons-hes-proms`, but for primary knee replacement


`R NHSD 11 Example queries`
- Opening a read only database connection
- Querying Duck DB using SQL
- Querying the 'in-memory' HES dataset using arrow and dplyr
- Writing a new table to the Duck DB con (e.g. to add fields to HES)


## Useful resources

- [National Joint Registry](https://www.njrcentre.org.uk/)
- [Hospital Episode Statistics (HES)](https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/hospital-episode-statistics)
- [HES Pipeline R](https://github.com/HFAnalyticsLab/HES_pipeline)
- [Analysing Patient-Level Data using Hospital Episode Statistics (HES)](https://www.york.ac.uk/che/courses/patient-data/)
