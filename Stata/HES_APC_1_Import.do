********************************************************************************
***************             Import HES APC text files            ***************
********************************************************************************

***** Specify directory of HES APC text files
cd "A:\Working\HES"


***** Log import of text files
capture log close
log using "HES_APC_1_Import.log", replace


***** Convert all text files in directory to Stata .dta files
local fls : dir . files "*.txt"
foreach f in `fls' {
    insheet using "`f'", delim("|") names clear
    local f = subinstr("`f'",".txt",".dta",.)
    save "`f'", replace
}


***** Inspect text files
***** The Stata function 'precombine' will generate a report of incompatible field types
cd "A:\Working\HES"
fs "*.dta"
precombine `r(files)'


***** Incompatible field types between files should be converted to string
***** In our dataset, the following fields needed to be converted:

clear
local filenames: dir "." files "*.dta"

foreach f of local filenames {
    use `"`f'"', clear
tostring acpstar_1, replace
tostring acpstar_2, replace
tostring acpstar_3, replace
tostring acpstar_4, replace
tostring acpstar_5, replace
tostring acpstar_6, replace
tostring acpstar_7, replace
tostring acpstar_8, replace
tostring acpstar_9, replace
tostring admimeth, replace
tostring ccg_residence, replace
tostring ccg_responsibility, replace
tostring ccg_treatment, replace
tostring diag_08, replace
tostring diag_09, replace
tostring diag_10, replace
tostring diag_11, replace
tostring diag_12, replace
tostring diag_13, replace
tostring diag_14, replace
tostring diag_15, replace
tostring diag_16, replace
tostring diag_17, replace
tostring diag_18, replace
tostring diag_19, replace
tostring diag_20, replace
tostring domproc, replace
tostring firstreg, replace
tostring hrgnhs, replace
tostring hrgnhsvn, replace
tostring lsoa11, replace
tostring opdate_05, replace
tostring opdate_06, replace
tostring opdate_07, replace
tostring opdate_08, replace
tostring opdate_09, replace
tostring opdate_10, replace
tostring opdate_11, replace
tostring opdate_12, replace
tostring opdate_13, replace
tostring opdate_14, replace
tostring opdate_15, replace
tostring opdate_16, replace
tostring opdate_17, replace
tostring opdate_18, replace
tostring opdate_19, replace
tostring opdate_20, replace
tostring opdate_21, replace
tostring opdate_22, replace
tostring opdate_23, replace
tostring opdate_24, replace
tostring operstat, replace
tostring opertn_05, replace
tostring opertn_06, replace
tostring opertn_07, replace
tostring opertn_08, replace
tostring opertn_09, replace
tostring opertn_10, replace
tostring opertn_11, replace
tostring opertn_12, replace
tostring opertn_13, replace
tostring opertn_14, replace
tostring opertn_15, replace
tostring opertn_16, replace
tostring opertn_17, replace
tostring opertn_18, replace
tostring opertn_19, replace
tostring opertn_20, replace
tostring opertn_21, replace
tostring opertn_22, replace
tostring opertn_23, replace
tostring opertn_24, replace
tostring procodet, replace
tostring protype, replace
tostring resstha06, replace
tostring sthatret, replace
tostring suscorehrg, replace
tostring sushrg, replace
    save `"`f'"', replace
}

capture log close
