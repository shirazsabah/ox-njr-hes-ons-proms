********************************************************************************
***************             Clean HES APC .dta files             ***************
********************************************************************************

clear

capture log close
log using "HES_APC_2_Clean_fields.log", replace

local filenames: dir "." files "*.dta"

foreach f of local filenames {
    use `"`f'"', clear

	
***** Relabel diagnosis and operation fields
***** This step is not strictly required, but we preferred fieldnames without the leading zero

foreach num of numlist 1 2 3 4 5 6 7 8 9 {
rename diag_0`num' diag_`num'
rename opertn_0`num' opertn_`num'
rename opdate_0`num' opdate_`num'
}


***** Remove "&" or "-" or "." from diagnosis and operation fields
***** Note that "&" == true missing, "-" == no operation performed.

foreach var of varlist diag_1 - diag_20 opertn_1 - opertn_24 {
	rename `var' temp
	gen `var' = substr(temp,1,4)
	replace `var' = "" if `var'=="-" 
	replace `var' = "" if `var'=="&" 
	replace `var' = "" if `var'=="." 
	drop temp
}


***** Convert dates to Stata data format
***** Recode missing and invalid dates
***** 2012-13 onwards: 01/01/1800 - Null date submitted; 01/01/1801 - Invalid date submitted
***** 1989/90 to 2011/12: 01/01/1600 – Null date submitted; 15/10/1582 – Invalid date submitted

foreach var of varlist admidate disdate elecdate epiend epistart opdate_1 - opdate_24 {
		rename `var' temp
		gen `var' = mdy(real(substr(temp, 6,2)), real(substr(temp, 9,2)), real(substr(temp, 1,4)))
		format `var' %td
		replace `var' = . if `var' == td(01jan1800) | ///
							 `var' == td(01jan1801) | ///
							 `var' == td(01jan1600) | ///
							 `var' == td(15oct1582)
		drop temp
}


***** Save file
save `"`f'"', replace

}

capture log close
