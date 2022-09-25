********************************************************************************
***************                 Window functions                 ***************
********************************************************************************


clear
capture log close
log using "HES_APC_4_Window_Functions.log", replace


***** A window function looks forwards or backwards from an episode for a given patient

***** Examples include:
***** Identifying complications up to 90 days following an episode
***** Calculating a 5 year look-back for the Charlson Comorbidity Index
***** Calculating length of stay across for a provider spell or continuous inpatient spells

***** Since one may need to examine across more than one financial year, the datasets need to be appended together
***** For systems with very large amounts of memory (RAM), entire .dta files may be appended
***** Our system imports only the fields necessary for a given function to reduce memory requirements


local files: dir "." files "*dta"
tempfile building
clear
save `building', emptyok

foreach file of local files {
	di `"`file'"'
    use study_id epikey fyear row_number /// Fields to merge record back to original dataset
	provspno procode3 epistart epiend disdest admisorc admimeth admidate /// Fields for Spells & CIPS
	comp1 comp2 comp3 comp4 comp5 comp6 comp7 /// Fields for complications
	cm1 cm2 cm3 cm4 cm5 cm6 cm7 cm8 cm9 cm10 cm11 cm12 cm13 cm14 cm15 cm16 cm17 /// Fields for Charlson comorbidity index
	using `"`file'"', clear

    append using `building'
    save `"`building'"', replace
}


***** Calculate complications at up to 90-days following an episode
***** This requires the rangestat function
***** ssc install rangestat
rangestat (max) comp1_90 = comp1 ///
		  (max) comp2_90 = comp2 ///
		  (max) comp3_90 = comp3 ///
		  (max) comp4_90 = comp4 ///
		  (max) comp5_90 = comp5 ///
		  (max) comp6_90 = comp6 ///		  
		  (max) comp7_90 = comp7 ///
		  , interval(admidate, 0, 90) by(study_id) describe
		  

***** Calculate Charlson Comorbidity Index using SHMI weightings over a maximum 

  rangestat (max) CM1 = cm1 ///
			(max) CM2 = cm2 ///
			(max) CM3 = cm3 ///
			(max) CM4 = cm4 ///
			(max) CM5 = cm5 ///
			(max) CM6 = cm6 ///
			(max) CM7 = cm7 ///
			(max) CM8 = cm8 ///
			(max) CM9 = cm9 ///
			(max) CM10 = cm10 ///
			(max) CM11 = cm11 ///
			(max) CM12 = cm12 ///
			(max) CM13 = cm13 ///
			(max) CM14 = cm14 ///
			(max) CM15 = cm15 ///
			(max) CM16 = cm16 ///
			(max) CM17 = cm17 ///
		    , interval(admidate, -1826, 0) by(study_id) describe
		  
gen byte CM18 = cond(CM11 >0 & CM15 >0, 0, CM11)
gen CCI = (5*CM1) + (11*CM2) + (13*CM3) + (4*CM4) + (14*CM5) + (3*CM6) + (8*CM7) + (9*CM8) + (6*CM9) + (4*CM10) + (8*CM18) + (-1*CM12) + (1*CM13) + (10*CM14) + (14*CM15) + (18*CM16) + (2*CM17)
replace CCI =0 if CCI==-1


***** Construct Spells & CIPS
gsort study_id admidate
gen long spell_temp =_n
replace spell_temp = spell_temp[_n-1] if ///
						study_id == study_id[_n-1] &	///
						provspno == provspno[_n-1] &			///
						procode3 == procode3[_n-1] &			///
						admidate == admidate[_n-1]
										
egen spell = group(spell_temp)
drop spell_temp

gen long cips_temp =_n
replace cips_temp = cips_temp[_n-1] if ///
						spell ==  spell[_n-1] |	///
						(study_id == study_id[_n-1] &	///
						epistart <= (epiend[_n-1]+2) & ///
						((disdest[_n-1]>=51 & 			///
						disdest[_n-1]<=53) |			///
						(admisorc>=51 & admisorc<=53) |  ///
						admimeth==81))

egen cips = group(cips_temp)
drop cips_temp


***** Stata's compress function selects the smallest field type, there is no 'compression' in the traditional sense
compress
save hes_appended.dta, replace

capture log close
