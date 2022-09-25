********************************************************************************
***************              Create dummy variables              ***************
********************************************************************************

clear

capture log close
log using "HES_APC_3_Dummy_Variables.log", replace

local filenames: dir "." files "*.dta"

foreach f of local filenames {
    use `"`f'"', clear

	
***** Row quality
***** The 'rq' dummy variables tag each record with a '0' (high quality) or '1' (low quality)
***** These variables will be used later to prioritise deletion of lower quality duplicate records

forval i = 1/14 {
	gen rq`i'=1
}

replace rq1 = 0 if admimeth !="98" & admimeth !="99" & admimeth !=""
replace rq2 = 0 if admisorc !=98 & admisorc !=99 & admisorc !=.
replace rq3 = 0 if disdest !=98 & disdest !=99 & disdest !=.
replace rq4 = 0 if startage !=. & startage >18 & startage <105
replace rq5 = 0 if admidate !=.
replace rq6 = 0 if disdate !=.
replace rq7 = 0 if epistart !=.
replace rq8 = 0 if epiend !=.
replace rq9 = 0 if mainspef =="110" | tretspef =="110"
replace rq10 = 0 if opertn_1 !=""
replace rq11 = 0 if diag_1 !=""
replace rq12 = 0 if diag_2 !=""
replace rq13 = 0 if diag_3 !=""
replace rq14 = 0 if opdate_1 !=.

egen rq = rowtotal(rq1 rq2 rq3 rq4 rq5 rq6 rq7 rq8 rq9 rq10 rq11 rq12 rq13 rq14)
drop rq1 rq2 rq3 rq4 rq5 rq6 rq7 rq8 rq9 rq10 rq11 rq12 rq13 rq14


***** Charlson comorbidity index
***** Create binary variables for each of the seventeen comorbidity groups
forval i = 1/17{
gen byte cm`i'= 0
}

***** Define local macros for each comorbidity group
***** Note that the inlist() function only accepts nine arguments
***** As such, some comorbidities are split into multiple local macros
local ami  `""I21","I22","I23""'
local ami1  `""I252","I258""'
local cva  `""G450","G451","G452","G454","G458","G459""'
local cva1  `""G46","I60","I61""'
local cva2  `""I62","I63","I64","I65","I66","I67","I68","I69""'
local chf  `""I50""'
local ctd  `""M05","M32","M34""'
local ctd1  `""M060","M063","M069","M332","M353""'
local dem  `""F00","F01","F02","F03""'
local dem1  `""F051""'
local dia  `""E101","E105","E106","E108","E109","E111","E115","E116","E118""'
local dia1  `""E119","E131","E135","E136","E138","E139","E141","E145","E146""'
local dia2  `""E148","E149""'
local ldi  `""K702","K703","K717""'
local ldi1  `""K73","K74""'
local pud  `""K25","K26","K27","K28""'
local pvd  `""I71","R02""'
local pvd1  `""I739","I790","Z958","Z959""'
local pdi  `""J40","J41","J42","J43","J44","J45","J46","J47","J60""'
local pdi1  `""J61","J62","J63","J64","J65","J66","J67""'
local can  `""C00","C01","C02","C03","C04","C05","C06","C07","C08""'
local can1  `""C09","C10","C11","C12","C13","C14","C15","C16","C17""'
local can2  `""C18","C19","C20","C21","C22","C23","C24","C25","C26""'
local can3  `""C27","C28","C29","C30","C31","C32","C33","C34","C35""'
local can4  `""C36","C37","C38","C39","C40","C41","C42","C43","C44""'
local can5  `""C45","C46","C47","C48","C49","C50","C51","C52","C53""'
local can6  `""C54","C55","C56","C57","C58","C59","C60","C61","C62""'
local can7  `""C63","C64","C65","C66","C67","C68","C69","C70","C71""'
local can8  `""C72","C73","C74","C75","C76","C81","C82","C83","C84""'
local can9  `""C85","C86","C87","C88","C89","C90","C91","C92","C93""'
local can10  `""C94","C95","C96","C97""'
local dco  `""E102","E103","E104","E107","E112","E113","E114","E117","E132""'
local dco1  `""E133","E134","E137","E142","E143","E144","E147""'
local par  `""G041","G820","G821","G822""'
local par1  `""G81""'
local rdi  `""I12","I13","N01","N03","N18","N19","N25""'
local rdi1  `""N052","N053","N054","N055","N056","N072","N073","N074""'
local mcan  `""C77","C78","C79","C80""'
local sld  `""K721","K729","K766","K767""'
local hiv  `""B20","B21","B22","B23","B24""'
local hiv1  `""O987""'

***** Loop over diagnosis fields to identify and tag comorbidities
***** Note: The SHMI guidance specifies that fields diag_2 to diag_20 should be searched. However, we have also included diag_1.
foreach var of varlist diag_1 - diag_20 {
replace cm1 = 1 if cm1 ==0 & inlist(substr(`var',1,3), `ami') | inlist(`var', `ami1')
replace cm2 = 1 if cm2 ==0 & inlist(substr(`var',1,3), `cva1') | inlist(substr(`var',1,3), `cva2')| inlist(`var', `cva')
replace cm3 = 1 if cm3 ==0 & substr(`var',1,3)=="I50"
replace cm4 = 1 if cm4 ==0 & inlist(substr(`var',1,3), `ctd') |inlist(`var', `ctd1')
replace cm5 = 1 if cm5 ==0 & inlist(substr(`var',1,3), `dem') | `var'=="F051"
replace cm6 = 1 if cm6 ==0 & inlist(`var', `dia')| inlist(`var', `dia1')| inlist(`var', `dia2')
replace cm7 = 1 if cm7 ==0 & inlist(substr(`var',1,3), `ldi1') |inlist(`var', `ldi')
replace cm8 = 1 if cm8 ==0 & inlist(substr(`var',1,3), `pud')
replace cm9 = 1 if cm9 ==0 & inlist(substr(`var',1,3), `pvd') | inlist(`var', `pvd1')
replace cm10 = 1 if cm10 ==0 & inlist(substr(`var',1,3), `pdi') |inlist(substr(`var',1,3), `pdi1')
replace cm11 = 1 if cm11 ==0 & inlist(substr(`var',1,3), `can') |inlist(substr(`var',1,3), `can1')|inlist(substr(`var',1,3), `can2')|inlist(substr(`var',1,3), `can3')|inlist(substr(`var',1,3), `can4')|inlist(substr(`var',1,3), `can5')|inlist(substr(`var',1,3), `can6')|inlist(substr(`var',1,3), `can7')|inlist(substr(`var',1,3), `can8')|inlist(substr(`var',1,3), `can9')|inlist(substr(`var',1,3), `can10')
replace cm12 = 1 if cm12 ==0 & inlist(`var', `dco')|inlist(`var', `dco1')
replace cm13 = 1 if cm13 ==0 & substr(`var',1,3)=="G81"|inlist(`var', `par')
replace cm14 = 1 if cm14 ==0 & inlist(substr(`var',1,3), `rdi') | inlist(`var', `rdi1')
replace cm15 = 1 if cm15 ==0 & inlist(substr(`var',1,3), `mcan')
replace cm16 = 1 if cm16 ==0 & inlist(`var', `sld')
replace cm17 = 1 if cm17 ==0 & inlist(substr(`var',1,3), `hiv') | `var'=="O987"
}


***** Complications
***** Create dummy variables to code medical complications from surgery
forval i = 1/7 {
	generate byte comp`i'=.
}

***** Define local macros to identify complications
local acutemi  `""I21""'
local acutemi1  `" "I210",	"I211",	"I212",	"I213",	"I214",	"I219",	"I220",	"I221" "'
local acutemi2  `""I228", "I229" "'
local dvt `" "I801", "I802", "I803", "I808", "I809", "I828", "I829", "I824" "'
local pe `" "I822",	"I823",	"I260",	"I269" "'
local lrti `" "J12",	"J13",	"J14",	"J15",	"J16",	"J18",	"J20",	"J22",	"J86" "'
local lrti1 `" "J440",	"J441",	"J851",	"J852",	"J690" "'
local acva `" "I60",	"I61",	"I62",	"I63" "'
local acva1 `" "I610",	"I611",	"I612",	"I613",	"I614",	"I615",	"I616",	"I618",	"I619" "'
local acva2 `" "I630",	"I631",	"I632",	"I633",	"I634",	"I635",	"I636",	"I638",	"I639" "'
local aki `" "N170", "N171", "N172", "N178", "N179" "'
local uti `" "N300", "N308", "N309", "N390" "'

***** Loop over diagnosis fields to identify complications during an episode
***** comp1 = Acute myocardial infarction
***** comp2 = Deep vein thrombosis
***** comp3 = Pulmonary embolism
***** comp4 = Lower respiratory tract infection
***** comp5 = Acute cerebrovascular event
***** comp6 = Acute kidney injury
***** comp7 = Urinary tract infection

foreach var of varlist diag_1 - diag_20 {
replace comp1=1 if comp1==0 & substr(`var',1,3)=="I21"|inlist(`var', `acutemi1')|inlist(`var', `acutemi2')
replace comp2=1 if comp2==0 & inlist(`var', `dvt')
replace comp3=1 if comp3==0 & inlist(`var', `pe')
replace comp4=1 if comp4==0 & inlist(`var', `lrti1')|inlist(substr(`var',1,3), `lrti')
replace comp5=1 if comp5==0 & inlist(substr(`var',1,3), `acva')|inlist(`var', `acva1')|inlist(`var', `acva2')
replace comp6=1 if comp6==0 & inlist(`var', `aki')
replace comp7=1 if comp7==0 & inlist(`var', `uti')
}

***** Hip or knee replacement episodes
***** Create dummy variables
gen jr=0
gen hproc=0
gen kproc=0
gen hbp=0
gen kbp=0
gen actcode=0
gen side = 0
gen phrs = 0
gen rhrs = 0
gen pkrs = 0
gen rkrs = 0

***** Define local macros for:
***** Action codes
***** Body part codes
***** Procedure codes

local act `""Y032 ","Y037""'
local hbp `""Z756","Z761","Z843""'
local kbp `""Z765","Z774","Z787","Z844","Z845","Z846""'
local phr `""W371","W378","W379","W381","W388","W389","W391","W398","W399""'
local phr1 `""W931","W938","W939","W941","W948","W949","W951","W958","W959""'
local phr2 `""W461","W468","W469","W471","W478","W479","W481","W488","W489""'
local phrbp `""W521","W531","W541","W581""'
local phrbp1 `""W528","W529","W538","W539","W548","W549""'
local pkr `""O181","O188","O189","W401","W408","W409","W411","W418","W419""'
local pkr1 `""W421","W428","W429","W528","W529","W538","W539","W548","W549""'
local pkrbp `""W521 ","W531 ","W541","W581""'
local rhr `""W370","W372","W373","W374","W380","W382","W383","W384","W390""'
local rhr1 `""W392","W393","W395","W462","W472","W482","W930","W932","W933""'
local rhr2 `""W940","W942","W943","W950","W952","W953","W954""'
local rhr3 `""W460","W463","W470","W473","W480","W483","W484""'
local rhrbp `""W522 ","W523","W532","W533","W542","W543","W572","W574","W582""'
local rhrbp1 `""W520","W530","W540""'
local rhrbpa `""W394","W544""'
local rkr `""O180","O182","O183","O184","W400","W402","W403","W404","W410""'
local rkr1 `""W412","W413","W414","W420","W422","W423","W425""'
local rkrbp `""W522","W523","W532","W533","W542","W543","W553","W564","W574""'
local rkrbp1 `""W582","W603","W613","W641","W642","W520","W530","W540""'
local rkrbpa `""W424","W544""'

***** Loop over operation fields and tag episodes
foreach var of varlist opertn_1 - opertn_24 {

replace actcode = 1 if inlist(`var', `act')
replace hbp = 1 if inlist(`var', `hbp')
replace kbp = 1 if inlist(`var', `kbp')

replace jr = 1 if inlist(`var', `phr') | inlist(`var', `phr1')| inlist(`var', `phr2')
replace hproc = 1 if inlist(`var', `phrbp')| inlist(`var', `phrbp1')
replace jr = 1 if hproc==1 & hbp==1 

replace jr = 2 if inlist(`var', `rhr') | inlist(`var', `rhr1') | inlist(`var', `rhr2')| inlist(`var', `rhr3')
replace hproc = 2 if inlist(`var', `rhrbp')| inlist(`var', `rhrbp1')
replace jr = 2 if hproc==2 & hbp==1 
replace hproc =3 if inlist(`var', `rhrbpa')
replace jr = 2 if hproc==3 & hbp==1 & actcode==1

replace jr = 3 if inlist(`var', `pkr') | inlist(`var', `pkr1')
replace kproc = 1 if inlist(`var', `pkrbp')
replace jr = 3 if kproc==1 & kbp==1

replace jr = 4 if inlist(`var', `rkr') | inlist(`var', `rkr1')
replace kproc = 2 if inlist(`var', `rkrbp')| inlist(`var', `rkrbp1')
replace jr = 4 if kproc==2 & kbp==1 
replace kproc =3 if inlist(`var', `rkrbpa')
replace jr = 4 if kproc==3 & kbp==1 & actcode==1

}

***** Label the field 'jr' with the type of procedure
label define jr_lbl ///
1 "Primary Hip Replacement" ///
2 "Revision Hip Replacement" ///
3 "Primary Knee Replacement" ///
4 "Revision Knee Replacement" ///
0 ""
label values jr jr_lbl

***** Label the field 'side' with the laterality of the procedure
foreach var of varlist opertn_1 - opertn_22 {
replace side = 1 if `var'=="Z941"
replace side = 2 if `var'=="Z942"
replace side = 3 if `var'=="Z943"
replace side = 4 if `var'=="Z944"
replace side = 8 if `var'=="Z948"
replace side = 9 if `var'=="Z949"
replace side = 7 if `var'=="Z94"
}

label define side_lbl ///
1 "Bilateral" ///
2 "Right" ///
3 "Left" ///
4 "Unilateral" ///
8 "Specified laterality NEC" ///
9 "Laterality NEC" ///
7 "Laterality of operation" ///
0 ""
label values side side_lbl

drop phrs rhrs pkrs rkrs hproc kproc hbp kbp actcode

***** Tag row_number for future merge procedures
gsort epikey admidate
gen row_number = _n

***** Save file
save `"`f'"', replace

}

capture log close
