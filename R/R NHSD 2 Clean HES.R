# R NHSD 2 Clean individual HES APC text files


########################################## Overview ##########################################

# Import each HES .txt file
# Create dummy variables for:
### Charlson comorbidity index
### Frailty
### Complications
### Duplicates
### Row quality - including an overall score (lower better)
### Procedures of interest (hip/knee replacement)
# Save as parquet files



########################################## Packages ##########################################

#p_unload(all)
pacman::p_load(pacman, data.table, arrow, dplyr, stringi, stringr, tictoc, logger, icd)

# Set threads
getDTthreads()
setDTthreads(10)

# Set directory to data and log file
setwd(data_dir)

log_info('Start of script')



########################################## Supporting functions and vectors ##########################################

# Function to create numbered headers
generate_numbered_headers <- function(string, n) {
  return(c(str_c(string, "0", 1:9), str_c(string, 10:n)))
}

# Vector of diagnosis columns
diagcols <- generate_numbered_headers("DIAG_", 20)

# Vector of procedure columns
proccols <- generate_numbered_headers("OPERTN_", 24)

# List of diagnosis codes to tag
char_frail_comps_codes <- list(
  # Charlson
  # https://files.digital.nhs.uk/E9/B3AB48/SHMI%20specification%20v1.45.pdf
  ami = c("I21", "I22", "I23", "I252", "I258"),
  cva = c("G450", "G451", "G452", "G454", "G458", "G459", "G46", "I60", "I61", "I62", "I63", "I64", "I65", "I66", "I67", "I68", "I69"),
  chf = c("I50"),
  ctd = c("M05", "M060", "M063", "M069", "M32", "M332", "M34", "M353"),
  dem = c("F00", "F01", "F02", "F03", "F051"),
  dia = c("E101", "E105", "E106", "E108", "E109", "E111", "E115", "E116", "E118", "E119", "E131", "E135", "E136", "E138", "E139", "E141", "E145", "E146", "E148", "E149"),
  ldi = c("K702","K703","K717","K73","K74"),
  pud = c("K25","K26","K27","K28"),
  pvd = c("I71", "I739", "I790", "R02", "Z958", "Z959"),
  pdi = c("J40", "J41", "J42", "J43", "J44", "J45", "J46", "J47", "J60", "J61", "J62", "J63", "J64", "J65", "J66", "J67"),
  can = c("C00", "C01", "C02", "C03", "C04", "C05", "C06", "C07", "C08", "C09", "C10", "C11", "C12", "C13", "C14", "C15", "C16", "C17", "C18", "C19", "C20", "C21", "C22", "C23", "C24", "C25", "C26", "C30", "C31", "C32", "C33", "C34", "C37", "C38", "C39", "C40", "C41", "C43", "C45", "C46", "C47", "C48", "C49", "C50", "C51", "C52", "C53", "C54", "C55", "C56", "C57", "C58", "C60", "C61", "C62", "C63", "C64", "C65", "C66", "C67", "C68", "C69", "C70", "C71", "C72", "C73", "C74", "C75", "C76", "C81", "C82", "C83", "C84", "C85", "C86", "C88", "C90", "C91", "C92", "C93", "C94", "C95", "C96", "C97"),
  dco = c("E102", "E103", "E104", "E107", "E112", "E113", "E114", "E117", "E132", "E133", "E134", "E137", "E142", "E143", "E144", "E147"),
  par = c("G041", "G81", "G820", "G821", "G822"),
  rdi = c("I12", "I13", "N01", "N03", "N052", "N053", "N054", "N055", "N056", "N072", "N073", "N074", "N18", "N19", "N25"),
  mcan = c("C77", "C78", "C79", "C80"),
  sld = c("K721", "K729", "K766", "K767"),
  hiv = c("B20", "B21", "B22", "B23", "B24", "O987"),
  # Frailty
  anaem = c("C92", "C920", "D46", "D460", "D461", "D462", "D463", "D464", "D467", "D469", "D50", "D500", "D501", "D508", "D509", "D51", "D510", "D511", "D512", "D513", "D518", "D519", "D52", "D520", "D521", "D528", "D529", "D53", "D530", "D531", "D532", "D538", "D539", "D55", "D550", "D551", "D552", "D553", "D558", "D559", "D56", "D560", "D561", "D562", "D563", "D564", "D568", "D569", "D57", "D570", "D571", "D572", "D573", "D578", "D58", "D580", "D581", "D582", "D588", "D589", "D59", "D590", "D591", "D592", "D593", "D594", "D595", "D596", "D598", "D60", "D600", "D601", "D608", "D609", "D61", "D610", "D611", "D612", "D613", "D618", "D619", "D62", "D63", "D630", "D638", "D64", "D640", "D641", "D642", "D643", "D644", "D648", "D649", "D65", "E538", "K732"),
  arthr = c("M00", "M000", "M001", "M002", "M008", "M009", "M01", "M010", "M011", "M012", "M013", "M014", "M015", "M016", "M018", "M02", "M020", "M021", "M022", "M023", "M028", "M029", "M03", "M030", "M031", "M032", "M036", "M05", "M050", "M051", "M052", "M053", "M058", "M059", "M06", "M060", "M061", "M062", "M063", "M064", "M068", "M069", "M07", "M070", "M071", "M072", "M073", "M074", "M075", "M076", "M08", "M080", "M081", "M082", "M083", "M084", "M088", "M089", "M09", "M090", "M091", "M092", "M098", "M10", "M100", "M101", "M102", "M103", "M104", "M109", "M11", "M110", "M111", "M112", "M118", "M119", "M12", "M120", "M121", "M122", "M123", "M124", "M125", "M128", "M13", "M130", "M131", "M138", "M139","M14", "M140", "M141", "M143", "M144", "M145", "M146", "M148", "M15", "M150", "M151", "M152", "M153", "M154", "M158", "M159", "M16", "M160", "M161", "M162", "M163", "M164", "M165", "M166", "M167", "M169", "M17", "M170", "M171", "M172", "M173", "M174", "M175", "M179", "M18", "M180", "M181", "M182", "M183", "M184", "M185", "M189", "M19", "M190", "M191", "M192", "M198", "M199", "M20", "M200", "M201", "M202", "M203", "M204", "M205", "M206", "M21", "M210", "M211", "M212", "M213", "M214", "M215", "M216", "M217", "M218", "M219", "M22", "M220", "M221", "M222", "M223", "M224", "M228", "M229", "M23", "M230", "M231", "M232", "M233", "M234", "M235", "M236", "M238", "M239", "M24", "M240", "M241", "M242", "M243", "M244", "M245", "M246", "M247", "M248", "M249", "M25", "M250", "M251", "M252", "M253", "M254", "M255", "M256", "M257", "M258", "M259", "M42", "M420", "M421", "M429", "M45", "M450", "M451", "M452", "M453", "M454", "M455", "M456", "M457", "M458", "M459", "M46", "M460", "M461", "M462", "M463", "M464", "M465", "M468", "M469", "M47", "M470", "M471", "M472", "M478", "M479", "M48", "M480", "M481", "M482", "M483", "M488", "M489", "M48", "M772", "Z966", "W159"),
  atria = c("I48", "I480", "I481", "I482", "I483", "I484", "I489"),
  cereb = c("G45", "G450", "G451", "G452", "G453", "G454", "G458", "G459", "G46", "G460", "G461", "G462", "G463", "G464", "G465", "G466", "G467", "G468", "I60", "I600", "I601", "I602", "I603", "I604", "I605", "I606", "I607", "I608", "I609", "I61", "I610", "I611", "I612", "I613", "I614", "I615", "I616", "I617", "I618", "I619", "I62", "I620", "I621", "I629", "I63", "I630", "I631", "I632", "I633", "I634", "I635", "I636", "I637", "I638", "I639", "I64", "I65", "I650", "I651", "I652", "I653", "I658", "I659", "I66", "I660", "I661", "I662", "I663", "I664", "I668", "I669", "I67", "I670", "I671", "I672", "I673", "I674", "I675", "I676", "I677", "I678", "I679", "I68", "I680", "I681", "I688", "I69", "I690", "I691", "I692", "I693", "I694", "I698", "S065", "S066", "S068"),
  chron = c("E102", "E107", "E112", "E117", "E132", "E137", "E142", "E147", "N083", "N18", "N183", "N184", "N185", "N188", "N189", "Q61", "Q610", "Q611", "Q612", "Q613", "Q614", "Q615", "Q618", "Q619", "R80"),
  diabe = c("E10", "E100", "E101", "E102", "E103", "E104", "E105", "E106", "E107", "E108", "E109", "E11", "E110", "E111", "E112", "E113", "E114", "E115", "E116", "E117", "E118", "E119", "E12", "E120", "E121", "E122", "E123", "E124", "E125", "E126", "E127", "E128", "E129", "E13", "E130", "E131", "E132", "E133", "E134", "E135", "E136", "E137", "E138", "E139", "E14", "E140", "E141", "E142", "E143", "E144", "E145", "E146", "E147", "E148", "E149", "G590", "G632", "G730", "G990", "H280", "H360", "M142", "M146", "N083"),
  dizzi = c("H81", "H810", "H811", "H812", "H813", "H814", "H818", "H819", "H82", "R42"),
  dyspn = c("R06"),
  falls = c("W00", "W000", "W001", "W002", "W003", "W004", "W005", "W006", "W007", "W008", "W009", "W01", "W010", "W011", "W012", "W013", "W014", "W015", "W016", "W017", "W018", "W019", "W02", "W020", "W021", "W022", "W023", "W024", "W025", "W026", "W027", "W028", "W029", "W03", "W030", "W031", "W032", "W033", "W034", "W035", "W036", "W037", "W038", "W039", "W04", "W040", "W041", "W042", "W043", "W044", "W045", "W046", "W047", "W048", "W049", "W05", "W050", "W051", "W052", "W053", "W054", "W055", "W056", "W057", "W058", "W059", "W06", "W060", "W061", "W062", "W063", "W064", "W065", "W066", "W067", "W068", "W069", "W07", "W070", "W071", "W072", "W073", "W074", "W075", "W076", "W077", "W078", "W079", "W08", "W080", "W081", "W082", "W083", "W084", "W085", "W086", "W087", "W088", "W089", "W09", "W090", "W091", "W092", "W093", "W094", "W095", "W096", "W097", "W098", "W099", "W10", "W100", "W101", "W102", "W103", "W104", "W105", "W106", "W107", "W108", "W109", "W11", "W110", "W111", "W112", "W113", "W114", "W115", "W116", "W117", "W118","W119", "W11", "W110", "W111", "W112", "W113", "W114", "W115", "W116", "W117", "W118", "W119", "W12", "W120", "W121", "W122", "W123", "W124", "W125", "W126", "W127", "W128", "W129", "W13", "W130", "W131", "W132", "W133", "W134", "W135", "W136", "W137", "W138", "W139", "W14", "W140", "W141", "W142", "W143", "W144", "W145", "W146", "W147", "W148", "W149", "W15", "W150", "W151", "W152", "W153", "W154", "W155", "W156", "W157", "W158", "W159", "W16", "W160", "W161", "W162", "W163", "W164", "W165", "W166", "W167", "W168", "W169", "W17", "W170", "W171", "W172", "W173", "W174", "W175", "W176", "W177", "W178", "W179", "W18", "W180", "W181", "W182", "W183", "W184", "W185", "W186", "W187", "W188", "W189", "W19", "W190", "W191", "W192", "W193", "W194", "W195", "W196", "W197", "W198", "W199", "R268", "R296", "R54"),
  footp = c("L84"),
  fragi = c("M484", "M485", "M495", "M800", "M801", "M802", "M803", "M804", "M805", "M808", "M809", "M844", "S12", "S120", "S121", "S122", "S127", "S129", "S22", "S220", "S221", "S222", "S223", "S224", "S225", "S228", "S229", "S32", "S320", "S321", "S322", "S323", "S324", "S325", "S327", "S328", "S42", "S420", "S421", "S422", "S423", "S424", "S427", "S428", "S429", "S52", "S520", "S521", "S522", "S523", "S524", "S525", "S526", "S527", "S528", "S529", "S62", "S620", "S621", "S622", "S623", "S624", "S625", "S626", "S627", "S628", "S629", "S72", "S720", "S721", "S722", "S723", "S724", "S727", "S728", "S729", "S82", "S820", "S821", "S822", "S823", "S824", "S825", "S826", "S827", "S828", "S829", "S92", "S920", "S921", "S922", "S923", "S924", "S925", "S927", "S928", "S929", "T08", "T080", "T081", "T10", "T100", "T101", "T12"),
  heari = c("F446", "H90", "H900", "H901", "H902", "H903", "H904", "H905", "H906", "H907", "H908", "H909", "H91", "H910", "H911", "H912", "H913", "H914", "H915", "H916", "H917", "H918", "H919", "Z461", "Z962", "Z974"),
  heafa = c("I110", "I130", "I132", "I50", "I500", "I501", "I509", "I971"),
  heava = c("I05", "I050", "I051", "I052", "I058", "I059", "I06", "I060", "I061", "I062", "I068", "I069", "I07", "I070", "I071", "I072", "I078", "I079", "I08", "I080", "I081", "I082", "I088", "I089", "I099", "I34", "I340", "I341", "I342", "I348", "I349", "I35", "I350", "I351", "I352", "I358", "I359", "I36", "I360", "I361", "I362", "I368", "I369", "I37", "I370", "I371", "I372", "I378", "I379", "I38", "I39", "I390", "I391", "I392", "I393", "I394", "I398"),
  house = c("Z593", "Z636"),
  hyper = c("I10", "I11", "I110", "I119", "I12", "I120", "I129", "I13", "I130", "I131", "I132", "I139", "I15", "I150", "I151", "I152", "I158", "I159", "I708"),
  hypot = c("G238", "I95", "I950", "I951", "I952", "I958", "I959", "R42", "R55"),
  ischa = c("I20", "I200", "I201", "I208", "I209", "I21", "I210", "I211", "I212", "I213", "I214", "I219", "I22", "I220", "I221", "I228", "I229", "I23", "I230", "I231", "I232", "I233", "I234", "I235", "I236", "I238", "I24", "I240", "I241", "I248", "I249", "I25", "I250", "I251", "I252", "I253", "I254", "I255", "I256", "I258", "I259"),
  memor = c("A810", "B220", "F00", "F000", "F001", "F002", "F009", "F01", "F010", "F011", "F012", "F013", "F018", "F019", "F02", "F020", "F021", "F022", "F023", "F024", "F028", "F03", "F04", "F051", "F067", "G10", "G30", "G300", "G301", "G308", "G309", "G31", "G310", "G311", "G312", "G318", "G319", "I673", "R41", "R410", "R411", "R413", "R418", "R412"),
  mobil = c("R26", "R260", "R261", "R262", "R263", "R268", "Z740", "R993"),
  osteo = c("M80", "M800", "M801", "M802", "M803", "M804", "M805", "M808", "M809", "M81", "M810", "M811", "M812", "M813", "M814", "M815", "M816", "M818", "M819", "M82", "M820", "M821", "M828", "M83", "M831", "M85", "M858", "M859"),
  parki = c("A521", "F028", "G20", "G21", "G210", "G211", "G212", "G213", "G214", "G218", "G219", "G22", "G23", "G230", "G231", "G232", "G233", "G238", "G239", "G24", "G240", "G241", "G242", "G243", "G244", "G245", "G248", "G249", "G25", "G250", "G251", "G252", "G253", "G254", "G255", "G256", "G258", "G259", "G26", "G31", "R251", "Y467"),
  pepti = c("K221", "K25", "K250", "K251", "K252", "K253", "K254", "K255", "K256", "K257", "K258", "K259", "K26", "K260", "K261", "K262", "K263", "K264", "K265", "K266", "K267", "K268", "K269", "K27", "K270", "K271", "K272", "K273", "K274", "K275", "K276", "K277", "K278", "K279", "K28", "K280", "K281", "K282", "K283", "K284", "K285", "K286", "K287", "K288", "K289", "K633", "Z871"),
  perip = c("E105", "E115", "E125", "E135", "E145", "I672", "I70", "I700", "I701", "I702", "I708", "I709", "I73", "I730", "I731", "I738", "I739", "I78", "I780", "I781", "I788", "I789", "I79", "I790", "I791", "I792", "I798", "I80", "I800", "I801", "I802", "I803", "I808", "I809", "I83", "I830", "I831", "I832", "I838", "I839"),
  respi = c("I269", "I27", "I270", "I271", "I272", "I278", "I279", "J40", "J41", "J410", "J411", "J418", "J42", "J43", "J430", "J431", "J432","J438", "J439", "J44", "J440", "J441", "J448", "J449", "J45", "J450", "J451", "J458", "J459", "J46", "J47", "J64", "J66", "J660", "J661", "J662", "J668", "J67", "J670", "J671", "J672", "J673", "J674", "J675", "J676", "J677", "J678", "J679", "J68", "J680", "J681", "J682", "J683", "J684", "J688", "J689", "J69", "J690", "J691", "J698", "J70", "J700", "J701", "J702", "J703", "J704", "J708", "J709", "J80", "J84", "J840", "J841", "J848", "J849", "J96", "J960", "J961", "J969", "M051", "R05", "R093", "R942", "Y556", "Z870"),
  skinu = c("E115", "E125", "E135", "E145", "I830", "E832", "L89", "L890", "L891", "L892", "L893", "L899", "L97", "L984"),
  sleep = c("G470", "G479", "F51", "F510", "F511", "F512", "F513", "F514", "F515", "F518", "F519"),
  socia = c("Z59", "Z590", "Z591", "Z592", "Z593", "Z594", "Z595", "Z596", "Z597", "Z598", "Z599", "Z60", "Z600", "Z602", "Z603", "Z604", "Z605", "Z608", "Z609", "Z63", "Z630", "Z631", "Z632", "Z633", "Z634", "Z635", "Z636", "Z637", "Z638", "Z639", "Z719", "Z734", "Z735", "Z739", "Z912", "Z918"),
  thyro = c("E00", "E000", "E001", "E002", "E009", "E01", "E010", "E011", "E012", "E018", "E02", "E020", "E03", "E030", "E031", "E032", "E033", "E034", "E035", "E038", "E039", "E04", "E040", "E041", "E042", "E048", "E049", "E05", "E050", "E051", "E052", "E053", "E054", "E055", "E058", "E059", "E06", "E060", "E061", "E062", "E063", "E064", "E065", "E069", "E07", "E070", "E071", "E078", "E079", "E230", "E350", "E890", "Z863", "R946"),
  incon = c("F980", "N393", "N394", "R32", "R398"),
  uridi = c("N00", "N000", "N001", "N002", "N003", "N004", "N005", "N006", "N007", "N008", "N009", "N01", "N010", "N011", "N012", "N013", "N014", "N015", "N016", "N017", "N018", "N019", "N02", "N020", "N021", "N022", "N023", "N024", "N025", "N026", "N027", "N028", "N029", "N0", "N030", "N031", "N032", "N033", "N034", "N035", "N036", "N037", "N038", "N039", "N04", "N040", "N041", "N042", "N043", "N044", "N045", "N046", "N047", "N048", "N049", "N05", "N050", "N051", "N052", "N053", "N054", "N055", "N056", "N057", "N058", "N059", "N06", "N060", "N061", "N062", "N063", "N064", "N065", "N066", "N067", "N068", "N069", "N07", "N070", "N071", "N072", "N073", "N074", "N075", "N076", "N077", "N078", "N079", "N08", "N080", "N081", "N082", "N083", "N084", "N085", "N088", "N10", "N100", "N11", "N110", "N111", "N118", "N119", "N12", "N120", "N13", "N130", "N131", "N132", "N133", "N134", "N135", "N136", "N137", "N138", "N139", "N14", "N140", "N141", "N142", "N143", "N144", "N15", "N150", "N151", "N158", "N159", "N16", "N160", "N161", "N162", "N163", "N164", "N165", "N168", "N169", "N17", "N170", "N171", "N172", "N178", "N179", "N20", "N200", "N201", "N202", "N209", "N21", "N210", "N211", "N218", "N219", "N22", "N220", "N228", "N229", "N23", "N25", "N250", "N251", "N258", "N259", "N26", "N27", "N270", "N271", "N279", "N28", "N280", "N281", "N288", "N289", "N29", "N290", "N291", "N298", "N30", "N300", "N301", "N302", "N303", "N304", "N308", "N309", "N31", "N310", "N311", "N312", "N318", "N319", "N32", "N320", "N321", "N322", "N323", "N324", "N328", "N329", "N33", "N338", "N34", "N340", "N341", "N342", "N343", "N35", "N350", "N351", "N358", "N359", "N36", "N360", "N361", "N362", "N363", "N368", "N369", "N37", "N370", "N378", "N39", "N390", "N391", "N392", "N393", "N394", "N398", "N399", "N40", "N41", "N410", "N411", "N412", "N413", "N418", "N419", "N42", "N420", "N421", "N422", "N423", "N428", "N429", "R30", "R300", "R301", "R309", "R31", "R33", "R35", "Y846", "T830"),
  visua = c("E103", "H113", "H123", "H133", "H143", "H161", "H25", "H250", "H251", "H252", "H258", "H259", "H26", "H260", "H261", "H262", "H263", "H264", "H268", "H269", "H28", "H280", "H281", "H282", "H288", "H30", "H300", "H301", "H302", "H308", "H309", "H31", "H310", "H311", "H312", "H313", "H314", "H318", "H319", "H341", "H342", "H348", "H349", "H35", "H350", "H351", "H352", "H353", "H354", "H355", "H356", "H357", "H358", "H359", "H36", "H360", "H368", "H448", "H46", "H47", "H470", "H472", "H473", "H474", "H475", "H476", "H477", "H53", "H530", "H531", "H532", "H533", "H534", "H535", "H536", "H538", "H539", "H54", "H540", "H541", "H542", "H543", "H544", "H545", "H546", "H549", "H59", "H590", "H598", "H599", "Q120", "Q129", "S040", "S057", "Z866", "Z973"),
  weigh = c("E46", "R630", "R633", "R634", "R64"),
  activ = c("Z73", "Z736"),
  # Complications
  compacva = c("I60",	"I61",	"I62",	"I63", "I610",	"I611",	"I612",	"I613",	"I614",	"I615",	"I616",	"I618",	"I619", "I630",	"I631",	"I632",	"I633",	"I634",	"I635",	"I636",	"I638",	"I639", "I64"),
  complrti = c("J12",	"J13",	"J14",	"J15",	"J16",	"J18",	"J20",	"J22",	"J86", "J440",	"J441",	"J851",	"J852",	"J690"),
  compmi = c("I21", "I210",	"I211",	"I212",	"I213",	"I214",	"I219",	"I220",	"I221", "I228", "I229"),
  compdvt = c("I801", "I802", "I803", "I808", "I809", "I828", "I829", "I824"),
  comppe = c("I822",	"I823",	"I260",	"I269"),
  compaki = c("N170", "N171", "N172", "N178", "N179"),
  computi = c("N10", "N300", "N308", "N309", "N390"),
  compwound = c("T813"),
  compssi = c("T814"),
  compppf = c("M966"),
  comppros = c("T840"),
  compnvinj = c("T812"),
  compbloodtx = c("X331", "X332", "X333", "X337", "X338", "X339", "X341", "X342", "X343", "X344")
)

# log_info('Codes to check
# 
#          - List element
#          {names(char_frail_comps_codes)}
# 
#          - ICD-10 codes
#          {char_frail_comps_codes}
# 
#          - Code descriptions
#          {icd::explain_code(char_frail_comps_codes)}
# 
#          ---------------------------------------------------------------------
#          ')


# List of procedures to tag

proc_codes <- list(
  #Action code vectors
  act = c("Y032", "Y037"),
  #Body part vectors
  hbp = c("Z756","Z761","Z843"),
  kbp = c("Z765","Z774","Z787","Z844","Z845","Z846"),
  #Procedure vectors
  thr = c("W371","W378","W379","W381","W388","W389","W391","W398","W399", "W931","W938","W939","W941","W948","W949","W951","W958","W959", "W461","W468","W469","W471","W478","W479","W481","W488","W489"),
  thrbp = c("W521","W531","W541","W581", "W528","W529","W538","W539","W548","W549"),
  tkr = c("O181","O188","O189","W401","W408","W409","W411","W418","W419", "W421","W428","W429","W528","W529","W538","W539","W548","W549"),
  tkrbp = c("W521 ","W531 ","W541","W581"),
  rhr = c("W370","W372","W373","W374","W380","W382","W383","W384","W390","W392","W393","W395","W462","W472","W482","W930","W932","W933","W940","W942","W943","W950","W952","W953","W954","W460","W463","W470","W473","W480","W483","W484"),
  rhrbp = c("W522 ","W523","W532","W533","W542","W543","W572","W574","W582","W520","W530","W540"),
  rhrbpa = c("W394","W544"),
  rkr = c("O180","O182","O183","O184","W400","W402","W403","W404","W410","W412","W413","W414","W420","W422","W423","W425"),
  rkrbp = c("W522","W523","W532","W533","W542","W543","W553","W564","W574","W582","W603","W613","W641","W642","W520","W530","W540"),
  rkrbpa = c("W424","W544")
)


# Loop
setwd(paste0(data_dir,"HES/"))
  
files <- list.files(pattern = "txt")

for (i in files){  setwd(paste0(data_dir,"HES/"))
  loopStart <- Sys.time()

  log_info('FILENAME = {i}')

  hes <- fread(i,header = TRUE,sep = "|", data.table = TRUE, stringsAsFactors = FALSE)
  
  # Log number of rows
  log_info('Number of rows = {hes[,.N]}')

  # Clean
  hes[, ADMIMETH := fcase(ADMIMETH =="2A", "66",
                          ADMIMETH =="2B", "67",
                          ADMIMETH =="2C", "68",
                          ADMIMETH =="2D", "69",
                          rep(TRUE, .N), as.character(ADMIMETH))]
  
  hes[, ADMIMETH := fifelse((ADMIMETH =="98" | ADMIMETH =="99"), NA_character_, ADMIMETH)]
  
  hes[, ADMINCAT := fifelse((ADMINCAT =="98" | ADMINCAT =="99"), NA_real_, ADMINCAT)]
  
  hes[, CLASSPAT := fifelse(CLASSPAT =="9", NA_real_, CLASSPAT)]
  hes[, DISDEST := fifelse((DISDEST =="98" | DISDEST =="99"), NA_real_, DISDEST)]
  hes[, EPIORDER := fifelse((EPIORDER =="98" | EPIORDER =="99"), NA_real_, EPIORDER)]
  hes[, ETHNOS := fifelse((ETHNOS =="X" | ETHNOS =="Z" | ETHNOS =="99"), NA_character_, ETHNOS)]
  hes[, SEX := fifelse(SEX =="0" | SEX =="9", NA_real_, SEX)]

  # Define NULL dates
  datecols <- grep("*DATE", names(hes))
  nulldates <- c(as.IDate("1800-01-01"), as.IDate("1801-01-01"), as.IDate("1600-01-01"), as.IDate("1582-10-15"))
  hes[ , (datecols) := lapply(.SD, function(x) fifelse(x %in% nulldates, as.IDate(NA_integer_), as.IDate(x))), .SDcols = datecols]

  hes[ , (c("DOMPROC", "MAINSPEF", "TRETSPEF")) := lapply(.SD, function(x) fifelse(x == "&", NA_character_, as.character(x))), .SDcols = c("DOMPROC", "MAINSPEF", "TRETSPEF")]
  hes[, DOMPROC := fifelse(DOMPROC =="-", NA_character_, DOMPROC)]
  hes[, SPELEND := fifelse(SPELEND =="Y", 1, 0)]


  # Derive
  hes[, ETHNIC5 := fcase(ETHNOS == "A" | ETHNOS == "B" | ETHNOS == "C", "White",
                         ETHNOS == "D" | ETHNOS == "E" | ETHNOS == "F" | ETHNOS == "G", "Mixed", ETHNOS == "H" | ETHNOS == "J" | ETHNOS == "K" | ETHNOS == "L", "Asian/Asian British",
                         ETHNOS == "M" | ETHNOS == "N" | ETHNOS == "P", "Black/Black British",
                         ETHNOS == "R" | ETHNOS == "S", "Chinese/Other",
                         is.na(ETHNOS), "Unknown")]

  hes[, TRANSIT := fcase(!(ADMISORC %in% c(51, 52, 53)) & (DISDEST %in% c(51, 52, 53)) & !(ADMIMETH %in% c(67, 81)), 1,
                           ((ADMISORC %in% c(51, 52, 53)) | (ADMIMETH %in% c(67, 81))) & (DISDEST %in% c(51, 52, 53)), 2,
                           ((ADMISORC %in% c(51, 52, 53)) | (ADMIMETH %in% c(67, 81))) & !(DISDEST %in%  c(51, 52, 53)), 3,
                           rep(TRUE, .N), 0)]

  hes[, ADMIDATE_FILLED := fcase(!is.na(ADMIDATE), ADMIDATE,
                                 (is.na(ADMIDATE) & !is.na(EPISTART) & EPIORDER == 1 & !(ADMIMETH %in% c(67,81)) & !(ADMISORC %in% c(51,52,53))), EPISTART)]

  hes[, EPI_VALID := fifelse((!is.na(STUDY_ID) &
                                !is.na(ADMIDATE) &
                                !is.na(PROCODE3) &
                                EPISTAT == 3 &
                                !is.na(EPIKEY) &
                                !is.na(EPISTART) &
                                !is.na(EPIEND)), TRUE, FALSE)]

  hes[, EPIDUR_CALC := (EPIEND - EPISTART)]

  hes[, EPI_BAD := fifelse(EPIDUR_CALC <0, TRUE, FALSE)]
  

  ## Row quality dummies
  hes[, rq1 := fifelse(!is.na(ADMIMETH), 0, 1)]
  hes[, rq2 := fifelse(!is.na(ADMISORC), 0, 1)]
  hes[, rq3 := fifelse(!is.na(DISDEST), 0, 1)]
  hes[, rq4 := fifelse((STARTAGE >18 & STARTAGE <105 & !is.na(STARTAGE)), 0, 1)]
  hes[, rq5 := fifelse((!is.na(ADMIDATE) & ADMIDATE>as.Date('1998-01-01') & ADMIDATE<as.Date('2022-01-01')), 0, 1)]
  hes[, rq6 := fifelse((!is.na(DISDATE) & DISDATE>as.Date('1998-01-01') & DISDATE<as.Date('2022-01-01')), 0, 1)]
  hes[, rq7 := fifelse((!is.na(EPISTART) & EPISTART>as.Date('1998-01-01') & EPISTART<as.Date('2022-01-01')), 0, 1)]
  hes[, rq8 := fifelse((!is.na(EPIEND) & EPIEND>as.Date('1998-01-01') & EPIEND<as.Date('2022-01-01')), 0, 1)]
  hes[, rq9 := fifelse((MAINSPEF == "110" | TRETSPEF == "110"), 0, 1)]
  hes[, rq10 := fifelse((OPERTN_01 !="" & OPERTN_01 !="-" & OPERTN_01 !="-"), 0, 1)]
  hes[, rq11 := fifelse((DIAG_01 !=""), 0, 1)]
  hes[, rq12 := fifelse((DIAG_02 !=""), 0, 1)]
  hes[, rq13 := fifelse((DIAG_03 !=""), 0, 1)]
  hes[, rq14 := fifelse((!is.na(OPDATE_01) & OPDATE_01>as.Date('1998-01-01') & OPDATE_01<as.Date('2022-01-01')), 0, 1)]
  vecCols <- c("rq1", "rq2", "rq3", "rq4", "rq5", "rq6", "rq7", "rq8", "rq9", "rq10", "rq11", "rq12", "rq13", "rq14")
  hes[, rq := rowSums(.SD), .SDcols = vecCols]
  
  
  # Create row_id, which will be used as visit_name to reshape within icd package
  # And-- will be unique for a given .txt file / fyear
  hes <- hes[, row_id := .I]
  
  
  # Tag comorbidities
  # Reshape from wide to long within the icd function
  # The icd function will return a wide dataframe
  comorbs <-
    setDT(
      icd10_comorbid(
        data.table::melt(hes, id.vars = "row_id", measure.vars = diagcols),
        char_frail_comps_codes,
        visit_name = "row_id",
        icd_name = "value",
        short_code = TRUE,
        #short_map = guess_short(map),
        return_df = TRUE,
        return_binary = TRUE
        #icd10_comorbid_fun = icd10_comorbid_reduce
      )
    )
  
  comorbs$row_id <- as.numeric(comorbs$row_id)
  
  # Join to main dataset
  hes <- hes[comorbs, on = .(row_id)]
  rm(comorbs)
  
  
  # Repeat for procedure codes
  procs <-
    setDT(
      icd10_comorbid(
        data.table::melt(hes, id.vars = "row_id", measure.vars = proccols),
        proc_codes,
        visit_name = "row_id",
        icd_name = "value",
        short_code = TRUE,
        #short_map = guess_short(map),
        return_df = TRUE,
        return_binary = TRUE
        #icd10_comorbid_fun = icd10_comorbid_reduce
      )
    )
  
  procs$row_id <- as.numeric(procs$row_id)
  
  # Join to main dataset
  hes <- hes[procs, on = .(row_id)]
  rm(procs)

  
  # Create jr field
  hes[, jr := fcase(thr == 1, 1,
                    (thrbp == 1 & hbp == 1), 1,
                    tkr == 1, 3,
                    (tkrbp == 1 & kbp == 1), 3,
                    rhr == 1, 2,
                    (rhrbp == 1 & hbp == 1), 2,
                    (rhrbpa == 1 & hbp == 1 & act == 1), 2,
                    rkr == 1, 4,
                    (rkrbp == 1 & kbp == 1), 4,
                    (rkrbpa == 1 & kbp == 1 & act == 1), 4,
                    rep(TRUE, .N), NA_real_)]
  
  hes[, jr := factor(jr, levels = c(1,2,3,4), ordered=TRUE,
                     labels = c("Primary Hip Replacement", "Revision Hip Replacement", "Primary Knee Replacement", "Revision Knee Replacement"))]
  
  # Delete unwanted column(s)
  hes[ ,c("act", "hbp", "kbp", "thr", "thrbp", "tkr", "tkrbp", "rhr", "rhrbp", "rhrbpa", "rkr", "rkrbp", "rkrbpa") := NULL]
  

  # # Remove duplicates on all fields
  # hes <- unique(hes)
  # 
  # # Log number of unique rows
  # log_info('Number of unique rows = {hes[,.N]}')


  write_parquet(hes,paste0(data_dir,"HES_new/",i, ".parquet"))

  loopEnd <- Sys.time()
  loopTime <- round(as.numeric(loopEnd) - as.numeric(loopStart), 0)
}

# One cannot run window functions on individual datasets because of the need to look up the preceding/following dataset
rm(list= ls()[! (ls() %in% c('data_dir', 'script_dir'))])