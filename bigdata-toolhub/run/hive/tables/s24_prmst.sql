use zhgl;

create table if not exists S24_PRMST(
stmtcode         string,
effect_day       string,
sms_yn           string,
print_yn         string,
emial_yn         string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_prmst';