use zhgl;

create table if not exists S24_APLG
(
app_apque        string,
app_status       string,
bank             string,
emply_nbr        string,
proce_day        string,
app_jday         string,
app_seq          string,
microfilm        string,
nxt_emply        string,
nxt_status       string,
pre_status       string,
proce_src        string,
proce_time       string,
app_day          string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_aplg';