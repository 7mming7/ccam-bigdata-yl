use zhgl;

create table if not exists S24_APQU(
bank            string,
app_jday        string,
app_seq         string,
app_status      string,
app_apque       string,
business        string,
cur_emp         string,
pre_emp         string,
pre_sts         string,
pre_apque       string,
note_id         string,
apchk_cn        string,
proce_day       string,
proce_tme       string,
microfilm       string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_apqu';