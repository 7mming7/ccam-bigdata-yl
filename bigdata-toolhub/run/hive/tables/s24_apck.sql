use zhgl;

create table if not exists S24_APCK(
bank             string,
app_jday         string,
app_seq          string,
chk_code         string,
app_index        string,
chk_msg          string,
chk_day          string,
chk_time         string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_apck';