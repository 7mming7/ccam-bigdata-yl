use zhgl;

create table if not exists S24_APMO(
bank            string,
app_jday        string,
app_seq         string,
note_id         string,
note_msg1       string,
note_msg2       string,
note_day        string,
note_time       string,
employee        string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_apmo';