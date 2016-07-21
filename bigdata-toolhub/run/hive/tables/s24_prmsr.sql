use zhgl;

create table if not exists S24_PRMSR(
app_source       string,
src_desc         string
)
row format delimited
fields terminated by '|'
stored as textfile 
location '/user/hive/warehouse/HDFSDIRNAME/s24_prmsr';