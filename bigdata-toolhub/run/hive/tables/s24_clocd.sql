use zhgl;

create table if not exists S24_CLOCD(
close_cd         string,
close_sedc       string,
valid_clos       string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_clocd';