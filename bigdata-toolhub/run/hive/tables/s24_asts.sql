use zhgl;

create table if not exists S24_ASTS(
bank         string,
new_status   string,
old_status   string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_asts';