use zhgl;

create table if not exists S24_PRMPT(
point_no         string,
point_desc       string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_prmpt';