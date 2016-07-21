use zhgl;

create table if not exists S24_BRNCH(
branch           string,
brch_name        string,
brnch_code       string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_brnch';
