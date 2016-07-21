use zhgl;

create table if not exists TBL_BANKS
(
  bankid string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/tbl_banks';