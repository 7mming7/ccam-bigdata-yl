use zhgl;

create table if not exists TEMP_CUSTNUM_LIMITUSAGE
(
  custnum     string,
  limit       string,
  limit_add14 string,
  bal         string,
  bal_add14   string
)
PARTITIONED BY (months string)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/temp_custnum_limitusage';