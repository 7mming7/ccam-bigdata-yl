use zhgl;

create table if not exists TEMP_S24_CHGS_ADJ
(
  xaccount        string,
  limit_adj_type  string,
  limit_adj_sign  string,
  histupdatedtype string,
  entry_day       string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/temp_s24_chgs_adj';