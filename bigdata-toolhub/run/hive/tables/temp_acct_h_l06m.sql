use zhgl;

create table if not exists TEMP_ACCT_H_L06M
(
  xaccount  string,
  mths_odue string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/temp_acct_h_l06m';