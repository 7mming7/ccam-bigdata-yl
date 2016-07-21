use zhgl;

create table TEMP_MAIN_ACCT
(
  xaccount  string,
  custr_nbr string,
  category  string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/temp_main_acct';