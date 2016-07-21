use zhgl;

create table if not exists TEMP_ACCT_CDATE
(
  xaccount        string,
  cycle_date      string,
  close_cycle_nbr string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/temp_acct_cdate';