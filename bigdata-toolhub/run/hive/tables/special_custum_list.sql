use zhgl;

create table if not exists SPECIAL_CUSTUM_LIST
(
  id      string,
  custnum string,
  acct    string,
  flag    string,
  bankid  string,
  cycle_date string,
  update_date string,
  update_name  string,
  status  string,
  remark  string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/special_custum_list';