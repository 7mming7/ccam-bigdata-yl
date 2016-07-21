use zhgl;

create table if not exists S24_SCODE (
  bank       string,
  code_type  string,
  code_value string,
  brief      string,
  value_def  string,
  short_def  string,
  var_a1     string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_scode';