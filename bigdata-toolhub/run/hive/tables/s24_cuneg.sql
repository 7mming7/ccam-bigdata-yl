use zhgl;

create table if not exists S24_CUNEG (
  bank       string,
  custr_nbr  string,
  file_type  string,
  inp_day    string,
  inp_time   string,
  name_key   string,
  applnref   string,
  custr_ref  string,
  employee   string,
  name_extnd string,
  reasn_code string,
  reasn_desc string,
  status     string,
  chg_day    string,
  chg_time   string,
  chg_employ string,
  inp_source string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_cuneg';