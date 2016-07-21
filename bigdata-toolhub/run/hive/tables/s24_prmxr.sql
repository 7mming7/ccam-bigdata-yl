use zhgl;

create table if not exists S24_PRMXR (
  bank       string,
  curr_num   string,
  effect_day string,
  rate_val0  string,
  rate_val1  string,
  rate_val2  string,
  rate_val3  string,
  rate_val4  string,
  rate_val5  string,
  rate_val6  string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_prmxr';