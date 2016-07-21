use zhgl;

CREATE TABLE IF NOT EXISTS SYSTEMPARA
(
  curdate    string,
  procstep   string,
  is_week    string,
  is_ten     string,
  is_month   string,
  is_quarter string,
  is_syear   string,
  is_year    string
)
row format delimited fields 
terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/systempara';