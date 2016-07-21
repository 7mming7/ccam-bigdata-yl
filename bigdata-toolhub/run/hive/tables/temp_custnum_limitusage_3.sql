use zhgl;

CREATE TABLE IF NOT EXISTS TEMP_CUSTNUM_LIMITUSAGE_3
(
  custnum      string,
  limit        string,
  limit_add14  string,
  bal          string,
  bal_add14    string,
  bal_wo_auths string,
  days         string
)
row format delimited fields 
terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/temp_custnum_limitusage_3';