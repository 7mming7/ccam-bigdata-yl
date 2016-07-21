use zhgl;

create table if not exists TEMP_CUSTUM_SPECIAL_SP
(
  custr_nbr              string,
  limitusage             string,
  bal                    string,
  bal_add14              string,
  bal_wo_auths           string,
  stmtrepayratio_l12m    string,
  stmtrepayaveratio_l12m string,
  last3monavgrate        string,
  stmtutility_ave_l6m    string,
  stmtrepayratio_l6m     string,
  total_usepre           string,
  interest6mon           string,
  maxproductline         string,
  points_6m              string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/temp_custum_special_sp';