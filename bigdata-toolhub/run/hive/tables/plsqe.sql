use zhgl;

CREATE TABLE IF NOT EXISTS PLSQE
(
  custr_nbr         string,
  acct              string,
  adjust_type       string,
  adjust_limit      string,
  interest_charges1 string,
  interest_charges2 string
)
row format delimited fields 
terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/plsqe';