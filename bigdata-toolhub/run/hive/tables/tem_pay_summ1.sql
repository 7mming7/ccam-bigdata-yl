use zhgl;

CREATE TABLE IF NOT EXISTS TEM_PAY_SUMM1
(
  acct     string,
  monthnbr string,
  summoney string
)
row format delimited fields 
terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/tem_pay_summ1';