use zhgl;

CREATE TABLE IF NOT EXISTS TEM_PAYMENT_M3
(
  acct            string,
  monthnur        string,
  countt          string,
  hmaxmoney       string,
  crentsummoney   string,
  closebal        string,
  payment         string,
  maxmoneytimenum string,
  summoneytimenum string,
  summooney_time  string
)
row format delimited fields 
terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/tem_payment_m3';