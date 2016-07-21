use zhgl;

create table if not exists S24_APCD
(
	bank      string,
  app_jday  string,
  app_seq   string,
  custr_nbr string,
  card_nbr  string,
  app_day   string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_apcd';