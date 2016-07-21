use zhgl;

create table if not exists FDM_S24_APCD (
  bank      string,
  app_jday  string,
  app_seq   string,
  custr_nbr string,
  card_nbr  string,
  app_day   string
)
partitioned by (CREATE_DATE string)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/fdm_s24_apcd';