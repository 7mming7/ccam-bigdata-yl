use zhgl;

create table if not exists S24_CNTA (
	bank       string,
  custr_nbr  string,
  con_name   string,
  app_jday   string,
  app_seq    string,
  microfilm  string,
  con_id     string,
  con_tel    string,
  con_ext    string,
  con_comp   string,
  con_mobile string,
  con_rel    string,
  con_flag   string,
  etl_day    string,
  con_area   string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_cnta';