use zhgl;

create table if not exists S24_GUAR (
  bank       string,
  guarn_id   string,
  account    string,
  card_id    string,
  custr_id   string,
  app_jday   string,
  app_seq    string,
  guarn_code string,
  guarn_bank string,
  guarn_amt  string,
  guarn_ref  string,
  guarn_rel  string,
  etl_day    string,
  app_day    string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_guar';