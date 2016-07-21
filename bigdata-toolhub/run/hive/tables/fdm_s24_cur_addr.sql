use zhgl;

create table if not exists FDM_S24_CUR_ADDR (
  start_date string,
  bank       string,
  custr_nbr  string,
  addr_line1 string,
  addr_line2 string,
  addr_line3 string,
  addr_line4 string,
  addr_line5 string,
  post_code  string,
  state_c    string,
  addr_type  string,
  rec_type   string,
  merchant   string,
  oseaaddr_f string,
  change_day string,
  change_tme string,
  create_day string,
  create_tme string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/fdm_s24_cur_addr';