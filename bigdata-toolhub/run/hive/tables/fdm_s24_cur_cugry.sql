use zhgl;

create table if not exists FDM_S24_CUR_CUGRY (
  start_date  string,
  bank        string,
  custr_nbr   string,
  name_key    string,
  inp_src     string,
  inp_reason  string,
  reason_desc string,
  rcu_emply   string,
  branch      string,
  inp_day     string,
  inp_time    string,
  chg_day     string,
  chg_time    string,
  note        string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/fdm_s24_cur_cugry';