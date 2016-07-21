use zhgl;

create table if not exists S24_ACHG (
  bank       string,
  entry_day  string,
  entry_isp  string,
  entry_key  string,
  entry_time string,
  entry_type string,
  inp_day    string,
  purge_day  string,
  emply_brch string,
  emply_lvl  string,
  emply_nbr  string,
  entry_stn  string,
  new_value  string,
  old_value  string,
  rep_flag   string,
  rcu_emply  string,
  or_rcu_emp string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_achg';