use zhgl;

create table if not exists S24_CHGS (
  xaccount   string,
  bank       string,
  business   string,
  entry_code string,
  entry_day  string,
  entry_time string,
  inp_day    string,
  purge_day  string,
  seq_nbr    string,
  branch     string,
  card_bin   string,
  card_nbr   string,
  custr_nbr  string,
  dev_addr   string,
  dev_type   string,
  emply_nbr  string,
  end_digits string,
  entry_sub  string,
  entry_type string,
  new_valu   string,
  no_changed string,
  notes_day  string,
  notes_seq  string,
  oride_empl string,
  oride_lvl  string,
  prior_valu string,
  product    string,
  rep_flag   string,
  sec_level  string,
  rcu_emply  string,
  or_rcu_emp string,
  brno       string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_chgs';