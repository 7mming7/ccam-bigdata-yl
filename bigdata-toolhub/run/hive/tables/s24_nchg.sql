use zhgl;

create table if not exists S24_NCHG(
bank             string,
entry_day        string,
inp_day          string,
inp_key          string,
inp_time         string,
purge_day        string,
branch           string,
devaddr          string,
emplevel         string,
employno         string,
entry_code       string,
inp_src          string,
new_value        string,
old_value        string,
rep_flag         string,
rcu_emp          string,
or_rcu_emp       string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_nchg';