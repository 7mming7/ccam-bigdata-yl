use zhgl;

create table if not exists S24_OCHG(
xaccount         string,
bank             string,
entry_day        string,
entry_time       string,
entry_type       string,
inp_day          string,
card_bin         string,
custr_nbr        string,
employee         string,
emply_brch       string,
emply_lvl        string,
entry_stn        string,
new_value        string,
old_value        string,
trans_src        string,
rcu_emply        string,
or_rcu_emp       string
)
row format delimited
fields terminated by '|'
stored as textfile 
location '/user/hive/warehouse/HDFSDIRNAME/s24_ochg';