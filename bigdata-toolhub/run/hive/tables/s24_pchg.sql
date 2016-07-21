use zhgl;

create table if not exists S24_PCHG(
bank             string,
entry_day        string,
entry_isp        string,
entry_time       string,
extra_key        string,
product          string,
emply_brch       string,
emply_lvl        string,
emply_nbr        string,
entry_stn        string,
entry_type       string,
inp_day          string,
new_value        string,
old_value        string,
oride_empl       string,
oride_lvl        string,
rcu_emply        string,
or_rcu_emp       string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_pchg';