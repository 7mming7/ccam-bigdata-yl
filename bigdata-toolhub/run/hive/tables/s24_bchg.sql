use zhgl;

create table if not exists S24_BCHG(
bank             string,
business         string,
entry_code       string,
entry_day        string,
entry_time       string,
inp_day          string,
purge_day        string,
branch           string,
dev_addr         string,
emply_nbr        string,
entry_isp        string,
new_valu         string,
oride_empl       string,
oride_lvl        string,
prior_valu       string,
sec_level        string,
rcu_emply        string,
or_rcu_emp       string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_bchg';