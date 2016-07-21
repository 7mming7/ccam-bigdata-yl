use zhgl;

create table if not exists S24_DDAC(
bank             string,
xaccount         string,
bankacct         string,
bankcode         string,
ddtype           string,
rel_stat         string,
add_acct         string,
onus_flag        string,
acct_bank        string,
create_day       string,
force_flag       string,
onus_bank        string
)
row format delimited
fields terminated by '|'
stored as textfile 
location '/user/hive/warehouse/HDFSDIRNAME/s24_ddac';