use zhgl;

create table if not exists S24_STMA(
xaccount         string,
bank             string,
curr_num         string,
month_nbr        string,
stm_closdy       string,
balance          string,
balance_flag     string,
close_code       string,
cred_limit       string,
cupaym24         string,
custr_name       string,
custr_nbr        string,
lastpayday       string,
min_due          string,
open_day         string,
ovdu_amts        string,
ovdu_month       string,
paymnts          string,
paymt12          string,
paymt24          string,
stm_balnce       string,
stm_balnce_flag  string,
serve_nbr        string,
pbc_brnch        string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_stma';