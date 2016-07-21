use zhgl;

create table if not exists S24_FCHG(
bank             string,
emply_nbr        string,
entry_day        string,
sys_time         string,
tran_no          string,
txncode          string,
amount           string,
amount_flag      string,
branchw          string,
card_nbr         string,
curr_num         string,
custr_nbr        string,
entry_code       string,
entry_dat        string,
info             string,
inp_day          string,
inp_ispec        string,
inp_source       string,
opt_flag         string,
ori_inpday       string,
ori_tranno       string,
ori_valday       string,
oride_empl       string,
purge_day        string,
resp_code        string,
sys_date         string,
trans_type       string,
val_day          string,
audit_num        string,
date_time        string,
msg_type         string,
ori_party        string,
prc_code         string,
trans_amt        string,
trans_amt_flag   string,
adj_emp          string,
adj_ren          string,
adj_src          string,
rcu_emp          string,
or_rcu_emp       string,
brno             string
)
row format delimited
fields terminated by '|'
stored as textfile 
location '/user/hive/warehouse/HDFSDIRNAME/s24_fchg';