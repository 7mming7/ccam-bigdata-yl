use zhgl;

create table if not exists S24_ACSR(
xaccount         string,
card_nbr         string,
bank             string,
serv_type        string,
serv_lv          string,
fee_lv           string,
serv_sts         string,
next_sts         string,
fee_month        string,
fee_amt          string,
feesign          string,
eff_day          string,
exp_dte          string,
inp_day          string,
inp_time         string,
chg_day          string,
chg_time         string,
info             string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_acsr';