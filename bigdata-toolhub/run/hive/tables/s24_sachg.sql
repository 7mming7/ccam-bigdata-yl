use zhgl;

create table if not exists S24_SACHG(
xaccount         string,
bank             string,
card_bin         string,
chg_flag         string,
curr_num         string,
month_nbr        string,
p_flag           string,
employee         string,
inp_day          string,
inp_sourve       string,
inp_time         string,
f_name           string,
proc_day         string,
new_flag         string,
shar_lmt         string,
pbc_brnch        string,
err_code         string,
rcu_emply        string,
branch           string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_sachg';