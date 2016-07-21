use zhgl;

create table if not exists S24_JORJ(
bank             string,
inp_time         string,
o_r_flag         string,
rel_day          string,
trans_type       string,
xaccount         string,
bankacct         string,
bankacc1         string,
brno             string,
card_bin         string,
card_nbr         string,
curr_num         string,
descriptn        string,
empno            string,
merchant         string,
serial_no        string,
tran_amt         string,
tran_code        string,
openbrno         string,
category         string,
pipe             string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_jorj';