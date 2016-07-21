use zhgl;

create table if not exists S24_TRDEF(
trans_type       string,
desc_print       string,
o_r_flag         string,
bank_ac_dt       string,
bankacc1         string,
bankacct         string,
bankacc1b        string,
bankacctb        string,
curr_num         string,
feetx_code       string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_trdef';