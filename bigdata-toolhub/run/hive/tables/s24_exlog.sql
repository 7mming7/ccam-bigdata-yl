use zhgl;

create table if not exists S24_EXLOG(
bank             string,
card_nbr         string,
card_bin         string,
account          string,
inp_date         string,
inp_time         string,
logtype          string,
exch_date        string,
bankacct         string,
rem_examt        string,
req_amtcd        string,
req_fix          string,
req_perc         string,
exch_amt         string,
exch_rate        string,
exch_flag        string,
serv_type        string,
exch_result      string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_exlog';