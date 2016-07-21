use zhgl;

create table if not exists S24_ICLG(
bank             string,
trans_type       string,
trans_src        string,
tran_code        string,
card_nbr         string,
opdate           string,
optime           string,
proc_resul       string,
amount           string,
currncy_cd       string,
authcode         string,
acptor_id        string,
card_name        string
)
row format delimited
fields terminated by '|'
stored as textfile 
location '/user/hive/warehouse/HDFSDIRNAME/s24_iclg';