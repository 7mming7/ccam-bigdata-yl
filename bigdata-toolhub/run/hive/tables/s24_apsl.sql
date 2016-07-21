use zhgl;

create table if not exists S24_APSL(
bank              string,
app_jday          string,
app_seq           string,
app_index         string,
product           string,
fee_code          string,
base_card         string,
card_sel          string,
cdfrm             string,
cdesploc          string,
cdespmtd          string,
courierf          string,
cred_lmt          string,
cred_amt          string,
custr_nbr         string,
emb_card          string,
embosnme          string,
embossul          string,
embossur          string,
expydate          string,
member            string,
pin_chk           string,
pin_reqd          string,
v_prod            string,
v_feecd           string,
v_embnm           string,
fee_group         string,
card_to           string,
app_day           string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_apsl';