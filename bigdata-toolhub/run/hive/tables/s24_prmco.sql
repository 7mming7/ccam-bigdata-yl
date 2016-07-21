use zhgl;

create table if not exists S24_PRMCO(
product         string,
effect_day      string,
card_rep1       string,
card_rep2       string,
card_urg1       string,
card_urc1       string,
card_desp1      string,
card_desp2      string,
card_desp3      string,
pin_fee         string,
pin_fee1        string,
rush_fee        string,
cancl_fee       string,
vouch_fee1      string,
vouch_fee2      string,
vouch_fee3      string,
vouch_fee4      string,
balinq_yn       string,
balinq_amt      string,
balfreeno       string,
balinqx_yn      string,
balinq_amx      string,
balfreenox      string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_prmco';