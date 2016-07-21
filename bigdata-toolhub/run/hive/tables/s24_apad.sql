use zhgl;

create table if not exists S24_APAD
(
app_jday        string,
app_seq         string,
bank            string,
address1_a      string,
address2_a      string,
address3_a      string,
address4_a      string,
address5_a      string,
addrtype_a      string,
mail_to         string,
mail_to2        string,
osea_f_a        string,
postcode_a      string,
state_c_a       string,
intr_cnbr       string,
intr_name       string,
intr_nbr        string,
intr_recom      string,
intr_rl         string,
ab_source       string,
ab_phone        string,
app_day         string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_apad';