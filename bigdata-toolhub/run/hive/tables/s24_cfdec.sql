use zhgl;

create table if not exists S24_CFDEC(
cf_idx           string,
cf_desc          string,
open_day         string,
close_day        string,
cnt_flag         string,
lowtra_yn        string,
feepcnt_a        string,
trancnt1_a       string,
tranamt1_a       string,
trancnt2_a       string,
tranamt2_a       string,
trancnt3_a       string,
tranamt3_a       string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_cfdec';