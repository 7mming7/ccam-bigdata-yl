use zhgl;

create table if not exists S24_INTCT(
bank             string,
effect_day       string,
int_code         string,
int_fee          string,
fee_type         string,
fee_typex        string,
ignore_p         string,
ignore_c         string,
int_cmp_yn       string,
ir_earn          string,
ir_earnx         string,
int_fee_p        string,
ir_cash          string,
ir_purch         string,
ir_cashx         string,
ir_purchx        string,
noif_fl          string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_intct';