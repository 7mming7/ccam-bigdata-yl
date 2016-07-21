use zhgl;

create table if not exists S24_PTGFT(
bank             string,
gift_no          string,
chged_amt        string,
counts           string,
gift_brief       string,
gift_name        string,
gift_price       string,
merchant         string,
point            string,
fee_flag         string,
day_flag         string,
begin_day        string,
endgf_day        string,
ptprod_yn        string,
prod_no1         string,
prod_no2         string,
prod_no3         string,
prod_no4         string,
prod_no5         string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_ptgft';