use zhgl;

create table if not exists S24_PRMCD(
product          string,
prod_desc        string,
prod_level       string,
category         string,
serv_code        string,
card_plan        string,
curr_num2        string,
product_ty       string,
prod_grp         string,
branch           string,
fee_group        string,
approd_grp       string,
hcardpre         string,
lcardpre         string,
yr_1st_iss       string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_prmcd';