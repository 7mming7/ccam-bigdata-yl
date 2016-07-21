use zhgl;

create table if not exists S24_INTR(
app_jday         string,
app_seq          string,
bank             string,
card_id          string,
intr_cnbr        string,
intr_name        string,
intr_nbr         string,
intr_recom       string,
intr_rl          string,
ab_access        string,
ab_branch        string,
ab_name          string,
ab_user          string,
app_batch        string,
microfilm        string,
ab_fashion       string,
ab_phone         string,
ab_source        string,
etl_day          string,
app_day          string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_intr';