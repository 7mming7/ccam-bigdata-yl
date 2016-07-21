use zhgl;

create table if not exists S24_ICCD(
bank             string,
card_nbr         string,
balance          string,
lst_update       string
)
row format delimited
fields terminated by '|'
stored as textfile 
location '/user/hive/warehouse/HDFSDIRNAME/s24_iccd';