use zhgl;

create table if not exists TEMP_EVENT_PUR_MERCHANT_6M
(
  acctnbr       string,
  merchantsum6m string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/temp_event_pur_merchant_6m';