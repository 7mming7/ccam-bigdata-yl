use zhgl;

create table if not exists TEMP_EVENT_PUR_MERCHANT_0
(
  acctnbr string,
  max_amt string,
  cnt     string,
  sum_amt string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/temp_event_pur_merchant_0';