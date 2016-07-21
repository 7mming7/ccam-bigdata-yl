use zhgl;

create table if not exists TEMP_EVENT_PUR_MERCHANT_CNT_6M
(
  acctnbr                   string,
  offline_nodl_merchant_cnt string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/temp_event_pur_merchant_cnt_6m';