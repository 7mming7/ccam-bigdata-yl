use zhgl;

create table if not exists TEMP_EVENT_CASH_6M
(
  acctnbr    string,
  cash_1     string,
  cash_cnt_1 string,
  cash_2     string,
  cash_cnt_2 string,
  cash_3     string,
  cash_cnt_3 string,
  cash_4     string,
  cash_cnt_4 string,
  cash_5     string,
  cash_cnt_5 string,
  cash_6     string,
  cash_cnt_6 string,
  cash_0     string,
  cash_cnt_0 string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/temp_event_cash_6m';