use zhgl;

create table if not exists TEMP_EVENT_INCOME
(
  acctnbr    string,
  income     string,
  goodincome string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/temp_event_income';