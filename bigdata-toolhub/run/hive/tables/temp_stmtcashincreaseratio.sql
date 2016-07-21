use zhgl;

create table if not exists TEMP_STMTCASHINCREASERATIO
(
  acctnbr               string,
  flag_con_add_cash_cnt string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/temp_stmtcashincreaseratio';