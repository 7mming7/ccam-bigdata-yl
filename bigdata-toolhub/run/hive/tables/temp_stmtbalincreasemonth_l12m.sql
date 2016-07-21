use zhgl;

create table if not exists TEMP_STMTBALINCREASEMONTH_L12M
(
  acctnbr              string,
  flag_con_add_bal_cnt string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/temp_stmtbalincreasemonth_l12m';