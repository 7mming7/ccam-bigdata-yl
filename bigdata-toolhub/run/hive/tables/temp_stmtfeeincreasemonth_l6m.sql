use zhgl;

create table if not exists TEMP_STMTFEEINCREASEMONTH_L6M
(
  acctnbr               string,
  flag_con_add_cost_cnt string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/temp_stmtfeeincreasemonth_l6m';