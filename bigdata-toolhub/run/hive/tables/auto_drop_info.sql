use zhgl;

CREATE TABLE IF NOT EXISTS AUTO_DROP_INFO
(
  tablename string,
  timeout   string
)
row format delimited fields 
terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/auto_drop_info';