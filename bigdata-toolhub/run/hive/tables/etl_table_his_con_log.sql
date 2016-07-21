use zhgl;

CREATE TABLE IF NOT EXISTS ETL_TABLE_HIS_CON_LOG
(
  data_date   string,
  stable_name string,
  itable_name string,
  start_time  string,
  end_time    string,
  status      string
)
row format delimited fields 
terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/etl_table_his_con_log';