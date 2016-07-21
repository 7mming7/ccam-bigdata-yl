use zhgl;

CREATE TABLE IF NOT EXISTS ETL_PROC_LOG
(
  data_date string,
  proccode  string,
  procname  string,
  ctltyp    string,
  sql_sn    string,
  rec_ts_s  string,
  rec_ts_e  string,
  recstat   string,
  sqlstat   string
)
row format delimited fields 
terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/etl_proc_log';