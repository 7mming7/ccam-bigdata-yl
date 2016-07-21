use zhgl;

CREATE TABLE IF NOT EXISTS FILE_LOAD_TABLE_LOG
(
  data_date      string,
  filename       string,
  tablename      string,
  load_state     string,
  sqlstat        string,
  load_read      string,
  load_skipped   string,
  load_load      string,
  load_rejected  string,
  load_deleted   string,
  load_committed string,
  remark         string
)
row format delimited fields 
terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/file_load_table_log';