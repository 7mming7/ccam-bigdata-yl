use zhgl;

CREATE TABLE IF NOT EXISTS ETL_PROC_CTRL
(
  data_date    string,
  headdt       string,
  ctltyp       string,
  btime        string,
  etime        string,
  etlstat      string,
  filelog_path string,
  remark       string
)
row format delimited fields 
terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/etl_proc_ctrl';