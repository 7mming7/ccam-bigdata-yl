use zhgl;

CREATE TABLE IF NOT EXISTS LOAD_DATA_META
(
  filename     string,
  tablename    string,
  loadtype     string,
  importorload string,
  cleartype    string,
  reqsource    string,
  packname     string
)
row format delimited fields 
terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/load_data_meta';