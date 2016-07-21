use zhgl;

CREATE TABLE IF NOT EXISTS ETL_TABLE_CLEAR_CON
(
  tablename string,
  datecol   string,
  savdate   string
)
row format delimited fields 
terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/etl_table_clear_con';