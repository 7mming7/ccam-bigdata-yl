use zhgl;

CREATE TABLE IF NOT EXISTS ETL_TABLE_HIS_CON
(
  src_table      string,
  ord_data_space string,
  ord_idx_space  string,
  ord_table      string,
  run_type       string,
  sav_date       string
)
row format delimited fields 
terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/etl_table_his_con';