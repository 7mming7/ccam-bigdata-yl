use zhgl;

create table if not exists T_FDM_TABLENAME
(
  fdm_table      string,
  group_id       string,
  cur_data_table string,
  source_table   string,
  key_col        string,
  typeid         string,
  last_date      string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/t_fdm_tablename';