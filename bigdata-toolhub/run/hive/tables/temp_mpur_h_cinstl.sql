use zhgl;

create table if not exists TEMP_MPUR_H_CINSTL
(
  xaccount  string,
  mth_instl string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/temp_mpur_h_cinstl';