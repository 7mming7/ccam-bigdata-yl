use zhgl;

create table if not exists TEMP_CUSTUM_FLAG
(
  custr_nbr string,
  flag1     string,
  flag2     string,
  flag3     string,
  flag4     string,
  flag5     string,
  flag6     string,
  flag7     string,
  flag8     string,
  flag9     string,
  flag10    string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/temp_custum_flag';