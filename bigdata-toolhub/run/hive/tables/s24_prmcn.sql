use zhgl;

create table if not exists S24_PRMCN(
category        string,
cate_desc       string,
curr_num2       string,
proc_days       string,
branch          string,
pdcat_flag      string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_prmcn';