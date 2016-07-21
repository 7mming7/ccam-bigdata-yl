use zhgl;

create table if not exists S24_ASST(
bank         string,
assistant    string,
xaccount     string,
custr_nbr    string
)
row format delimited
fields terminated by '|'
stored as textfile 
location '/user/hive/warehouse/HDFSDIRNAME/s24_asst';