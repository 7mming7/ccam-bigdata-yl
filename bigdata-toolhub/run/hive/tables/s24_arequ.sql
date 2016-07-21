use zhgl;

create table if not exists S24_AREQU(
bank          string,
regioncode    string,
areacode      string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_arequ';