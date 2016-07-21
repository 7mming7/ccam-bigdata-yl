use zhgl;

create table MATCHRESULT
(
  custnum  string,
  countnum string,
  bankid   string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/matchresult';