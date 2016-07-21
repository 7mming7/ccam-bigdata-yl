use zhgl;

create table if not exists S24_CDPP(
bank             string,
custr_nbr        string,
id_type          string,
card_no          string,
cdtype           string,
emp_nbr          string,
source           string,
inp_date         string,
inp_time         string,
issueno          string,
chg_day          string,
chg_time         string,
chg_emp          string,
status           string,
cancl_code       string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_cdpp';