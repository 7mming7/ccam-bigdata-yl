use zhgl;

create table if not exists S24_ODUE(
bank             string,
account          string,
branch           string,
category         string,
curr_num         string,
inp_day          string,
draft_day        string,
month_nbr        string,
odue_flag        string,
inp_bal          string,
inp_balori       string,
rem_bal          string,
rem_balori       string,
last_paymt       string,
inp_time         string,
inp_noint        string,
rem_noint        string,
inp_nint01       string,
inp_nint02       string,
inp_nint03       string,
inp_nint04       string,
inp_nint05       string,
inp_nint06       string,
inp_nint07       string,
inp_nint08       string,
inp_nint09       string,
inp_nint10       string,
rem_nint01       string,
rem_nint02       string,
rem_nint03       string,
rem_nint04       string,
rem_nint05       string,
rem_nint06       string,
rem_nint07       string,
rem_nint08       string,
rem_nint09       string,
rem_nint10       string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_odue';