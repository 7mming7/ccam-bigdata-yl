use zhgl;

create table if not exists S24_APDY(
bank             string,
custr_nbr        string,
surname          string,
app_jday         string,
app_seq          string,
reas_code        string,
dec_ref          string,
employee         string,
inp_day          string,
inp_time         string,
inp_source       string
)
row format delimited
fields 
terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_apdy';