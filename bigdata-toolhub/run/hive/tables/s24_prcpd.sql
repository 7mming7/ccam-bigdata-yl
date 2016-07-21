use zhgl;

create table if not exists S24_PRCPD(
bank             string,
cp_no            string,
begin_day        string,
cp_desc          string,
cp_inpscr        string,
cp_mcc           string,
cp_mch           string,
cp_openday       string,
end_day          string,
max_mths         string,
min_mths         string,
xunique          string,
cp_type          string,
pmapc            string,
mths_fl          string,
tran_pur         string,
tran_wthd        string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_prcpd';