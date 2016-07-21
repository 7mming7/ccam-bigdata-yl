use zhgl;

create table if not exists S24_APPNT(
bank           string,
app_jday       string,
app_seq        string,
microfilm      string,
note_types     string,
race_code      string,
custr_nbr      string,
chg_day        string,
app_prop       string,
class_code     string,
occ_code       string,
occupation     string,
asset_prv      string,
trail_code     string,
ab_fashion     string,
ab_rel         string,
bank_rel       string,
pos_char       string,
fte_codes      string,
credit_ttl     string,
earning        string,
asset          string,
app_day        string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_appnt';