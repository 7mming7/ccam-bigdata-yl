use zhgl;

create table if not exists S24_PRMFE(
product          string,
fee_code         string,
effect_day       string,
crd1             string,
crd2             string,
crd3             string,
crd4             string,
crd5             string,
crd6             string,
crd7             string,
crd8             string,
crd9             string,
cf_idx           string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_prmfe';