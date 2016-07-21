use zhgl;

create table if not exists S24_ACLOW(
  acqcode              string,
  bank                 string,
  custr_nbr            string,
  mcc_sel              string,
  out_nbr              string,
  bank_name            string,
  bus_nam              string,
  cor_no               string,
  des_name             string,
  des_type             string,
  inp_date             string,
  inp_time             string,
  license              string,
  mcc                  string,
  org_id               string,
  xstatus              string,
  tax_id               string,
  un_bank_id           string
)
row format delimited
fields terminated by '|'
stored as textfile 
location '/user/hive/warehouse/HDFSDIRNAME/s24_aclow';