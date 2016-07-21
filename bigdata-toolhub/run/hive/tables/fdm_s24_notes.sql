use zhgl;

create table if not exists FDM_S24_NOTES (
  bank       string,
  xaccount   string,
  entry_day  string,
  entry_seq  string,
  entry_time string,
  entry_type string,
  amt_invlvd string,
  business   string,
  card_nbr   string,
  custr_nbr  string,
  entry_brch string,
  entry_srce string,
  letr_code  string,
  narr_1     string,
  operator   string,
  query_code string,
  resln_day  string,
  resln_mthd string,
  resln_time string,
  short_desc string,
  narr_2     string,
  narr_3     string,
  narr_4     string
)
partitioned by (CREATE_DATE string)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/fdm_s24_notes';