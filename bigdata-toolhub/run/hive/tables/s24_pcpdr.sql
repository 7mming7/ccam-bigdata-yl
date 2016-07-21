use zhgl;

create table if not exists S24_PCPDR (
  bank         string,
  curr_num     string,
  cp_no        string,
  product      string,
  max_amt      string,
  min_amt      string,
  sign_min_amt string,
  nbr_mths0    string,
  prod_id0     string,
  nbr_mths1    string,
  prod_id1     string,
  nbr_mths2    string,
  prod_id2     string,
  nbr_mths3    string,
  prod_id3     string,
  nbr_mths4    string,
  prod_id4     string,
  nbr_mths5    string,
  prod_id5     string,
  nbr_mths6    string,
  prod_id6     string,
  nbr_mths7    string,
  prod_id7     string,
  nbr_mths8    string,
  prod_id8     string,
  nbr_mths9    string,
  prod_id9     string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/s24_pcpdr';