use zhgl;

CREATE TABLE IF NOT EXISTS MDM_CARD_CUSTR
(
  card_nbr          string,
  bank              string,
  xaccount          string,
  cardholder        string,
  master_nbr        string,
  custr_nbr         string,
  supp_birthdate    string,
  app_relation_supp string,
  employergeocode   string
)
row format delimited fields 
terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/mdm_card_custr';