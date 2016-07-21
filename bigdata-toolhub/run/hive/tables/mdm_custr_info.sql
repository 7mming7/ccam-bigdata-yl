use zhgl;

create table if not exists MDM_CUSTR_INFO
(
  bank                  string,
  xaccount              string,
  custr_nbr             string,
  yyyymm_birth          string,
  yyyymmdd_birth        string,
  app_corptype          string,
  acctblacklist         string,
  acctwhitelist         string,
  marriage              string,
  education_level       string,
  customer_id           string,
  app_national          string,
  custindustry          string,
  restrictivetradeflag  string,
  sendingmethod         string,
  custtype              string,
  app_relation_contact  string,
  annual_income         string,
  cardapp_income        string,
  apptype               string,
  pboc_revolveguarantor string,
  appid                 string,
  monthlysalary         string,
  idissuedgeocode       string,
  app_criticalindustry  string,
  localpropertysize     string,
  race_type             string,
  gender                string,
  dispute_exclude_flag  string,
  pledgevalue           string,
  pledgetype            string,
  app_residentialstatus string,
  cred_limit            string,
  yeyyday               string,
  leyyday               string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/mdm_custr_info';