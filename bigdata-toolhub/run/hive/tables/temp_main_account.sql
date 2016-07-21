use zhgl;

create table if not exists TEMP_MAIN_ACCOUNT
(
  custnum                    string,
  acctnum                    string,
  recordtype                 string,
  eventtype                  string,
  nodetype                   string,
  closuretype                string,
  dateclosed                 string,
  cycle_date                 string,
  branch                     string,
  card_id                    string,
  resultreasoncode           string,
  card_block_codes           string,
  riskgrade                  string,
  opendate                   string,
  restrictivetradeflag       string,
  sendingmethod              string,
  custtype                   string,
  temp_limit                 string,
  acctbossapproved           string,
  histupdatedtype            string,
  histupdateddate            string,
  apptype                    string,
  amount                     string,
  app_terms                  string,
  app_appdate                string,
  app_autodebit              string,
  death_exclude_flag         string,
  pro_acctstatus_cc_existing string,
  score                      string,
  histupdatedupordn          string,
  gender                     string,
  collecttimes               string,
  long_cred_limit            string,
  acctacctlimit              string,
  cycle_day_only             string,
  acctnumb                   string,
  last_cus_perm_line_inc     string,
  last_cus_temp_line_adj     string,
  last_bank_perm_line_inc    string,
  last_bank_temp_adj         string,
  last_bill_address_change   string,
  datelastactive             string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/temp_main_account';