use zhgl;

CREATE TABLE if not exists ADJUST_SEND_RECORD_T_H
(
  ID                string,
  CUSTR_NBR         string,
  MO_PHONE          string,
  BANK_ID           string,
  BANK_NUMBER       string,
  ADJUST_TYPE       string,
  ADJUST_DIRECT     string,
  ADJUST_LIMIT      string,
  STATUS            string,
  CREATE_TIME       string,
  NOTFY_SEND_TIME   string,
  END_TIME          string,
  NOTIFY_CHANNEL    string,
  ISBATCH           string,
  TEXT_MESSAGE      string,
  SWIFT_NUMBER      string,
  BRANCHNO          string,
  SOURCE_FLAG       string,
  TEST_RUTURN       string,
  LONGAMOUNT        string,
  ACCT              string,
  UPLOAD_TIME       string,
  ADJUST_FAILURE    string,
  UPLOAD_BATCH      string,
  COMFIRM_TIME      string,
  SMS_SEND_TIME     string,
  SMS_SEND_RECORD   string,
  SMS_NOTICE_RECORD string,
  SMS_NOTICE_TIME   string,
  SMHFYXTIME        string,
  ACCTLIMIT         string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/adjust_send_record_t_h';
