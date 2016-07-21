use zhgl;

create table if not exists FDM_S24_CUR_ACCA (
  start_date      string,
  xaccount        string,
  bank            string,
  sms_fees        string,
  feesign         string,
  sms_lowamt      string,
  sms_lowamx      string,
  sms_yn          string,
  sms_month       string,
  mp_l_lmt        string,
  mp_auths        string,
  mpausign        string,
  mp_rem_ppl      string,
  mpremsig        string,
  mp_bal          string,
  mpbalsig        string,
  cal_limit       string,
  cal_auths       string,
  cal_auths_flag  string,
  cal_bal         string,
  cal_bal_flag    string,
  cal_remppl      string,
  cal_remppl_flag string,
  sms_freeyn      string,
  bal_mppl        string,
  bal_mpplx       string,
  bal_l_mppl      string,
  daily_rep       string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/fdm_s24_cur_acca';