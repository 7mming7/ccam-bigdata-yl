use zhgl;

create table if not exists FDM_S24_MPTR (
  acctnbr      string,
  bank         string,
  inp_date     string,
  inp_time     string,
  merchant     string,
  micro_ref    string,
  month_nbr    string,
  ori_tranno   string,
  xtranno      string,
  trans_type   string,
  val_date     string,
  acptor_id    string,
  acqmemb_id   string,
  auth_code    string,
  bankacct     string,
  bill_amt     string,
  bill_amtflag string,
  brno         string,
  card_nbr     string,
  card_plan    string,
  cardholder   string,
  currncy_cd   string,
  des_line1    string,
  des_line2    string,
  empno        string,
  m_p_ind      string,
  mer_cat_cd   string,
  merch_seq    string,
  nbr_mths     string,
  product      string,
  pur_date     string,
  pur_time     string,
  rev_ind      string,
  terminali    string,
  trans_src    string
)
partitioned by (CREATE_DATE string)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/fdm_s24_mptr';