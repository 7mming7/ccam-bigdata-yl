use zhgl;

create table if not exists S24_PRMMT(
mer_type         string,
amccg_type       string,
cattype_mc       string,
cattype_vi       string,
contr_type       string,
custpayser       string,
daily_subs       string,
floor_lim        string,
floor_lim2       string,
hi_rsk_csh       string,
indual_amt       string,
indul_msg        string,
indul_resp       string,
m_p_ind          string,
mcc_analy        string,
mcc_cntrl        string,
mcc_desc         string,
mccat_type       string,
quasi_cash       string,
recur_tx         string,
rep_bull         string,
te_ind           string,
trans_type       string,
txn_permit       string
)
row format delimited
fields terminated by '|'
stored as textfile 
location '/user/hive/warehouse/HDFSDIRNAME/s24_prmmt';