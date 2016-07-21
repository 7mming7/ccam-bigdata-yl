use zhgl;

create table if not exists TEMP_EVENT_PUR_TOP3SUM
(
  acctnbr                   string,
  top3sum0                  string,
  top3sum1                  string,
  top3sum2                  string,
  top3sum3                  string,
  top3sum4                  string,
  top3sum5                  string,
  top3sum6                  string,
  merchantsum6m             string,
  offline_nodl_merchant_cnt string,
  max_amt_0                 string,
  cnt_0                     string,
  sum_amt_0                 string,
  max_amt_1                 string,
  cnt_1                     string,
  sum_amt_1                 string,
  max_amt_2                 string,
  cnt_2                     string,
  sum_amt_2                 string,
  max_amt_3                 string,
  cnt_3                     string,
  sum_amt_3                 string,
  max_amt_4                 string,
  cnt_4                     string,
  sum_amt_4                 string,
  max_amt_5                 string,
  cnt_5                     string,
  sum_amt_5                 string,
  max_amt_6                 string,
  cnt_6                     string,
  sum_amt_6                 string,
  active_day                string,
  max_ovd_6m                string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/temp_event_pur_top3sum';