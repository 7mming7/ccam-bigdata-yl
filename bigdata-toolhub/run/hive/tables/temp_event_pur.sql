use zhgl;

CREATE TABLE IF NOT EXISTS TEMP_EVENT_PUR
(
  xaccount      string,
  badpur        string,
  badmaxpur     string,
  pur           string,
  gas_pur       string,
  gas_cnt       string,
  bx_pur        string,
  bx_cnt        string,
  goodcnt       string,
  mcc90sum      string,
  mcc90netsum   string,
  mcc55sum      string,
  mcc55netsum   string,
  mcc26sum      string,
  mcc26netsum   string,
  jw120sum      string,
  jw120netsum   string,
  dlsum         string,
  dlnetsum      string,
  mcc90count    string,
  mcc90netcount string,
  mcc55count    string,
  mcc55netcount string,
  mcc26count    string,
  mcc26netcount string,
  jw120count    string,
  jw120netcount string,
  dlcount       string,
  dlnetcount    string,
  jwrecency     string,
  dlcount_2     string,
  dlnetcount_2  string
)
row format delimited fields 
terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/temp_event_pur';