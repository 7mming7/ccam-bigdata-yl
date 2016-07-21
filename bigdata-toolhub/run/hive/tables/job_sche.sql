use zhgl;

CREATE TABLE IF NOT EXISTS JOB_SCHE
(
  job_seq_id    string,
  job_nm        string,
  job_status    string,
  job_prio      string,
  job_sche_date string,
  job_begin_dt  string,
  job_begin_tm  string,
  job_end_dt    string,
  job_end_tm    string
)
row format delimited fields 
terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/job_sche';