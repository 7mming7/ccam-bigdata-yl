use zhgl;

CREATE TABLE IF NOT EXISTS JOB_METADATA
(
  job_nm       string,
  job_cnm      string,
  job_res_type string,
  job_run_type string,
  job_run_dt   string,
  job_type     string,
  job_cmd      string,
  job_par      string,
  job_prio     string,
  job_wkres    string,
  job_times    string,
  remark       string,
  sys_id       string
)
row format delimited fields 
terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/job_metadata';