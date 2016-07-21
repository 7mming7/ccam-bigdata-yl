use zhgl;

CREATE TABLE IF NOT EXISTS JOB_RES_CTRL
(
  job_res_type string,
  job_res_nm   string,
  job_res_max  string,
  job_res_idle string,
  remark       string
)
row format delimited fields 
terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/job_res_ctrl';