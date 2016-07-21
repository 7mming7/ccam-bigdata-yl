use zhgl;

CREATE TABLE IF NOT EXISTS JOB_SEQ
(
  job_nm  string,
  pre_job string
)
row format delimited fields 
terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/job_seq';