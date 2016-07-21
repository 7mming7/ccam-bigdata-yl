use zhgl;

create table if not exists CCAM_CODES (
  BANK_ID string,
  CREATE_TIME string,
  CODE_TYPE string,
  CODE_VALUE string,
  BRIEF string,
  SHORT_DEF string
)
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/ccam_codes';