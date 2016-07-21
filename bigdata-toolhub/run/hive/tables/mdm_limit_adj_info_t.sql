use zhgl;

CREATE TABLE if not exists MDM_LIMIT_ADJ_INFO_T
   (BANK 			string,
	XACCOUNT 		string,
	LIMIT_ADJ_TYPE 	string,
	LIMIT_ADJ_SIGN 	string,
	HISTUPDATEDTYPE string,
	HISTUPDATEDDATE string
   ) 
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/mdm_limit_adj_info_t';