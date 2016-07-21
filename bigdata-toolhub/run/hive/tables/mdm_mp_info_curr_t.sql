use zhgl;

CREATE TABLE if not exists MDM_MP_INFO_CURR_T
   (BANK 							string,
	XACCOUNT 						string,
	CURR_TERM 						string,
	CU_LINS_CUR_FLG 				string, 
	LINS_LIMIT 						string,
	LINS_TYPE 						string,
	LINS_TERM 						string,
	LINS_REM_TERM 					string,
	LINS_UNSH_PRINCIPAL 			string,
	LINS_SH_UNPAY_PRINCIPAL_BIG 	string,
	LINS_SH_UNPAY_BAL 				string,
	LINS_STATUS 					string,
	AMOUNT 							string,
	APP_TERMS 						string
   ) 
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/mdm_mp_info_curr_t';