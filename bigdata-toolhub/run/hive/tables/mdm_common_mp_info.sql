use zhgl;

CREATE TABLE if not exists MDM_COMMON_MP_INFO
   (XACCOUNT  					string,
	CUSTR_NBR 					string,
	PRODUCT   					string,
	TERMS     					string,
	APP_CHANNEL  				string,
	FIRSTPAYMENT 				string,
	LINS_REM_UNSH_PRINCIPAL 	string,
	LINS_REM_UNSH_FEE 			string,
	LINS_SH_UNPAY_PRINCIPAL 	string
   )
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/mdm_common_mp_info';