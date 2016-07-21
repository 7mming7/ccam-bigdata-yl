use zhgl;

CREATE TABLE if not exists MDM_CARD_INFO
   (BANK      				string,
	XACCOUNT  				string,
	CUSTR_NBR 				string,
	CARD_ID   				string,
	CARD_BLOCK_CODES 		string,
	RISKGRADE 				string,
	ACCOUNT_DATE 			string,
	ISSUERSUM 				string,
	CHEAT_FLG 				string,
	CHANNEL_CODE			string,
	APP_CHANNEL 			string,
	APP_BIZ   				string,
	APP_JOINT 				string,
	APP_EMPLOYEE   			string,
	APP_EXISTINGCC 			string,
	APP_APPDATE 			string,
	CCONUS      			string,
	PRO_EXPIREMONTHSLEFT 	string,
	APP_CCVALIDDATE 		string,
	PRO_STATUS_EXISTINGCC 	string,
	ACCTSUM_REPAYED 		string
   ) 
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/mdm_card_info';