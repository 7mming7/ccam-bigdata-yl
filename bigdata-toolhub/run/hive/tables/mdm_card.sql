use zhgl;

CREATE TABLE if not exists MDM_CARD
   (CARD_NBR 				string,
	BANK 					string,
	XACCOUNT 				string,
	RESULT_REASON_CODE 		string,
	CARD_BLOCK_CODES 		string,
	RISK_GRADE 				string,
	ACCOUNT_DATE 			string,
	ISSUER_SUM   			string,
	QIZHA_FLAG   			string,
	CHANNEL_CODE 			string,
	CHANNEL  				string,
	BIZ      				string,
	JOINT    				string,
	EMPLOYEE 				string,
	EXISTING_CC 			string,
	APP_DATE    			string,
	CCONUS      			string,
	EXPIRE_MONTHS_LEFT 		string,
	CCVALID_DATE       		string,
	STATUS_EXISTINGCC  		string,
	ACCTSUM_REPAYED    		string,
	ACTIVE_DAY 				string,
	CARDHOLDER 				string,
	ISS_SERIAL 				string
   )
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/mdm_card';