use zhgl;

CREATE TABLE if not exists MDM_CARD_CUSTR_INFO
   (BANK 				string,
	XACCOUNT 			string,
	CUSTR_NBR 			string,
	MAIN_CARD_NBR 		string,
	SUPP_CARD_NBR 		string,
	CARDHOLDER 			string,
	APP_SUPP_BIRTHDATE 	string,
	APP_RELATION_SUPP  	string,
	EMPLOYERGEOCODE 	string,
	ACTIVE_DAY 			string,
	MO_PHONE   			string
   )
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/mdm_card_custr_info';