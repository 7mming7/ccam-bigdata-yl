use zhgl;

CREATE TABLE if not exists MDM_LIMIT_ADJ_INFO
   (BANK 						string,
	XACCOUNT 					string,
	CUSTR_NBR 					string,
	HISTUPDATEDTYPE 			string,
	HISTUPDATEDDATE 			string,
	LAST_BANK_ALOP_LINE 		string,
	LAST_CUS_PERM_LINE_INC 		string,
	LAST_CUS_TEMP_LINE_ADJ 		string,
	LAST_BANK_PERM_LINE_INC 	string,
	LAST_BANK_TEMP_ADJ 			string,
	LASTDELETEMON   			string,
	LASTINCREMONASE 			string,
	LASTBATCHDEMON  			string,
	LASTBATCHINMON  			string
   ) 
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/mdm_limit_adj_info';