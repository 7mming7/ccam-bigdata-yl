use zhgl;

CREATE TABLE if not exists MDM_ACCT_X_SP
   (XACCOUNT 		string,
	BANK 			string,
	BUSINESS 		string,
	CATEGORY 		string,
	CUSTR_NBR 		string,
	CYCLE_NBR 		string,
	DAY_OPENED 		string,
	ACC_NAME1 		string,
	ACC_TYPE 		string,
	ADD_CHGDAY 		string,
	ADDR_TYPE 		string,
	ADDR_TYPE2 		string,
	AGE 			string,
	APP_SOURCE 		string,
	AUTH_CASH 		string,
	AUTH_OVER 		string,
	AUTHOV_YN 		string,
	AUTHS_AMT 		string,
	BAL_CMPINT 		string,
	BAL_FREE 		string,
	BAL_INT 		string,
	BAL_NOINT 		string,
	BAL_ORINT 		string,
	BANKACCT1 		string,
	BANKCODE1 		string,
	BANKACCT2 		string,
	BANKCODE2 		string,
	BANKACCT3 		string,
	BANKCODE3 		string,
	BANKACCT4 		string,
	BANKCODE4 		string,
	BRANCH 			string,
	BRANCH_DAY 		string,
	CA_PCNT 		string,
	CARD_FEES 		string,
	CARDS_CANC 		string,
	CARDS_ISSD 		string,
	CASH_1ST 		string,
	CASH_ADFEE 		string,
	CASH_ADVCE 		string,
	CASH_TDAY 		string,
	CASH1STAC 		string,
	CLASS_CHDY 		string,
	CLASS_CODE 		string,
	CLOSE_CHDY 		string,
	CLOSE_CODE 		string,
	COLLS_DAY 		string,
	CRDACT_DAY 		string,
	CRDNXT_DAY 		string,
	CRED_ACTIV 		string,
	CRED_ADJ 		string,
	CRED_CHDAY 		string,
	CRED_LIMIT 		string,
	CRED_VOUCH 		string,
	CREDLIM_X 		string,
	CURR_NUM 		string,
	CUTOFF_DAY 		string,
	CY_CHGCNT 		string,
	CY_CHGDAY 		string,
	CY_EFFDAY 		string,
	CY_YRCNT 		string,
	CYCLE_NEW 		string,
	CYCLE_PRV 		string,
	DEBIT_ADJ 		string,
	DISHNRDAY 		string,
	DUTY_CREDT 		string,
	DUTY_DEBIT 		string,
	EXCH_CODE 		string,
	EXCH_FLAG 		string,
	EXCH_PERC 		string,
	EXCH_RTDT 		string,
	FEE_MONTH 		string,
	FEES_TAXES 		string,
	FIX_EXAMT  		string,
	GUARN_FLAG 		string,
	HI_CASHADV 		string,
	HI_CASMMYY 		string,
	HI_CRDMMYY 		string,
	HI_CREDIT 		string,
	HI_DEBIT 		string,
	HI_DEBMMYY 		string,
	HI_MP_PUR 		string,
	HI_MPMMYY 		string,
	HI_OLIMIT 		string,
	HI_OLIMMYY 		string,
	HI_PURCHSE 		string,
	HI_PURMMYY 		string,
	INT_CASH 		string,
	INT_CHDCMP 		string,
	INT_CHDY 		string,
	INT_CHGD 		string,
	INT_CMPOND 		string,
	INT_CODE 		string,
	INT_CUNOT 		string,
	INT_CURCMP 		string,
	INT_EARNED 		string,
	INT_NOTION 		string,
	INT_PROCDY 		string,
	INT_RATE 		string,
	INT_RATECR 		string,
	INT_UPTODY 		string,
	LANG_CODE 		string,
	LAST_TRDAY 		string,
	LASTAUTHDY 		string,
	LASTNTDATE 		string,
	LASTPAYAMT 		string,
	LASTPAYDAY 		string,
	LOSSES 			string,
	MONTH_NBR 		string,
	MONTR_CHDY 		string,
	MONTR_CODE 		string,
	MP_AM_TDY 		string,
	MP_AUTHS 		string,
	MP_BAL 			string,
	MP_BIL_AMT 		string,
	MP_LIMIT 		string,
	MP_NO_TDY 		string,
	MP_REM_PPL 		string,
	MPLM_CHDAY 		string,
	MTHS_ODUE 		string,
	NBR_CASHAD 		string,
	NBR_FEEDTY 		string,
	NBR_OLIMIT 		string,
	NBR_OTHERS 		string,
	NBR_PAYMNT 		string,
	NBR_PURCH 		string,
	NBR_TRANS 		string,
	OCT_COUNT 		string,
	OCT_DAYIN 		string,
	ODUE_FLAG 		string,
	ODUE_HELD 		string,
	OLFLAG 			string,
	OTHER_FEES 		string,
	PAY_FLAG 		string,
	PAY1ST_IND 		string,
	PAYMT_CLRD 		string,
	PAYMT_TDAY 		string,
	PAYMT_UNCL 		string,
	PEN_CHRG 		string,
	PENCHG_ACC 		string,
	POINT_ADJ 		string,
	PT_ADJFLAG 		string,
	POINT_CLM 		string,
	POINT_CUM 		string,
	PT_CUMFLAG 		string,
	POINT_CUM2 		string,
	POINT_EAR 		string,
	POINT_FREZ 		string,
	POINT_FZDA 		string,
	POST_DD 		string,
	PREV_BRDAY 		string,
	PREV_BRNCH 		string,
	PURCHASES 		string,
	QUERY_AMT 		string,
	QUERY_CODE 		string,
	QUERY_STMT 		string,
	RECLA_CHDY 		string,
	RECLA_CODE 		string,
	RECVRY_AMT 		string,
	REPAY_AMT 		string,
	REPAY_AMTX 		string,
	REPAY_CODE 		string,
	REPAY_CODX 		string,
	REPAY_DAY 		string,
	REPAY_PCT 		string,
	REPAY_PCTX 		string,
	REPY_CHGDY 		string,
	SCORE_PTS 		string,
	STATEMENTS 		string,
	STM_AMT_OL 		string,
	STM_BALFRE 		string,
	STM_BALINT 		string,
	STM_BALNCE 		string,
	STM_BALORI 		string,
	STM_CLOSDY 		string,
	STM_CODE 		string,
	STM_INSTL 		string,
	STM_MINDUE 		string,
	STM_NOINT 		string,
	STM_OLFLAG 		string,
	STM_OVERDU 		string,
	STM_PAY_UN 		string,
	STM_QRYAMT 		string,
	STM_REPAY 		string,
	STMT_DD 		string,
	STMT_PULL 		string,
	TODAY_AMT 		string,
	TODAY_REL 		string,
	UNCL_PCT 		string,
	WROFF_CHDY 		string,
	WROFF_CODE 		string,
	PROD_LEVEL 		string,
	PROD_NBR 		string,
	ACCT_PRSTS 		string,
	ACCT_STS 		string,
	APP_APQUE 		string,
	POINT_ENC 		string,
	POINT_IMP 		string,
	POINT_EXP 		string,
	RISK_COUNT 		string,
	RISK_DAYIN 		string,
	BAL_MP 			string,
	STM_BALMP 		string,
	LMT_RSN 		string,
	POINT_CARD 		string,
	TEMP_LIMIT 		string,
	TLMT_BEG 		string,
	TLMT_END 		string,
	TLMT_NO 		string,
	CANCL_RESN 		string,
	SHADOW_PEN 		string,
	SHADOW_INT 		string,
	SHADOW_CMP 		string,
	BAL_NINT 		string,
	STM_NINT 		string,
	CURR_NUM2 		string,
	WROF_FLAG 		string,
	LAYERCODER1 	string,
	LAYERCODER2 	string,
	MCNTRL_YN 		string,
	NCRED_RSN 		string,
	BSC_CRED  		string,
	CUSTR_REF 		string,
	PBC_BRNCH 		string,
	STOPMP_YN 		string,
	CRED_LMT2 		string,
	BAL_CMPFEE 		string,
	EXCH_AMT 		string,
	EXCH_AUTH 		string,
	EXCH_INQAM 		string,
	LOAD_DATE 		string,
	BAL_ADD14 		string,
	PRODUCT 		string,
	BAL 			string,
	CAL_LIMIT 		string,
	MP_L_LMT 		string,
	BAL_WO_AUTHS	string
   )
row format delimited
fields terminated by '|'
stored as textfile
location '/user/hive/warehouse/HDFSDIRNAME/mdm_acct_x_sp';