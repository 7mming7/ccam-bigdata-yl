use zhgl;

CREATE VIEW if not exists V_MAIN_ACCOUNT AS
SELECT    acct.CUSTR_NBR CUSTNUM,
          acct.ACCTNUM,
          acct.RECORDTYPE,
          acct.EVENTTYPE,
          acct.NODETYPE,
          main.CATEGORY,
          acct.DATECLOSED,
          acct.CYCLE_DATE,
          acct.BRANCH,
          acct.CARD_ID,
          acct.RESULTREASONCODE,
          acct.CARD_BLOCK_CODES,
          acct.RISKGRADE,
          acct.OPENDATE,
          acct.RESTRICTIVETRADEFLAG,
          acct.SENDINGMETHOD,
          acct.CUSTTYPE,
          acct.TEMP_LIMIT,
          acct.ACCTBOSSAPPROVED,
          acct.HISTUPDATEDTYPE,
          acct.HISTUPDATEDDATE,
          acct.APPTYPE,
          acct.AMOUNT,
          acct.APP_TERMS,
          acct.APP_APPDATE,
          acct.APP_AUTODEBIT,
          acct.DEATH_EXCLUDE_FLAG,
          acct.PRO_ACCTSTATUS_CC_EXISTING,
          acct.SCORE,
          acct.HISTUPDATEDUPORDN,
          acct.GENDER,
          acct.CollectTimes,
          acct.LONG_CRED_LIMIT,acct.ACCTACCTLIMIT,acct.CYCLE_DAY_ONLY,acct.ACCTNUMB,acct.LAST_CUS_PERM_LINE_INC,acct.LAST_CUS_TEMP_LINE_ADJ,acct.LAST_BANK_PERM_LINE_INC,
          acct.LAST_BANK_TEMP_ADJ,
          acct.LAST_BILL_ADDRESS_CHANGE,
          acct.DateLastActive
     FROM account acct inner join temp_main_acct main on acct.ACCTNUM = main.XACCOUNT;



