use zhgl;
CREATE VIEW if not exists V_FDM_S24_GUAR AS
SELECT END_DATE,
          START_DATE,
          BANK,
          GUARN_ID,
          ACCOUNT,
          CARD_ID,
          CUSTR_ID,
          APP_JDAY,
          APP_SEQ,
          GUARN_CODE,
          GUARN_BANK,
          GUARN_AMT,
          GUARN_REF,
          GUARN_REL,
          ETL_DAY,
          APP_DAY
     FROM FDM_S24_GUAR
   UNION ALL
   SELECT SYSDATE(),
          p.START_DATE,
          p.BANK,
          p.GUARN_ID,
          p.ACCOUNT,
          p.CARD_ID,
          p.CUSTR_ID,
          p.APP_JDAY,
          p.APP_SEQ,
          p.GUARN_CODE,
          p.GUARN_BANK,
          p.GUARN_AMT,
          p.GUARN_REF,
          p.GUARN_REL,
          p.ETL_DAY,
          p.APP_DAY
     FROM FDM_S24_CUR_GUAR p;


