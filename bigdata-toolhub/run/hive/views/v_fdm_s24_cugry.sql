use zhgl;
CREATE VIEW if not exists V_FDM_S24_CUGRY AS
SELECT END_DATE,
          START_DATE,
          BANK,
          CUSTR_NBR,
          NAME_KEY,
          INP_SRC,
          INP_REASON,
          REASON_DESC,
          RCU_EMPLY,
          BRANCH,
          INP_DAY,
          INP_TIME,
          CHG_DAY,
          CHG_TIME,
          NOTE
     FROM FDM_S24_CUGRY
   UNION ALL
   SELECT SYSDATE(),
          p.START_DATE,
          p.BANK,
          p.CUSTR_NBR,
          p.NAME_KEY,
          p.INP_SRC,
          p.INP_REASON,
          p.REASON_DESC,
          p.RCU_EMPLY,
          p.BRANCH,
          p.INP_DAY,
          p.INP_TIME,
          p.CHG_DAY,
          p.CHG_TIME,
          p.NOTE
     FROM FDM_S24_CUR_CUGRY p;


