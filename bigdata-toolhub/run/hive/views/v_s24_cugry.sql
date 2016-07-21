use zhgl;

CREATE VIEW if not exists V_S24_CUGRY AS
SELECT BANK,
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
     FROM S24_CUGRY;

