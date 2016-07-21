use zhgl;

CREATE VIEW if not exists V_S24_GUAR AS
SELECT BANK,
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
     FROM S24_GUAR;

