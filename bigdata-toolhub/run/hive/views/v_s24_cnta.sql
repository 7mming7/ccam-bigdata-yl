use zhgl;

CREATE VIEW if not exists V_S24_CNTA AS
SELECT BANK,
          CUSTR_NBR,
          CON_NAME,
          APP_JDAY,
          APP_SEQ,
          MICROFILM,
          CON_ID,
          CON_TEL,
          CON_EXT,
          CON_COMP,
          CON_MOBILE,
          CON_REL,
          CON_FLAG,
          ETL_DAY,
          CON_AREA
     FROM S24_CNTA;


