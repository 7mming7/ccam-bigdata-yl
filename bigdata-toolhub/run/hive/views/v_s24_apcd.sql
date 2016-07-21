use zhgl;

CREATE VIEW if not exists V_S24_APCD AS
SELECT BANK,
          APP_JDAY,
          APP_SEQ,
          CUSTR_NBR,
          CARD_NBR,
          APP_DAY
     FROM S24_APCD;

